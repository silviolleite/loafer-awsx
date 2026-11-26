# Requirements Document

## Introduction

This feature replaces the Consumer's receive-ahead buffering with demand-driven flow
control, so that a message is received only when the Consumer has capacity to start it
soon, and the visibility timeout is managed from the moment the message is received.

Today the polling loop receives up to `MaxMessages` messages per call and pushes each one
into a per-worker buffered channel whose capacity equals `MaxMessages`. With the defaults
of a production route (30 workers, `MaxMessages` 10) the Consumer can hold 330 received
messages, of which 300 wait in buffers while their visibility clock is already running.
The visibility extension goroutine starts only when a worker begins the message and its
first tick is measured from that point, so a message that waited in a buffer can expire
before its first extension. An expired message is received again, processed twice, and
counts toward the queue's `maxReceiveCount`, so it can be redriven to the DLQ without its
handler ever having failed. In Parallel mode the worker is chosen at random and in
PerGroupID mode by a fixed hash, so a single slow worker or a hash collision blocks the
polling loop for the whole route. Every in-flight message also owns a goroutine and a
ticker, and every extension is a separate `ChangeMessageVisibility` request.

The feature changes four things:

1. **Credit-based admission.** The Consumer holds at most `WorkerPoolSize + Prefetch`
   received-but-unfinished messages. A Poller acquires credits before each receive and
   asks only for as many messages as it holds credits for.
2. **Concurrent Pollers.** A route can run more than one Poller; all Pollers share the
   same credits.
3. **Non-blocking dispatch.** Parallel mode uses one shared work queue that any idle
   worker drains. PerGroupID mode uses a Group_Scheduler that assigns any Ready_Group to
   any idle worker while keeping at most one message per Group_Key in progress.
4. **Centralized, batched visibility management.** One Visibility_Manager per Consumer
   tracks the Visibility_Deadline of every Tracked_Message from its Receive_Time,
   extends only messages close to their deadline, sends extensions through
   `ChangeMessageVisibilityBatch` when the client supports it, and releases unstarted
   messages on graceful shutdown.

It also fixes `ExtensionLimit` so that a limit of `n` allows exactly `n` extensions; today
it allows `n + 1`.

Existing exported signatures do not change. `consumer.SQSClient` keeps its method set;
batched visibility changes are detected through a separate optional interface, as was
done for batched deletes. The change is released as a minor version.

Out of scope:

- A per-route processing timeout for handlers (`router.WithProcessingTimeout`).
- New metrics or new methods on `consumer.MetricsRecorder`.
- Changes to the Scheduled_Retry_Model outcome logic, the observe-only DLQ path, or the
  Delete_Batcher.
- Changes to the `producer`, `broker`, or `typed` packages beyond documentation.
- Configurable fixed worker affinity for PerGroupID mode (the hash-based assignment is
  removed, not kept as an option).

Accepted tradeoffs:

- `MaxMessages` keeps its meaning as the upper bound of one receive request but no longer
  sizes any internal buffer.
- A Poller waits until it holds a full Receive_Batch of credits before receiving. This
  keeps the number of receive requests per message equal to today's under backlog, at the
  cost of starting a receive slightly later than a per-credit design would.
- A Backoff_Request is applied when the handler returns, not at the moment the handler
  calls `Backoff`.
- On graceful shutdown, messages that were received but not started are released for
  redelivery instead of being run with a canceled context.
- Fixing the `ExtensionLimit` off-by-one shortens the maximum processing window of routes
  that relied on the extra extension. This is documented in the changelog.
- In PerGroupID mode the Group_Scheduler serializes each Group_Key, so the tail of a
  Receive_Batch that holds many messages of one group still waits for its predecessors;
  the Visibility_Manager covers that wait through extensions.

## Glossary

- **Consumer**: The loafer-awsx component that polls one queue for one Route, dispatches
  each message to the handler through a worker pool, and applies the outcome of each
  handler invocation.
- **Route**: The per-queue configuration unit (`router.Route`).
- **Worker**: A goroutine of the Consumer that runs the middleware-wrapped handler for one
  message at a time. The number of Workers equals the Route `WorkerPoolSize`.
- **Poller**: A goroutine of the Consumer that issues `ReceiveMessage` requests.
- **Pollers_Option**: The route option `router.WithPollers(n int)` that sets the number of
  Pollers.
- **Prefetch**: The number of received messages the Consumer may hold beyond the number of
  Workers, set by the route option `router.WithPrefetch(n int)` (the Prefetch_Option).
- **Credit**: A unit of admission capacity. One Credit is held for each Admitted_Message.
- **Credit_Pool**: The Consumer component holding the Credits. Its capacity equals the
  Route `WorkerPoolSize` plus the Route Prefetch.
- **Receive_Batch**: The number of messages a Poller requests in one `ReceiveMessage`
  call. It equals the smaller of the Route `MaxMessages` and the Route Prefetch plus one.
- **Admitted_Message**: A message returned by `ReceiveMessage` whose Outcome has not been
  reached.
- **Pending_Message**: An Admitted_Message whose handler has not started.
- **Active_Message**: An Admitted_Message whose handler is running.
- **Outcome**: The terminal decision for an Admitted_Message: Commit, Leave, Backoff,
  Hold, Release, or any Scheduled_Retry_Model decision.
- **Commit**: The decision to remove a message from the queue (synchronous delete or
  Pending_Delete).
- **Leave**: The decision, under the Visibility_Retry_Model, to leave a failed message in
  the queue with its current visibility timeout.
- **Backoff_Request**: A call to `Message.Backoff(delay)` made by the handler.
- **Hold**: The decision, in PerGroupID mode under the Visibility_Retry_Model, to skip the
  handler of a Pending_Message because an earlier message of the same Group_Key failed,
  and to leave it in the queue.
- **Release**: The decision, during graceful shutdown, to make a Pending_Message visible
  again immediately by setting its visibility timeout to zero.
- **Group_Key**: The PerGroupID routing key built from the `MessageGroupId` system
  attribute and the Route custom group fields.
- **Group_Scheduler**: The PerGroupID component that orders Pending_Messages per Group_Key
  and assigns Group_Keys to Workers.
- **Ready_Group**: A Group_Key that has at least one Pending_Message and no Active_Message.
- **Shared_Work_Queue**: The Parallel-mode queue of Pending_Messages from which any idle
  Worker takes the next message.
- **Receive_Time**: The instant the `ReceiveMessage` response that carried a message was
  returned to the Poller.
- **Visibility_Timeout**: The Route `VisibilityTimeout` in seconds.
- **Visibility_Deadline**: The instant at which a Tracked_Message becomes visible again
  unless extended. It starts at Receive_Time plus Visibility_Timeout.
- **Renewal_Margin**: The fixed interval of 10 seconds before a Visibility_Deadline at
  which an extension becomes due.
- **Coalesce_Window**: The fixed interval of 5 seconds by which the Visibility_Manager may
  send an Extension ahead of its due instant so that it shares a batch request with an
  Extension that is already due.
- **Extension**: A visibility change that moves the Visibility_Deadline of a
  Tracked_Message later. The k-th Extension (k starting at 1) sets the visibility timeout
  to k multiplied by Visibility_Timeout, measured from the instant it is sent.
- **Extension_Limit**: The Route `ExtensionLimit`, the maximum number of Extensions per
  Tracked_Message.
- **Max_Visibility**: The AWS SQS upper bound of 43,200 seconds (12 hours) on a visibility
  timeout, which also bounds the total time a message can stay invisible counted from its
  Receive_Time.
- **Tracked_Message**: An Admitted_Message whose Visibility_Deadline the
  Visibility_Manager maintains.
- **Visibility_Manager**: The single per-Consumer component that tracks
  Visibility_Deadlines and sends Extensions, backoff changes, and Releases.
- **Batch_Visibility_Client**: An SQS client that implements
  `ChangeMessageVisibilityBatch`, expressed as the `consumer.BatchVisibilityClient`
  interface.
- **Max_Batch_Entries**: The AWS SQS limit of 10 entries (inclusive) per batch request.
- **Release_Timeout**: The upper bound on the duration of each visibility request sent
  during graceful shutdown.
- **Visibility_Retry_Model**: The default retry model, in which a failed message is left
  in the queue for redelivery.
- **Scheduled_Retry_Model**: The retry model in which failed messages are re-published
  through EventBridge Scheduler or published to a DLQ before the original is deleted.
- **Delete_Batcher**: The existing component that sends Pending_Deletes through
  `DeleteMessageBatch` on routes configured with `router.WithDeleteBatch()`.
- **Benchmark_Suite**: The separate Go module under `benchmarks/`.

## Requirements

### Requirement 1: Route options

**User Story:** As a library user, I want to configure prefetch and the number of pollers
per route, so that I can trade receive-ahead for visibility safety and scale receive
throughput independently of the worker count.

#### Acceptance Criteria

1. THE Route SHALL expose the Prefetch_Option as `router.WithPrefetch(n int)`.
2. THE Route SHALL report the configured Prefetch through `Route.Prefetch() int`.
3. WHEN a Route is created without the Prefetch_Option, THE Route SHALL report a Prefetch
   equal to the Route `MaxMessages`.
4. IF the Prefetch_Option receives a value outside the inclusive range [0, 1000], THEN THE
   Route constructor SHALL return an error matching `errors.ErrInvalidOption` with
   `errors.Is`.
5. THE Route SHALL accept a Prefetch of zero.
6. THE Route SHALL expose the Pollers_Option as `router.WithPollers(n int)`.
7. THE Route SHALL report the configured number of Pollers through `Route.Pollers() int`.
8. WHEN a Route is created without the Pollers_Option, THE Route SHALL report 1 Poller.
9. IF the Pollers_Option receives a value below 1, THEN THE Route constructor SHALL
   return an error matching `errors.ErrInvalidOption` with `errors.Is`.
10. THE Prefetch_Option and the Pollers_Option SHALL be accepted together with every run
    mode, every retry model, and `router.WithDeleteBatch()`.

### Requirement 2: Backward compatibility

**User Story:** As an existing library user, I want to upgrade without code changes, so
that I get the new flow control within v1.

#### Acceptance Criteria

1. THE `consumer.SQSClient` interface SHALL keep its current method set.
2. THE feature SHALL NOT change the signature of any existing exported symbol.
3. THE `consumer.Message` interface SHALL keep its current method set.
4. THE Consumer SHALL keep requesting every custom and system message attribute on each
   `ReceiveMessage` call.
5. THE Consumer SHALL keep setting `VisibilityTimeout` on each `ReceiveMessage` call to the
   Visibility_Timeout.
6. THE Consumer SHALL keep setting `WaitTimeSeconds` on each `ReceiveMessage` call to the
   Route `WaitTimeSeconds`.
7. WHILE the SQS client is not a Batch_Visibility_Client, THE Consumer SHALL run without
   error and send every visibility change through `ChangeMessageVisibility`.

### Requirement 3: Credit-based admission

**User Story:** As a library user, I want the consumer to receive messages only when it has
capacity to start them soon, so that messages do not expire while waiting in memory.

#### Acceptance Criteria

1. THE Credit_Pool SHALL have a capacity equal to the Route `WorkerPoolSize` plus the Route
   Prefetch.
2. THE Consumer SHALL NOT hold more Admitted_Messages than the Credit_Pool capacity.
3. WHEN a Poller is about to call `ReceiveMessage`, THE Poller SHALL first acquire a number
   of Credits equal to the Receive_Batch.
4. WHILE a Poller cannot acquire a Receive_Batch of Credits and no Worker is waiting with
   nothing to run, THE Poller SHALL NOT call `ReceiveMessage`.
5. THE Poller SHALL set `MaxNumberOfMessages` on each `ReceiveMessage` call to the number
   of Credits it acquired for that call.
6. WHEN a `ReceiveMessage` call returns fewer messages than the Credits acquired for it,
   THE Poller SHALL return the unused Credits to the Credit_Pool before its next acquire.
7. IF a `ReceiveMessage` call returns an error, THEN THE Poller SHALL return every Credit
   acquired for that call to the Credit_Pool.
8. WHEN an Admitted_Message reaches an Outcome, THE Consumer SHALL return exactly one
   Credit to the Credit_Pool.
9. THE Credit_Pool SHALL NOT report fewer than zero available Credits or more available
   Credits than its capacity.
10. WHILE waiting to acquire Credits, THE Poller SHALL return when the Consumer run context
    is canceled.
11. WHILE fewer Admitted_Messages than the Route `WorkerPoolSize` exist, THE Credit_Pool
    SHALL hold at least one Receive_Batch of available Credits.
12. WHILE a Worker is waiting with nothing to run and at least one Credit is available,
    THE Poller SHALL call `ReceiveMessage` with the available Credits, up to one
    Receive_Batch, without waiting for a full Receive_Batch.

### Requirement 4: Concurrent pollers

**User Story:** As a library user with fast handlers, I want several pollers per route, so
that receive latency does not cap throughput.

#### Acceptance Criteria

1. THE Consumer SHALL run a number of Pollers equal to the Route Pollers.
2. THE Pollers SHALL acquire Credits from one shared Credit_Pool.
3. IF a `ReceiveMessage` call of one Poller returns an error, THEN THE Consumer SHALL log
   the error and keep the other Pollers running.
4. WHEN a `ReceiveMessage` call returns an error and the Consumer run context is not
   canceled, THE Poller SHALL wait the configured retry timeout before its next acquire.
5. WHEN the Consumer run context is canceled, THE Consumer SHALL stop every Poller before
   `Consumer.Run` returns.

### Requirement 5: Parallel dispatch

**User Story:** As a library user in Parallel mode, I want any idle worker to take the next
message, so that one slow worker never blocks receiving for the route.

#### Acceptance Criteria

1. WHILE consuming a Route in Parallel mode, THE Consumer SHALL place every received
   message on the Shared_Work_Queue.
2. WHILE consuming a Route in Parallel mode, THE Worker that becomes idle SHALL take the
   next Pending_Message from the Shared_Work_Queue.
3. WHILE consuming a Route in Parallel mode, THE Poller SHALL NOT block on the state of
   any individual Worker.
4. THE Shared_Work_Queue SHALL accept every Admitted_Message without blocking the Poller.

### Requirement 6: PerGroupID dispatch

**User Story:** As a FIFO user, I want each message group processed in order by whichever
worker is free, so that unrelated groups never wait for each other and one slow group never
stalls the route.

#### Acceptance Criteria

1. WHILE consuming a Route in PerGroupID mode, THE Group_Scheduler SHALL keep at most one
   Active_Message per Group_Key.
2. WHILE consuming a Route in PerGroupID mode, THE Group_Scheduler SHALL start the
   Pending_Messages of one Group_Key in the order in which the Consumer received them.
3. WHEN a Worker becomes idle and a Ready_Group exists, THE Group_Scheduler SHALL assign
   the Worker a Pending_Message of a Ready_Group.
4. THE Group_Scheduler SHALL NOT assign a Group_Key to a fixed Worker.
5. WHILE consuming a Route in PerGroupID mode, THE Poller SHALL NOT block on the state of
   any individual Worker or Group_Key.
6. THE Group_Scheduler SHALL build the Group_Key from the `MessageGroupId` system attribute
   and the Route custom group fields, reading each custom field from the native SQS user
   message attributes first and from the SNS envelope attributes second.
7. WHEN a Group_Key has no Admitted_Message left, THE Group_Scheduler SHALL discard every
   state it keeps for that Group_Key.

### Requirement 7: Group hold on failure

**User Story:** As a FIFO user, I want the rest of a group skipped when one of its messages
fails, so that the group is redelivered in order instead of being processed out of order.

#### Acceptance Criteria

1. WHILE consuming a Route in PerGroupID mode under the Visibility_Retry_Model, WHEN a
   handler returns an error, THE Group_Scheduler SHALL Hold every Pending_Message of the
   same Group_Key.
2. WHILE consuming a Route in PerGroupID mode under the Visibility_Retry_Model, WHEN a
   handler makes a Backoff_Request, THE Group_Scheduler SHALL Hold every Pending_Message
   of the same Group_Key.
3. THE Consumer SHALL NOT run the handler of a held message.
4. THE Consumer SHALL NOT delete a held message.
5. WHERE the Route queue is not a FIFO queue, THE Consumer SHALL NOT send any visibility
   change for a held message. WHERE the Route queue is a FIFO queue (its URL ends in
   `.fifo`), THE Consumer SHALL Release every held message, so the group is retried when
   the failed message becomes visible rather than when a held message's extended
   visibility runs out.
6. WHEN a message is held, THE Consumer SHALL log one Warn record that includes the queue
   URL, the receipt handle, and the group ID.
7. WHILE consuming a Route under the Scheduled_Retry_Model, THE Group_Scheduler SHALL NOT
   Hold any message.
8. WHEN a `ReceiveMessage` call returns messages, THE Consumer SHALL submit every admitted
   message of that call to the Group_Scheduler in one atomic step, so that no handler of
   that call starts before every message of that call is Pending.

### Requirement 8: Visibility deadline tracking

**User Story:** As a library user, I want visibility extended based on when a message was
received, so that a message never expires while it waits or runs within the extension
limit.

#### Acceptance Criteria

1. WHEN a message is admitted, THE Visibility_Manager SHALL start tracking it with a
   Visibility_Deadline equal to its Receive_Time plus the Visibility_Timeout.
2. THE Visibility_Manager SHALL track every Pending_Message and every Active_Message.
3. WHILE fewer than Extension_Limit Extensions were sent for a Tracked_Message, THE
   Visibility_Manager SHALL send its next Extension no later than its Visibility_Deadline
   minus the Renewal_Margin.
4. THE Visibility_Manager SHALL NOT send more than Extension_Limit Extensions for one
   Tracked_Message.
5. WHERE Extension_Limit is zero, THE Visibility_Manager SHALL NOT send any Extension.
6. THE Visibility_Manager SHALL NOT send a visibility timeout that would keep a message
   invisible beyond its Receive_Time plus Max_Visibility.
7. WHEN an Extension request succeeds, THE Visibility_Manager SHALL move the
   Visibility_Deadline of the Tracked_Message to the instant the request was sent plus the
   visibility timeout it set.
8. WHEN an Admitted_Message reaches an Outcome, THE Visibility_Manager SHALL stop tracking
   it before any further Extension is sent for it.
9. WHEN the Consumer stops tracking a message, THE Visibility_Manager SHALL NOT include
   that message in any visibility request built afterwards, except a Release or a backoff
   change requested for that message.
10. THE Visibility_Manager SHALL NOT send an Extension for a message whose
    Visibility_Deadline is more than the Renewal_Margin plus the Coalesce_Window away.
11. THE Visibility_Manager SHALL run as one goroutine per Consumer, independent of the
    number of Tracked_Messages.
12. IF the Visibility_Deadline of a Pending_Message has passed when a Worker takes it, or
    the message has no Extension left and less than 1 second of visibility remains, THEN
    THE Consumer SHALL NOT run its handler, SHALL log one Warn record, and, under the
    Visibility_Retry_Model in PerGroupID mode, SHALL Hold the rest of its group. Under the
    Scheduled_Retry_Model the rest of the group is not held, consistent with that model
    unblocking groups on failure.
13. WHEN a handler returns, THE Consumer SHALL stop tracking the message before it sends
    the delete or applies any other outcome request.

### Requirement 9: Batched visibility changes

**User Story:** As a library user, I want visibility changes grouped into batch requests,
so that extensions cost fewer SQS requests.

#### Acceptance Criteria

1. THE consumer package SHALL define the `BatchVisibilityClient` interface with the
   `ChangeMessageVisibilityBatch` method signature of `*sqs.Client` from aws-sdk-go-v2.
2. THE `*sqs.Client` type SHALL satisfy `BatchVisibilityClient`, asserted at compile time.
3. WHERE the SQS client is a Batch_Visibility_Client, THE Visibility_Manager SHALL send
   Extensions, backoff changes, and Releases through `ChangeMessageVisibilityBatch`.
4. WHEN an Extension is due, THE Visibility_Manager SHALL include in the same batch
   requests every other Extension due within the Coalesce_Window whose Extension sent at
   that instant would move its Visibility_Deadline later, up to Max_Batch_Entries entries
   per request.
5. THE Visibility_Manager SHALL NOT include more than Max_Batch_Entries entries in one
   `ChangeMessageVisibilityBatch` request.
6. THE Visibility_Manager SHALL assign each entry of a batch request an `Id` that is unique
   within that request and consists only of characters AWS SQS accepts.
7. IF a `ChangeMessageVisibilityBatch` request returns an error, THEN THE
   Visibility_Manager SHALL log one Error record per entry of that request.
8. IF a `ChangeMessageVisibilityBatch` response lists an entry as failed, THEN THE
   Visibility_Manager SHALL log one Error record for that entry.
9. THE Error record for a failed visibility change SHALL include the queue URL, the receipt
   handle, and the requested visibility timeout.
10. IF an Extension fails, THEN THE Visibility_Manager SHALL NOT move the
    Visibility_Deadline of that Tracked_Message.
11. THE Visibility_Manager SHALL NOT panic on any `ChangeMessageVisibilityBatch` response,
    including a nil output or a failed entry whose `Id` does not match a sent entry.
12. THE Visibility_Manager SHALL bound each visibility request by a timeout.
13. THE Visibility_Manager SHALL send up to 8 visibility requests concurrently, and the
    fallback sender SHALL send the changes of one batch concurrently, so a slow request
    does not delay other Extensions past their Visibility_Deadline.

### Requirement 10: Backoff

**User Story:** As a library user, I want `Backoff` to keep working and never block my
handler, so that existing handlers behave the same under the new visibility management.

#### Acceptance Criteria

1. THE `Message.Backoff` method SHALL return without blocking, for any number of calls.
2. WHEN a handler makes more than one Backoff_Request, THE Consumer SHALL use the delay of
   the last Backoff_Request.
3. WHILE consuming a Route under the Visibility_Retry_Model, WHEN a handler that made a
   Backoff_Request returns, THE Visibility_Manager SHALL set the visibility timeout of the
   message to the requested delay in whole seconds, clamped to the inclusive range
   [0, Max_Visibility].
4. WHILE consuming a Route under the Visibility_Retry_Model, THE Consumer SHALL NOT delete
   a message whose handler made a Backoff_Request.
5. WHILE consuming a Route under the Scheduled_Retry_Model, THE Visibility_Manager SHALL
   NOT send a visibility change for a Backoff_Request.

### Requirement 11: Graceful shutdown

**User Story:** As an operator, I want unstarted messages returned to the queue at once on
shutdown, so that deploys do not delay those messages or their FIFO groups by a full
visibility timeout.

#### Acceptance Criteria

1. WHEN the Consumer run context is canceled, THE Consumer SHALL stop starting
   Pending_Messages.
2. WHEN the Consumer run context is canceled, THE Consumer SHALL Release every
   Pending_Message that is not held.
3. THE Consumer SHALL NOT run the handler of a released message.
4. WHEN the Consumer run context is canceled, THE Consumer SHALL wait for every
   Active_Message to reach its Outcome before `Consumer.Run` returns.
5. THE Visibility_Manager SHALL send Releases with a context that is not canceled by the
   Consumer run context.
6. THE Visibility_Manager SHALL bound each Release request by the Release_Timeout.
7. WHEN the Consumer run context is canceled, THE Consumer SHALL keep the existing
   Delete_Batcher flush behavior.
10. WHEN a handler returns after the Consumer run context was canceled, THE Consumer SHALL
    apply its outcome requests with a context that is not canceled and is bounded by a
    timeout.
8. WHEN `Consumer.Run` returns, THE Consumer SHALL have no running Poller, Worker,
   Visibility_Manager, or Delete_Batcher goroutine.
9. IF a Release fails, THEN THE Visibility_Manager SHALL log one Error record for the
   affected message and leave the message to become visible at its Visibility_Deadline.

### Requirement 12: Outcome parity

**User Story:** As a library user, I want handler outcomes to mean the same thing as before,
so that only flow control changes.

#### Acceptance Criteria

1. WHILE consuming a Route under the Visibility_Retry_Model, WHEN a handler succeeds, THE
   Consumer SHALL Commit the message.
2. WHILE consuming a Route under the Visibility_Retry_Model, WHEN a handler returns an
   error, THE Consumer SHALL Leave the message.
3. WHILE consuming a Route under the Visibility_Retry_Model, WHEN a handler returns an
   error, THE Consumer SHALL keep the existing observe-only DLQ signals and error log.
4. WHILE consuming a Route under the Scheduled_Retry_Model, THE Consumer SHALL keep the
   existing schedule, DLQ publish, delete, and metric behavior.
5. WHILE consuming a Route with `router.WithDeleteBatch()`, THE Delete_Batcher SHALL keep a
   Pending_Delete queue capacity equal to the Route `WorkerPoolSize` multiplied by the
   Route `MaxMessages`.
6. THE Consumer SHALL pass the Consumer run context to every handler invocation.

### Requirement 13: Test doubles

**User Story:** As a library user writing tests, I want the fake SQS client to support
batched visibility changes, so that I can test routes without AWS.

#### Acceptance Criteria

1. THE `fake.SQSClient` type SHALL implement `ChangeMessageVisibilityBatch`.
2. THE `fake.SQSClient` type SHALL expose a `ChangeMessageVisibilityBatchFunc` field that,
   when set, determines the `ChangeMessageVisibilityBatch` result.
3. WHEN `ChangeMessageVisibilityBatchFunc` is not set, THE `fake.SQSClient` SHALL replay
   every entry of a `ChangeMessageVisibilityBatch` call through `ChangeMessageVisibility`
   and report the entries whose single call succeeded as successful.
4. THE `fake.SQSClient` type SHALL expose `ChangeMessageVisibilityBatchCalls()` returning a
   copy of the recorded inputs in call order.
5. THE `fake.SQSClient` type SHALL be safe for concurrent use.
6. WHEN `ChangeMessageVisibilityBatchFunc` is not set, THE `fake.SQSClient` SHALL NOT record
   the batch call in `ChangeMessageVisibilityBatchCalls`.

### Requirement 14: Benchmarks

**User Story:** As a maintainer, I want benchmarks that show the effect of flow control, so
that the documented gains are reproducible.

#### Acceptance Criteria

1. THE Benchmark_Suite SHALL include a scenario with 30 Workers, `MaxMessages` 10, and a
   non-zero simulated handler latency, in Parallel mode and in PerGroupID mode.
2. THE Benchmark_Suite SHALL report the number of visibility-change requests per message as
   a custom benchmark metric.
3. THE Benchmark_Suite SHALL report the mean interval between Receive_Time and handler
   start as a custom benchmark metric.
4. THE Benchmark_Suite SHALL report the number of `ReceiveMessage` requests per message as
   a custom benchmark metric.
5. THE Benchmark_Suite SHALL include a scenario with more than one Poller.
6. THE Benchmark_Suite in-memory client SHALL implement `ChangeMessageVisibilityBatch`.

### Requirement 15: Documentation

**User Story:** As a library user, I want the documentation to describe the new flow
control, so that I can size workers, prefetch, pollers, and the visibility timeout.

#### Acceptance Criteria

1. THE README Configuration Reference SHALL document the Prefetch_Option and the
   Pollers_Option with their defaults and bounds.
2. THE README SHALL include a section on flow control covering credit-based admission,
   dispatch per run mode, visibility deadline tracking, batched visibility changes, and
   shutdown behavior.
3. THE README SHALL give a sizing rule relating Visibility_Timeout, Extension_Limit,
   Prefetch, and handler duration.
4. THE README IAM permissions table SHALL state that `ChangeMessageVisibilityBatch` is
   authorized by the `sqs:ChangeMessageVisibility` action.
5. THE consumer package documentation SHALL describe the Pollers, the Credit_Pool, the
   dispatch per run mode, and the Visibility_Manager.
6. THE `router.Mode` documentation SHALL describe Parallel and PerGroupID dispatch as
   implemented by this feature.
7. THE `router.WithExtensionLimit` documentation SHALL state that a limit of `n` allows
   exactly `n` Extensions.
8. THE feature SHALL provide a doc comment for every new exported symbol.
9. THE README SHALL include upgrade notes listing every behavior change of this feature.

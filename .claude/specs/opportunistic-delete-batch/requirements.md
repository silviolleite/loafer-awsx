# Requirements Document

## Introduction

This feature adds an opt-in, per-route mode in which the Consumer removes successfully
processed messages from the queue through the AWS SQS `DeleteMessageBatch` API instead of
one `DeleteMessage` call per message. Batches are formed opportunistically: a Sender waits
for the first Pending_Delete, then takes whatever other Pending_Deletes are already queued
(up to the AWS limit of ten) without waiting, and sends the batch immediately. There is no
linger timer and no receive-cycle flush.

The motivation is twofold. Under high throughput, batching reduces the number of SQS
requests spent on deletes by up to ten times, which lowers request cost and relieves the
per-action API quota of FIFO queues that are not in high-throughput mode. The opportunistic
design was chosen over a linger-based design (as proposed upstream in JustCodes/loafer-go
PR #12) because a linger delay holds a FIFO Message_Group_Id until the delete is sent, and a
receive-cycle flush couples the delete of one group to the slowest handler of another group
in the same receive. With opportunistic batching, the only wait a Pending_Delete experiences
is the duration of a delete request already in flight.

The existing behavior is preserved unchanged and remains the default: without the option,
every delete is a Synchronous_Delete exactly as today. The feature is backward compatible
with v1: the `consumer.SQSClient` interface does not change, and batching capability is
detected through a separate optional interface. The change is released as a minor version.

Out of scope:

- A linger timer or any time-based flush trigger.
- Batching of `ChangeMessageVisibility` calls.
- Batching in the `producer` package.
- Changes to the `consumer.SQSClient` interface or to any existing exported signature.
- New metrics for delete outcomes.

Accepted tradeoffs:

- Under a Delete_Batch_Route the delete is asynchronous to the worker: the worker continues
  to the next message once the Pending_Delete is queued, not when the delete completes.
- Batch size depends on load. Under light load most batches carry one entry and the request
  savings are small; savings grow as the delete rate approaches the Sender capacity.
- If the process stops without a graceful shutdown, queued and in-flight Pending_Deletes
  are lost and the corresponding messages are redelivered. This stays within the existing
  at-least-once contract; handlers are expected to be idempotent.

## Glossary

- **Consumer**: The loafer-awsx component that polls one queue, dispatches each message to
  the handler through a worker pool, and applies the outcome of each handler invocation.
- **Route**: The per-queue configuration unit (`router.Route`) that binds a queue, a
  handler, and the options controlling consumption.
- **Delete_Batch_Option**: The route option `router.WithDeleteBatch()` that enables batched
  deletes for a Route.
- **Delete_Batch_Route**: A Route configured with the Delete_Batch_Option.
- **Standard_Route**: A Route configured without the Delete_Batch_Option.
- **Commit**: The Consumer decision to remove a message from the queue. Commits happen on
  handler success under the Visibility_Retry_Model, and after a successful retry schedule,
  a successful DLQ publish, or handler success under the Scheduled_Retry_Model.
- **Synchronous_Delete**: The existing Commit path that issues one `DeleteMessage` request
  per message from the worker that processed it.
- **Pending_Delete**: A committed message queued for removal by the Delete_Batcher and not
  yet included in a sent batch.
- **Delete_Batcher**: The Consumer component that owns the Pending_Delete queue and the
  Senders for a Delete_Batch_Route.
- **Sender**: A goroutine of the Delete_Batcher that turns Pending_Deletes into
  `DeleteMessageBatch` requests.
- **Opportunistic_Drain**: The Sender step that, after receiving one Pending_Delete, takes
  every other Pending_Delete already queued, without waiting, until the batch holds
  Max_Batch_Entries entries or the queue is empty.
- **Max_Batch_Entries**: The maximum number of entries in one `DeleteMessageBatch` request,
  fixed at 10 (inclusive) by AWS SQS.
- **Batch_Delete_Client**: An SQS client that implements the `DeleteMessageBatch`
  operation, expressed as the `consumer.BatchDeleteClient` interface.
- **Flush_Timeout**: The upper bound on the duration of each `DeleteMessageBatch` request
  sent during graceful shutdown.
- **Visibility_Retry_Model**: The default retry model, in which a failed message is left in
  the queue for redelivery.
- **Scheduled_Retry_Model**: The retry model in which failed messages are re-published
  through EventBridge Scheduler or published to a DLQ before the original is deleted.
- **Held_Message**: A message the Consumer holds back without running its handler because an
  earlier message of the same FIFO group failed in the same receive (PerGroupID mode under
  the Visibility_Retry_Model).
- **Benchmark_Suite**: The separate Go module under `benchmarks/`.

## Requirements

### Requirement 1: Opt-in route option

**User Story:** As a library user, I want to enable batched deletes per route, so that I can
adopt them only on the queues where they pay off.

#### Acceptance Criteria

1. THE Route SHALL expose the Delete_Batch_Option as `router.WithDeleteBatch()`, taking no
   arguments.
2. THE Route SHALL report whether the Delete_Batch_Option is set through
   `Route.DeleteBatch() bool`.
3. WHEN a Route is created without the Delete_Batch_Option, THE Route SHALL report
   `DeleteBatch()` as false.
4. WHEN a Route is created with the Delete_Batch_Option, THE Route SHALL report
   `DeleteBatch()` as true.
5. THE Delete_Batch_Option SHALL be accepted together with every Retry_Model and every run
   mode.

### Requirement 2: Backward compatibility

**User Story:** As an existing library user, I want my current routes and client
implementations to keep working unchanged, so that I can upgrade within v1 safely.

#### Acceptance Criteria

1. THE `consumer.SQSClient` interface SHALL keep its current method set.
2. WHILE consuming a Standard_Route, THE Consumer SHALL perform every Commit as a
   Synchronous_Delete.
3. WHILE consuming a Standard_Route, THE Consumer SHALL NOT call `DeleteMessageBatch`.
4. WHILE consuming a Standard_Route, THE Consumer SHALL NOT require the SQS client to be a
   Batch_Delete_Client.
5. THE feature SHALL NOT change the signature of any existing exported symbol.

### Requirement 3: Client capability check

**User Story:** As a library user, I want a clear error when my client cannot batch deletes,
so that a misconfiguration fails at startup instead of silently changing behavior.

#### Acceptance Criteria

1. THE consumer package SHALL define the `BatchDeleteClient` interface with the
   `DeleteMessageBatch` method signature of `*sqs.Client` from aws-sdk-go-v2.
2. THE `*sqs.Client` type SHALL satisfy `BatchDeleteClient`, asserted at compile time.
3. THE errors package SHALL define the sentinel `ErrDeleteBatchUnsupported`.
4. IF a Delete_Batch_Route is run with an SQS client that is not a Batch_Delete_Client,
   THEN THE Consumer SHALL return an error matching `errors.ErrDeleteBatchUnsupported` with
   `errors.Is`.
5. IF a Delete_Batch_Route is run with an SQS client that is not a Batch_Delete_Client,
   THEN THE Consumer SHALL return before resolving the queue URL.
6. IF a Delete_Batch_Route is run with an SQS client that is not a Batch_Delete_Client,
   THEN THE Consumer SHALL NOT start any goroutine.

### Requirement 4: Opportunistic batching

**User Story:** As a library user, I want deletes to be grouped without any added delay, so
that I save requests under load without slowing down FIFO message groups.

#### Acceptance Criteria

1. WHILE consuming a Delete_Batch_Route, THE Consumer SHALL turn every Commit into a
   Pending_Delete handed to the Delete_Batcher.
2. WHILE consuming a Delete_Batch_Route, THE Consumer SHALL NOT call `DeleteMessage`.
3. WHEN a Sender receives a Pending_Delete, THE Sender SHALL perform an Opportunistic_Drain.
4. WHEN an Opportunistic_Drain ends, THE Sender SHALL send the collected Pending_Deletes in
   one `DeleteMessageBatch` request without waiting for further Pending_Deletes.
5. THE Delete_Batcher SHALL NOT include more than Max_Batch_Entries entries in one
   `DeleteMessageBatch` request.
6. THE Delete_Batcher SHALL assign each entry of a `DeleteMessageBatch` request an `Id`
   that is unique within that request and consists only of characters AWS SQS accepts.
7. THE Delete_Batcher SHALL include each Pending_Delete in exactly one sent entry.
8. THE Delete_Batcher SHALL send each entry with the receipt handle of the message it
   represents and the URL of the Route queue.
9. THE Delete_Batcher SHALL NOT use any timer to decide when to send a batch.

### Requirement 5: Delete throughput floor

**User Story:** As a library user, I want enabling batched deletes to never reduce delete
throughput, so that the option is safe to turn on for busy queues.

#### Acceptance Criteria

1. THE Delete_Batcher SHALL run a number of Senders equal to the Route worker pool size.
2. WHEN every Sender has a request in flight, THE Delete_Batcher SHALL keep accepting
   Pending_Deletes until its queue capacity is reached.
3. WHILE the Pending_Delete queue is full, THE Consumer SHALL block the committing worker
   until capacity is available.
4. THE Pending_Delete queue capacity SHALL equal the Route worker pool size multiplied by
   the Route max messages per receive.

### Requirement 6: Delete failures

**User Story:** As an operator, I want failed batched deletes reported the same way as
failed single deletes, so that my alerts and runbooks keep working.

#### Acceptance Criteria

1. IF a `DeleteMessageBatch` request returns an error, THEN THE Delete_Batcher SHALL log
   one Error record per entry of that request.
2. IF a `DeleteMessageBatch` response lists an entry as failed, THEN THE Delete_Batcher
   SHALL log one Error record for that entry.
3. THE Error record for a failed entry SHALL include the queue URL and the receipt handle.
4. WHERE a failure is reported per entry, THE Error record SHALL include the AWS error code
   and the sender-fault flag.
5. THE Delete_Batcher SHALL use the message text "failed to delete message" for every
   delete failure record.
6. IF a delete fails, THEN THE Delete_Batcher SHALL leave the message in the queue for
   redelivery after its visibility timeout.
7. THE Delete_Batcher SHALL NOT panic on any `DeleteMessageBatch` response, including a nil
   output or a failed entry whose `Id` does not match a sent entry.
8. THE Delete_Batcher SHALL NOT log any Error record for an entry listed as successful.

### Requirement 7: Graceful shutdown

**User Story:** As a library user, I want pending deletes sent before the consumer stops, so
that a graceful shutdown does not cause avoidable redeliveries.

#### Acceptance Criteria

1. WHEN the Consumer run context is canceled, THE Delete_Batcher SHALL keep accepting
   Pending_Deletes until every worker has exited.
2. WHEN every worker has exited, THE Delete_Batcher SHALL send every remaining
   Pending_Delete before `Consumer.Run` returns.
3. THE Delete_Batcher SHALL send requests with a context that is not canceled by the
   Consumer run context.
4. WHILE the Consumer is shutting down, THE Delete_Batcher SHALL bound each
   `DeleteMessageBatch` request by the Flush_Timeout.
5. WHEN `Consumer.Run` returns, THE Consumer SHALL have no running Sender goroutine.

### Requirement 8: FIFO and retry model interaction

**User Story:** As a FIFO user, I want batched deletes to keep group ordering guarantees, so
that enabling the option does not change which messages are processed or deleted.

#### Acceptance Criteria

1. WHILE consuming a Delete_Batch_Route in PerGroupID mode under the
   Visibility_Retry_Model, THE Consumer SHALL NOT turn a Held_Message into a Pending_Delete.
2. WHILE consuming a Delete_Batch_Route, THE Consumer SHALL NOT turn a message into a
   Pending_Delete when its handler returned an error under the Visibility_Retry_Model.
3. WHILE consuming a Delete_Batch_Route, THE Consumer SHALL NOT turn a message into a
   Pending_Delete when its handler requested a backoff under the Visibility_Retry_Model.
4. WHILE consuming a Delete_Batch_Route under the Scheduled_Retry_Model, THE Consumer SHALL
   turn a message into a Pending_Delete only after the retry schedule or DLQ publish for
   that message succeeded, or after its handler succeeded.

### Requirement 9: Observability parity

**User Story:** As an operator, I want metrics and logs to mean the same thing with and
without batched deletes, so that dashboards stay comparable.

#### Acceptance Criteria

1. WHILE consuming a Delete_Batch_Route, THE Consumer SHALL emit the success, retry, and
   dead-letter metrics under the same conditions as on a Standard_Route.
2. WHILE consuming a Delete_Batch_Route, THE Consumer SHALL emit the observe-only DLQ
   signals under the same conditions as on a Standard_Route.

### Requirement 10: Test doubles

**User Story:** As a library user writing tests, I want the fake SQS client to support batch
deletes, so that I can test Delete_Batch_Routes without AWS.

#### Acceptance Criteria

1. THE `fake.SQSClient` type SHALL implement `DeleteMessageBatch`.
2. THE `fake.SQSClient` type SHALL expose a `DeleteMessageBatchFunc` field that, when set,
   determines the `DeleteMessageBatch` result.
3. WHEN `DeleteMessageBatchFunc` is not set, THE `fake.SQSClient` SHALL report every entry
   of a `DeleteMessageBatch` call as successful.
4. THE `fake.SQSClient` type SHALL expose `DeleteMessageBatchCalls()` returning a copy of
   the recorded inputs in call order.
5. THE `fake.SQSClient` type SHALL be safe for concurrent use by multiple Senders.

### Requirement 11: Benchmarks

**User Story:** As a maintainer, I want benchmarks that compare synchronous and batched
deletes, so that the documented gains are reproducible.

#### Acceptance Criteria

1. THE Benchmark_Suite SHALL include benchmarks for a Delete_Batch_Route on a standard queue
   and on a FIFO queue in PerGroupID mode.
2. THE Benchmark_Suite SHALL run each delete benchmark with a zero simulated delete latency
   and with a non-zero simulated delete latency.
3. THE Benchmark_Suite SHALL run the same latency scenarios for a Standard_Route, so that
   synchronous and batched deletes are compared under identical conditions.
4. THE Benchmark_Suite SHALL report the number of delete requests per message as a custom
   benchmark metric.
5. THE Benchmark_Suite SHALL report the average number of entries per delete request as a
   custom benchmark metric.
6. THE Benchmark_Suite in-memory client SHALL count a message as processed when it is
   removed by either `DeleteMessage` or `DeleteMessageBatch`.
7. THE Benchmark_Suite SHALL keep the existing loafer-awsx and loafer-go benchmarks
   unchanged.

### Requirement 12: Documentation

**User Story:** As a library user, I want the documentation to explain batched deletes, so
that I can decide when to enable them and what to expect.

#### Acceptance Criteria

1. THE README SHALL document the Delete_Batch_Option in the Configuration Reference.
2. THE README SHALL include a section on batched deletes covering how batches form, when
   the option pays off, the FIFO latency behavior, the client capability requirement, and
   shutdown behavior.
3. THE README IAM permissions table SHALL state that `DeleteMessageBatch` is authorized by
   the `sqs:DeleteMessage` action.
4. THE README Benchmarks section SHALL include the batched-delete results and the
   reproduction command.
5. THE consumer package documentation SHALL describe the batched delete path.
6. THE router package documentation SHALL mention the Delete_Batch_Option.
7. THE feature SHALL provide a doc comment for every new exported symbol.

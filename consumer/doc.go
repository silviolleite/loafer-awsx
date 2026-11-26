// Package consumer implements the SQS polling loop, worker-pool dispatch,
// visibility-timeout management, and the message commit/backoff lifecycle for a
// single queue.
//
// # Flow control
//
// A consumer holds at most WorkerPoolSize plus Prefetch received messages that
// have not reached an outcome (see [router.WithPrefetch]). Each of the route's
// pollers ([router.WithPollers]) acquires min(MaxMessages, Prefetch+1) credits
// from a shared credit pool before every ReceiveMessage call and asks for
// exactly that many messages; the Prefetch+1 bound means a receive can start
// whenever a worker is idle. Credits the response does not use are returned at
// once, and each admitted message returns its credit when it reaches an
// outcome. Pollers therefore never receive messages the workers
// cannot start soon, and they never wait on a particular worker.
//
// In Parallel mode every worker drains one shared queue. In PerGroupID mode a
// group scheduler keeps a FIFO of pending messages per group key, runs at most
// one message per group at a time, and hands any ready group to any idle
// worker. Under the Visibility retry model, a handler error or backoff holds
// every pending message of the same group: its handler does not run and it is
// left in the queue, so SQS redelivers the group in order.
//
// A single visibility manager per consumer tracks the visibility deadline of
// every admitted message from the instant it was received. Extensions become
// due ten seconds before the deadline and are sent in
// ChangeMessageVisibilityBatch requests of up to ten entries, together with any
// extension due within the next five seconds, when the client implements
// [BatchVisibilityClient]; otherwise each change is sent through
// ChangeMessageVisibility. Backoff changes and shutdown releases go through the
// same path. A message that finishes before its first extension is due costs no
// visibility request.
//
// On shutdown the pollers stop, running handlers finish, and every admitted
// message that has not started is released with a visibility timeout of zero.
//
// # Scheduled Retry consumption path
//
// When a route selects the Scheduled Retry model (see the router package), the
// consumer follows a different failure path from the default visibility-timeout
// model. On a handler failure it increments the message's retry count and,
// while the count stays within the configured maximum, creates a one-time
// delayed schedule that re-publishes the message to its Entry_Queue after the
// computed backoff delay; once the maximum is exceeded it publishes the message
// to the configured DLQ instead. In both cases the original message is deleted
// only after the schedule or DLQ publish succeeds, so the message group is
// unblocked without risking message loss.
//
// On success the consumer simply deletes the message and records the success
// outcome. It performs no success-side publishing: forwarding a successfully
// handled message onward is the handler's responsibility.
//
// # Scheduled Retry dependencies
//
// SchedulerClient is the minimal AWS EventBridge Scheduler surface the consumer
// uses to create retry schedules; a concrete *scheduler.Client satisfies it.
// WithSchedulerClient wires the client for routes that select the Scheduled
// Retry model. The DLQ publish path reuses the SQSClient SendMessage operation.
//
// # Batched deletes
//
// By default every committed message is removed with its own synchronous
// DeleteMessage call. A route configured with [router.WithDeleteBatch] instead
// hands each committed receipt handle to a per-consumer Delete_Batcher, which
// removes messages through DeleteMessageBatch requests. The committing worker
// does not wait for the request to complete.
//
// Batches form opportunistically. The Delete_Batcher runs one sender goroutine
// per worker; each sender blocks until a receipt handle arrives, takes whatever
// other handles are already pending, up to the AWS limit of ten, without
// waiting, and sends one request. No timer is used and no delay is ever added
// to fill a batch, so batches grow only while earlier requests are in flight.
// The pending-delete queue is bounded; when it is full, committing workers
// block until a sender frees space, which applies backpressure to consumption.
//
// Each request is bounded by its own timeout. A failed request or a failed
// entry is logged once per affected message and never retried: the message
// becomes visible again after its visibility timeout and is redelivered.
//
// Batched deletes require an SQS client that also implements the optional
// [BatchDeleteClient] interface; a concrete *sqs.Client does. The interface is
// kept separate from [SQSClient] so existing implementations remain valid. When
// the route enables batched deletes but the client does not implement it,
// [Consumer.Run] fails fast with [errors.ErrDeleteBatchUnsupported] before
// resolving the queue URL or starting any goroutine.
//
// On shutdown the pending deletes are flushed, not dropped. After ctx is
// canceled and every worker has exited, the Delete_Batcher sends every
// remaining receipt handle and waits for its senders to exit before
// [Consumer.Run] returns. These final requests are not canceled by ctx; each
// is still bounded by its own timeout, so a stalled request cannot block
// shutdown indefinitely.
//
// # Metrics
//
// MetricsRecorder is a single observe-only interface, wired through WithMetrics,
// whose methods the consumer invokes once per corresponding outcome: IncDLQ for
// a message detected as exhausted under the observe-only DLQ path, and
// IncSuccess, IncRetry, and IncDeadLetter for a handled-and-deleted message, a
// successfully created retry schedule, and a successful DLQ publish under the
// Scheduled Retry model. The recorder is expected to be backed by counters
// registered by the Metrics middleware and is supplied only when metrics are
// enabled. A nil recorder is ignored, so callers never have to guard against nil
// and the counters are left untouched.
package consumer

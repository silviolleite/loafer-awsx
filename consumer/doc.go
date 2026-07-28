// Package consumer implements the SQS polling loop, worker-pool dispatch,
// visibility-timeout management, and the message commit/backoff lifecycle for a
// single queue.
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
// metric. It performs no success-side publishing: forwarding a successfully
// handled message onward is the handler's responsibility.
//
// # Scheduled Retry dependencies
//
// SchedulerClient is the minimal AWS EventBridge Scheduler surface the consumer
// uses to create retry schedules; a concrete *scheduler.Client satisfies it.
// WithSchedulerClient wires the client for routes that select the Scheduled
// Retry model. The DLQ publish path reuses the SQSClient SendMessage operation.
//
// # Scheduled Retry metrics
//
// SuccessMetric, RetryMetric, and DeadLetterMetric are observe-only hooks, each
// taking the route name, that the consumer invokes once per corresponding
// outcome under the Scheduled Retry model: a handled-and-deleted message, a
// successfully created retry schedule, and a successful DLQ publish. They are
// wired through WithSuccessMetric, WithRetryMetric, and WithDeadLetterMetric,
// are expected to be backed by counters registered by the Metrics middleware,
// and are supplied only when metrics are enabled. A nil hook is ignored, so
// callers never have to guard against nil and the corresponding counter is left
// untouched.
package consumer

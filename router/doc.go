// Package router defines a Route as the binding between a queue name, a
// handler, a middleware chain, and route-level configuration.
//
// Routes are pure configuration values with no runtime behavior, built via a
// functional-options API.
//
// # Retry models
//
// Each route selects one retry model through RetryModel, exactly one of
// VisibilityRetryModel or ScheduledRetryModel. WithRetryModel sets the model
// explicitly.
//
// VisibilityRetryModel is the default. A failed message is left in the
// Entry_Queue and its visibility timeout is extended until it succeeds or is
// redriven natively by AWS SQS.
//
// ScheduledRetryModel is the "fat consumer" model. On failure the consumer
// increments the retry count, schedules a delayed re-publish through AWS
// EventBridge Scheduler, and deletes the original message so the message group
// is unblocked immediately.
//
// # Configuring the Scheduled Retry model
//
// WithScheduledRetry selects ScheduledRetryModel and attaches a validated
// ScheduledRetryConfig assembled from sub-options. The assembled configuration
// is validated at construction; any error wraps errors.ErrScheduledRetryConfig
// and names the offending value, and the route is not built on failure. The
// sub-options are:
//
//   - WithSchedulerIdentity sets the required EventBridge Scheduler identity:
//     the target Entry_Queue ARN and the execution role ARN the scheduler
//     assumes to re-publish the message.
//   - WithScheduledDLQ sets the required DLQ destination queue URL for messages
//     whose retry count exceeds the configured maximum.
//   - WithMaxRetryCount sets the inclusive threshold, within [0, 2147483647],
//     after which a message is routed to the DLQ instead of being rescheduled.
//   - WithBackoff sets the base and maximum backoff delay, both within
//     [1ms, 86,400,000ms] (24h) with max at or above base. The base delay
//     defaults to 1000ms when left unset.
//
// ScheduledRetryConfig is the resulting validated configuration; it is nil for
// routes using the Visibility model. Selecting WithScheduledRetry together with
// the observe-only WithDLQ on the same route is a configuration error,
// regardless of the order the options are applied.
package router

package router

import "time"

// RetryModel selects the per-route retry behavior. It is exactly one of
// VisibilityRetryModel or ScheduledRetryModel.
type RetryModel int

const (
	// VisibilityRetryModel is the default retry model. A failed message is left
	// in the Entry_Queue and its visibility timeout is extended until it
	// succeeds or is redriven natively by AWS SQS.
	VisibilityRetryModel RetryModel = iota

	// ScheduledRetryModel is the "fat consumer" retry model. The consumer
	// increments the retry count, schedules a delayed re-publish through AWS
	// EventBridge Scheduler, and deletes the original message so the message
	// group is unblocked immediately.
	ScheduledRetryModel
)

// ScheduledRetryConfig is the validated per-route configuration for the
// Scheduled Retry model. It is nil for routes using the Visibility model.
type ScheduledRetryConfig struct {
	// TargetQueueARN is the EventBridge Scheduler target (Entry_Queue) ARN.
	TargetQueueARN string
	// ExecutionRoleARN is the role the scheduler assumes to send the message.
	ExecutionRoleARN string
	// DLQQueueURL is the DLQ destination for exhausted messages.
	DLQQueueURL string
	// MaxRetryCount is the inclusive threshold before DLQ routing.
	MaxRetryCount int
	// BaseBackoff is the first-retry delay; it defaults to 1s.
	BaseBackoff time.Duration
	// MaxBackoff is the upper clamp on the backoff delay; it is at most 24h.
	MaxBackoff time.Duration
}

// ScheduledRetryOption configures a ScheduledRetryConfig. Options are applied in
// order before the assembled configuration is validated at route construction.
type ScheduledRetryOption func(*ScheduledRetryConfig)

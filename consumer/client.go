package consumer

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// SQSClient defines the minimal set of AWS SQS operations the consumer relies
// on. It mirrors the aws-sdk-go-v2 sqs.Client method signatures so a concrete
// *sqs.Client satisfies it directly, while allowing fakes in tests.
type SQSClient interface {
	// ReceiveMessage retrieves one or more messages from the specified queue.
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	// DeleteMessage removes a message from the specified queue.
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	// ChangeMessageVisibility changes the visibility timeout of a message.
	ChangeMessageVisibility(
		ctx context.Context,
		params *sqs.ChangeMessageVisibilityInput,
		optFns ...func(*sqs.Options),
	) (*sqs.ChangeMessageVisibilityOutput, error)
	// GetQueueUrl resolves the URL of an existing queue by name.
	GetQueueUrl(ctx context.Context, params *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)
	// SendMessage delivers a message to the specified queue. It is used by the
	// Scheduled Retry model's DLQ publisher to forward exhausted messages.
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

// SchedulerClient defines the minimal EventBridge Scheduler surface the
// consumer relies on for the Scheduled Retry model. It mirrors the
// aws-sdk-go-v2 scheduler.Client method signatures so a concrete
// *scheduler.Client satisfies it directly, while allowing fakes in tests.
type SchedulerClient interface {
	// CreateSchedule creates a one-time schedule that re-publishes a failed
	// message to its Entry_Queue after the computed backoff delay.
	CreateSchedule(
		ctx context.Context,
		params *scheduler.CreateScheduleInput,
		optFns ...func(*scheduler.Options),
	) (*scheduler.CreateScheduleOutput, error)
}

// Compile-time assertions that the concrete aws-sdk-go-v2 clients satisfy the
// consumer's client interfaces.
var (
	_ SQSClient       = (*sqs.Client)(nil)
	_ SchedulerClient = (*scheduler.Client)(nil)
)

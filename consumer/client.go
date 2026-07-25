package consumer

import (
	"context"

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
}

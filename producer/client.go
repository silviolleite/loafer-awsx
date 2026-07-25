package producer

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sns"
)

// SNSClient defines the minimal set of AWS SNS operations the producer relies
// on. It mirrors the aws-sdk-go-v2 sns.Client method signatures so a concrete
// *sns.Client satisfies it directly, while allowing fakes in tests.
type SNSClient interface {
	// Publish sends a single message to an SNS topic or target.
	Publish(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error)
	// PublishBatch sends up to ten messages to an SNS topic in a single request.
	PublishBatch(ctx context.Context, params *sns.PublishBatchInput, optFns ...func(*sns.Options)) (*sns.PublishBatchOutput, error)
}

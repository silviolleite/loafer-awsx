package consumer

import (
	"context"
	"sort"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/silviolleite/loafer-awsx/errors"
	"github.com/silviolleite/loafer-awsx/idgen"
)

const (
	// maxMessageAttributes is the SQS limit on the number of message
	// attributes a single message may carry.
	maxMessageAttributes = 10

	// numberDataType is the SQS message-attribute data type used for the
	// numeric retry_count value.
	numberDataType = "Number"

	// stringDataType is the SQS message-attribute data type used for the
	// preserved user attributes.
	stringDataType = "String"
)

// dlqPublisher forwards messages whose retry count has exceeded the configured
// threshold to the dead-letter queue under the Scheduled Retry model. It owns
// the SQS client used to send, the resolved DLQ queue URL, and a deduplication
// ID generator used to assign a fresh MessageDeduplicationId for a FIFO DLQ.
type dlqPublisher struct {
	client   SQSClient
	dedup    idgen.DeduplicationIDGenerator
	queueURL string
}

// newDLQPublisher builds a dlqPublisher that sends to queueURL using client and
// assigns fresh deduplication IDs from dedup when publishing to a FIFO DLQ.
func newDLQPublisher(client SQSClient, queueURL string, dedup idgen.DeduplicationIDGenerator) *dlqPublisher {
	return &dlqPublisher{
		client:   client,
		queueURL: queueURL,
		dedup:    dedup,
	}
}

// publish sends msg to the configured DLQ, preserving the original body and all
// user message attributes and carrying the final retry_count (next). For a FIFO
// DLQ it reuses the original MessageGroupId and assigns a freshly generated
// MessageDeduplicationId. It returns an error wrapping errors.ErrDLQPublish when
// the deduplication ID cannot be generated or the send fails, so the caller
// retains the original message.
func (p *dlqPublisher) publish(ctx context.Context, msg *message, next int) error {
	input := &sqs.SendMessageInput{
		QueueUrl:          aws.String(p.queueURL),
		MessageBody:       aws.String(string(msg.Body())),
		MessageAttributes: buildDLQAttributes(msg.UserMessageAttributes(), next),
	}

	if groupID := msg.SystemAttributeByKey(messageGroupIDKey); groupID != "" {
		dedupID, err := p.dedup.Generate(ctx, msg.UserMessageAttributes())
		if err != nil {
			return errors.Wrap(errors.ErrDLQPublish, err)
		}

		input.MessageGroupId = aws.String(groupID)
		input.MessageDeduplicationId = aws.String(dedupID)
	}

	if _, err := p.client.SendMessage(ctx, input); err != nil {
		return errors.Wrap(errors.ErrDLQPublish, err)
	}

	return nil
}

// buildDLQAttributes assembles the SQS message attributes for a DLQ publish. It
// always includes the retry_count attribute set to next, then adds the user
// attributes in deterministic key order until the SQS limit of ten attributes
// is reached, so retry_count is never dropped by the cap.
func buildDLQAttributes(userAttrs map[string]string, next int) map[string]types.MessageAttributeValue {
	attrs := make(map[string]types.MessageAttributeValue, maxMessageAttributes)
	attrs[retryCountAttribute] = types.MessageAttributeValue{
		DataType:    aws.String(numberDataType),
		StringValue: aws.String(strconv.Itoa(next)),
	}

	keys := make([]string, 0, len(userAttrs))
	for k := range userAttrs {
		if k == retryCountAttribute {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		if len(attrs) >= maxMessageAttributes {
			break
		}
		attrs[k] = types.MessageAttributeValue{
			DataType:    aws.String(stringDataType),
			StringValue: aws.String(userAttrs[k]),
		}
	}

	return attrs
}

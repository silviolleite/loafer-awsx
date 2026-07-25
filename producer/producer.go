package producer

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"

	"github.com/silviolleite/loafer-awsx/errors"
	"github.com/silviolleite/loafer-awsx/idgen"
)

// stringDataType is the SNS message attribute data type used for all string
// attributes emitted by the producer.
const stringDataType = "String"

// fifoSuffix is the suffix SNS requires on FIFO topic names and, therefore, on
// their ARNs.
const fifoSuffix = ".fifo"

// isFIFOTopic reports whether topicARN denotes a FIFO SNS topic, detected by
// the required ".fifo" suffix. MessageGroupId and MessageDeduplicationId are
// only valid for FIFO topics; standard topics reject them with an
// InvalidParameter error.
func isFIFOTopic(topicARN string) bool {
	return strings.HasSuffix(topicARN, fifoSuffix)
}

// PublishInput holds the data for a single SNS publish. Only TopicARN and
// Message are required; GroupID and DeduplicationID apply to FIFO topics, and
// Attributes are sent as SNS message attributes with the String data type.
type PublishInput struct {
	Attributes      map[string]string
	TopicARN        string
	Message         string
	GroupID         string
	DeduplicationID string
}

// Producer publishes messages to SNS topics. It holds an immutable SNSClient
// and optional ID generators after construction and carries no mutable shared
// state, so a single Producer is safe for concurrent use by multiple
// goroutines.
type Producer struct {
	client     SNSClient
	groupIDGen idgen.GroupIDGenerator
	dedupIDGen idgen.DeduplicationIDGenerator
}

// New creates a Producer with the given SNS client and options. Options are
// applied in order and nil options are ignored.
//
// New returns errors.ErrNoSNSClient when client is nil so callers fail at
// construction time rather than panicking on the first Publish.
func New(client SNSClient, opts ...Option) (*Producer, error) {
	if client == nil {
		return nil, errors.ErrNoSNSClient
	}

	p := &Producer{
		client: client,
	}

	for _, opt := range opts {
		if opt != nil {
			opt(p)
		}
	}

	return p, nil
}

// resolveIDs determines the MessageGroupId and MessageDeduplicationId for an
// entry. An explicitly provided value is always preserved as-is and its
// generator is not invoked. Auto-generation is gated on fifo: when fifo is
// false the generators are never invoked and the provided values are returned
// unchanged, so standard topics never receive auto-generated IDs. When fifo is
// true and a value is empty and the corresponding generator is configured, the
// value is generated from attributes; any generator error is returned wrapped
// with context and aborts the publish.
func (p *Producer) resolveIDs(
	ctx context.Context,
	fifo bool,
	groupID, dedupID string,
	attributes map[string]string,
) (resolvedGroupID, resolvedDedupID string, err error) {
	if !fifo {
		return groupID, dedupID, nil
	}

	if groupID == "" && p.groupIDGen != nil {
		generated, err := p.groupIDGen.Generate(ctx, attributes)
		if err != nil {
			return "", "", fmt.Errorf("failed to generate group id: %w", err)
		}

		groupID = generated
	}

	if dedupID == "" && p.dedupIDGen != nil {
		generated, err := p.dedupIDGen.Generate(ctx, attributes)
		if err != nil {
			return "", "", fmt.Errorf("failed to generate deduplication id: %w", err)
		}

		dedupID = generated
	}

	return groupID, dedupID, nil
}

// Publish sends a single message to an SNS topic and returns the SNS message
// ID on success.
//
// It returns errors.ErrEmptyInput when input is nil or its Message is empty.
// When GroupID is set it is sent as MessageGroupId (FIFO); when
// DeduplicationID is set it is sent as MessageDeduplicationId. Auto-generation
// only applies to FIFO topics, detected by a TopicARN ending in ".fifo": when
// the topic is FIFO and either value is empty and the corresponding generator
// has been configured (see WithGroupIDGenerator and
// WithDeduplicationIDGenerator), the value is auto-generated from Attributes
// and a generator error aborts the publish and is returned. For standard
// topics the generators are never invoked and MessageGroupId /
// MessageDeduplicationId are left unset unless the caller provided them
// explicitly. Attributes are sent as SNS message attributes with the String
// data type. Any SNS API failure is returned wrapped with context about the
// destination topic.
func (p *Producer) Publish(ctx context.Context, input *PublishInput) (string, error) {
	if input == nil || input.Message == "" {
		return "", errors.ErrEmptyInput
	}

	groupID, dedupID, err := p.resolveIDs(ctx, isFIFOTopic(input.TopicARN), input.GroupID, input.DeduplicationID, input.Attributes)
	if err != nil {
		return "", err
	}

	pubInp := &sns.PublishInput{
		Message:  aws.String(input.Message),
		TopicArn: aws.String(input.TopicARN),
	}

	if groupID != "" {
		pubInp.MessageGroupId = aws.String(groupID)
	}

	if dedupID != "" {
		pubInp.MessageDeduplicationId = aws.String(dedupID)
	}

	if len(input.Attributes) > 0 {
		pubInp.MessageAttributes = messageAttributes(input.Attributes)
	}

	out, err := p.client.Publish(ctx, pubInp)
	if err != nil {
		return "", fmt.Errorf("failed to publish message to topic %s: %w", input.TopicARN, err)
	}

	if out == nil || out.MessageId == nil {
		return "", nil
	}

	return *out.MessageId, nil
}

// messageAttributes converts a string map into SNS message attribute values,
// tagging every entry with the String data type.
func messageAttributes(attr map[string]string) map[string]types.MessageAttributeValue {
	ma := make(map[string]types.MessageAttributeValue, len(attr))
	for k, v := range attr {
		ma[k] = types.MessageAttributeValue{
			DataType:    aws.String(stringDataType),
			StringValue: aws.String(v),
		}
	}

	return ma
}

// maxBatchSize is the maximum number of messages SNS accepts in a single
// PublishBatch request.
const maxBatchSize = 10

// PublishBatchInput holds the data for a batch SNS publish. TopicARN is the
// destination topic and Messages holds up to maxBatchSize entries.
type PublishBatchInput struct {
	// TopicARN is the ARN of the destination SNS topic.
	TopicARN string
	// Messages holds the batch entries to publish (maximum of ten).
	Messages []*PublishBatchEntry
}

// PublishBatchEntry is a single entry in a batch publish. ID must be unique
// within the batch so the result can be correlated back to its request entry.
type PublishBatchEntry struct {
	Attributes      map[string]string
	ID              string
	Message         string
	GroupID         string
	DeduplicationID string
}

// PublishBatchOutput holds the results of a batch publish, partitioned into the
// entries SNS accepted and the entries it rejected.
type PublishBatchOutput struct {
	// Successful holds the entries SNS accepted.
	Successful []*PublishBatchSuccess
	// Failed holds the entries SNS rejected.
	Failed []*PublishBatchFailure
}

// PublishBatchSuccess is a successfully published entry, correlating the
// request EntryID with the SNS-assigned MessageID.
type PublishBatchSuccess struct {
	// EntryID is the request entry ID that succeeded.
	EntryID string
	// MessageID is the SNS-assigned message ID.
	MessageID string
}

// PublishBatchFailure is a rejected entry, correlating the request EntryID with
// the error SNS reported for it.
type PublishBatchFailure struct {
	Err     error
	EntryID string
}

// PublishBatch sends up to ten messages to an SNS topic in a single request and
// returns the per-entry outcome split into successful and failed entries.
//
// It returns errors.ErrEmptyInput when input is nil or carries no messages, and
// errors.ErrMaxBatchSize when Messages holds more than ten entries. For each
// entry, GroupID is sent as MessageGroupId (FIFO) and DeduplicationID as
// MessageDeduplicationId when set. Auto-generation only applies to FIFO topics,
// detected by a TopicARN ending in ".fifo" (the ARN is batch-level and applies
// to every entry): when the topic is FIFO and either value is empty and the
// corresponding generator has been configured (see WithGroupIDGenerator and
// WithDeduplicationIDGenerator), the value is auto-generated from that entry's
// Attributes and a generator error aborts the whole batch and is returned. For
// standard topics the generators are never invoked and MessageGroupId /
// MessageDeduplicationId are left unset unless the caller provided them
// explicitly. Attributes are sent as SNS message attributes with the String
// data type. Any SNS API failure is returned wrapped with context about the
// destination topic. On a successful call the returned output maps SNS's
// Successful entries (EntryID, MessageID) and Failed entries (EntryID, error
// built from the SNS code and message) back to the request IDs.
func (p *Producer) PublishBatch(ctx context.Context, input *PublishBatchInput) (*PublishBatchOutput, error) {
	if input == nil || len(input.Messages) == 0 {
		return nil, errors.ErrEmptyInput
	}

	if len(input.Messages) > maxBatchSize {
		return nil, errors.ErrMaxBatchSize
	}

	fifo := isFIFOTopic(input.TopicARN)

	entries := make([]types.PublishBatchRequestEntry, 0, len(input.Messages))
	for _, msg := range input.Messages {
		groupID, dedupID, err := p.resolveIDs(ctx, fifo, msg.GroupID, msg.DeduplicationID, msg.Attributes)
		if err != nil {
			return nil, err
		}

		entry := types.PublishBatchRequestEntry{
			Id:      aws.String(msg.ID),
			Message: aws.String(msg.Message),
		}

		if groupID != "" {
			entry.MessageGroupId = aws.String(groupID)
		}

		if dedupID != "" {
			entry.MessageDeduplicationId = aws.String(dedupID)
		}

		if len(msg.Attributes) > 0 {
			entry.MessageAttributes = messageAttributes(msg.Attributes)
		}

		entries = append(entries, entry)
	}

	out, err := p.client.PublishBatch(ctx, &sns.PublishBatchInput{
		TopicArn:                   aws.String(input.TopicARN),
		PublishBatchRequestEntries: entries,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to publish batch to topic %s: %w", input.TopicARN, err)
	}

	return mapBatchOutput(out), nil
}

// mapBatchOutput converts an SNS PublishBatch response into a PublishBatchOutput,
// tolerating a nil response by returning an empty output.
func mapBatchOutput(out *sns.PublishBatchOutput) *PublishBatchOutput {
	result := &PublishBatchOutput{}
	if out == nil {
		return result
	}

	result.Successful = make([]*PublishBatchSuccess, 0, len(out.Successful))
	for _, s := range out.Successful {
		result.Successful = append(result.Successful, &PublishBatchSuccess{
			EntryID:   aws.ToString(s.Id),
			MessageID: aws.ToString(s.MessageId),
		})
	}

	result.Failed = make([]*PublishBatchFailure, 0, len(out.Failed))
	for _, f := range out.Failed {
		result.Failed = append(result.Failed, &PublishBatchFailure{
			EntryID: aws.ToString(f.Id),
			Err:     batchEntryError(f),
		})
	}

	return result
}

// batchEntryError builds an error from an SNS batch failure entry, combining the
// error code and message reported by SNS.
func batchEntryError(f types.BatchResultErrorEntry) error {
	return fmt.Errorf("%s: %s", aws.ToString(f.Code), aws.ToString(f.Message))
}

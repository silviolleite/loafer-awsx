package consumer

import (
	"context"
	stderrors "errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/silviolleite/loafer-awsx/errors"
	"github.com/silviolleite/loafer-awsx/idgen"
)

type dlqFakeSQS struct {
	sendErr error
	inputs  []*sqs.SendMessageInput
}

func (f *dlqFakeSQS) ReceiveMessage(_ context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	return &sqs.ReceiveMessageOutput{}, nil
}

func (f *dlqFakeSQS) DeleteMessage(_ context.Context, _ *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	return &sqs.DeleteMessageOutput{}, nil
}

func (f *dlqFakeSQS) ChangeMessageVisibility(_ context.Context, _ *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *dlqFakeSQS) GetQueueUrl(_ context.Context, _ *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	return &sqs.GetQueueUrlOutput{}, nil
}

func (f *dlqFakeSQS) SendMessage(_ context.Context, params *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	f.inputs = append(f.inputs, params)
	if f.sendErr != nil {
		return nil, f.sendErr
	}
	return &sqs.SendMessageOutput{}, nil
}

func dlqMessage(body, groupID string, userAttrs map[string]string) *message {
	m := types.Message{Body: aws.String(body)}
	if groupID != "" {
		m.Attributes = map[string]string{messageGroupIDKey: groupID}
	}
	if len(userAttrs) > 0 {
		m.MessageAttributes = make(map[string]types.MessageAttributeValue, len(userAttrs))
		for k, v := range userAttrs {
			m.MessageAttributes[k] = types.MessageAttributeValue{
				DataType:    aws.String(stringDataType),
				StringValue: aws.String(v),
			}
		}
	}
	return newMessage(m)
}

func TestDLQPublisherPublishSendsBodyAttributesAndRetryCount(t *testing.T) {
	client := &dlqFakeSQS{}
	p := newDLQPublisher(client, "dlq-url", idgen.NewRandom())

	userAttrs := map[string]string{"tenant": "acme", "kind": "order"}
	msg := dlqMessage("payload", "", userAttrs)

	err := p.publish(context.Background(), msg, 5)

	require.NoError(t, err)
	require.Len(t, client.inputs, 1)

	got := client.inputs[0]
	assert.Equal(t, "dlq-url", aws.ToString(got.QueueUrl))
	assert.Equal(t, "payload", aws.ToString(got.MessageBody))

	require.Contains(t, got.MessageAttributes, retryCountAttribute)
	assert.Equal(t, numberDataType, aws.ToString(got.MessageAttributes[retryCountAttribute].DataType))
	assert.Equal(t, "5", aws.ToString(got.MessageAttributes[retryCountAttribute].StringValue))

	for k, v := range userAttrs {
		require.Contains(t, got.MessageAttributes, k)
		assert.Equal(t, v, aws.ToString(got.MessageAttributes[k].StringValue))
		assert.Equal(t, stringDataType, aws.ToString(got.MessageAttributes[k].DataType))
	}

	assert.Nil(t, got.MessageGroupId)
	assert.Nil(t, got.MessageDeduplicationId)
}

func TestDLQPublisherPublishFIFOSetsGroupAndFreshDedup(t *testing.T) {
	client := &dlqFakeSQS{}
	p := newDLQPublisher(client, "dlq-url", idgen.NewRandom())

	msg := dlqMessage("payload", "grp-1", map[string]string{"tenant": "acme"})

	require.NoError(t, p.publish(context.Background(), msg, 3))
	require.NoError(t, p.publish(context.Background(), dlqMessage("payload", "grp-1", nil), 3))

	require.Len(t, client.inputs, 2)

	first := client.inputs[0]
	assert.Equal(t, "grp-1", aws.ToString(first.MessageGroupId))
	assert.NotEmpty(t, aws.ToString(first.MessageDeduplicationId))

	second := client.inputs[1]
	assert.Equal(t, "grp-1", aws.ToString(second.MessageGroupId))
	assert.NotEmpty(t, aws.ToString(second.MessageDeduplicationId))

	assert.NotEqual(t, aws.ToString(first.MessageDeduplicationId), aws.ToString(second.MessageDeduplicationId))
}

func TestDLQPublisherPublishSendFailureWrapsErrDLQPublish(t *testing.T) {
	client := &dlqFakeSQS{sendErr: stderrors.New("send boom")}
	p := newDLQPublisher(client, "dlq-url", idgen.NewRandom())

	err := p.publish(context.Background(), dlqMessage("payload", "", nil), 4)

	require.Error(t, err)
	assert.True(t, stderrors.Is(err, errors.ErrDLQPublish))
}

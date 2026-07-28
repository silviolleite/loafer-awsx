package consumer

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/errors"
	"github.com/silviolleite/loafer-awsx/idgen"
)

type fakeSchedulerClient struct {
	createErr error
	calls     []*scheduler.CreateScheduleInput
	mu        sync.Mutex
}

func (f *fakeSchedulerClient) CreateSchedule(_ context.Context, params *scheduler.CreateScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.CreateScheduleOutput, error) {
	f.mu.Lock()
	f.calls = append(f.calls, params)
	err := f.createErr
	f.mu.Unlock()

	if err != nil {
		return nil, err
	}
	return &scheduler.CreateScheduleOutput{}, nil
}

func (f *fakeSchedulerClient) createCalls() []*scheduler.CreateScheduleInput {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*scheduler.CreateScheduleInput, len(f.calls))
	copy(out, f.calls)
	return out
}

func fifoMessage(body, groupID string, userAttrs map[string]string) *message {
	attrs := make(map[string]sqstypes.MessageAttributeValue, len(userAttrs))
	for k, v := range userAttrs {
		attrs[k] = sqstypes.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(v),
		}
	}
	return newMessage(sqstypes.Message{
		Body:              aws.String(body),
		ReceiptHandle:     aws.String("receipt-handle"),
		Attributes:        map[string]string{messageGroupIDKey: groupID},
		MessageAttributes: attrs,
	})
}

func decodeInput(t require.TestingT, in *scheduler.CreateScheduleInput) sqsSendMessageRequest {
	require.NotNil(t, in.Target)
	require.NotNil(t, in.Target.Input)
	var req sqsSendMessageRequest
	require.NoError(t, json.Unmarshal([]byte(*in.Target.Input), &req))
	return req
}

// Feature: fifo-scheduled-retry, Property 5: Re-published retry preserves body and FIFO identity
func TestRetrySchedulerPreservesBodyAndFIFOIdentity(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		body := rapid.String().Draw(t, "body")
		groupID := rapid.StringMatching(`[A-Za-z0-9_-]{1,64}`).Draw(t, "groupID")
		count := rapid.IntRange(0, 15).Draw(t, "attrCount")
		userAttrs := make(map[string]string, count)
		for i := 0; i < count; i++ {
			userAttrs["attr-"+strconv.Itoa(i)] = rapid.String().Draw(t, "value-"+strconv.Itoa(i))
		}
		next := rapid.IntRange(1, 2_147_483_647).Draw(t, "next")

		client := &fakeSchedulerClient{}
		rs := newRetryScheduler(client, "arn:target", "arn:role", "https://queue-url", idgen.NewRandom())

		msg := fifoMessage(body, groupID, userAttrs)

		require.NoError(t, rs.schedule(context.Background(), msg, next, time.Second))
		require.NoError(t, rs.schedule(context.Background(), msg, next, time.Second))

		calls := client.createCalls()
		require.Len(t, calls, 2)

		first := decodeInput(t, calls[0])
		second := decodeInput(t, calls[1])

		assert.Equal(t, body, first.MessageBody)
		assert.Equal(t, groupID, first.MessageGroupID)
		assert.LessOrEqual(t, len(first.MessageAttributes), maxMessageAttributes)

		rc, ok := first.MessageAttributes[retryCountAttribute]
		require.True(t, ok)
		assert.Equal(t, numberDataType, rc.DataType)
		assert.Equal(t, strconv.Itoa(next), rc.StringValue)

		for key := range userAttrs {
			if key == retryCountAttribute {
				continue
			}
			if attr, present := first.MessageAttributes[key]; present {
				assert.Equal(t, userAttrs[key], attr.StringValue)
			}
		}

		assert.NotEmpty(t, first.MessageDeduplicationID)
		assert.NotEqual(t, "original-dedup-id", first.MessageDeduplicationID)
		assert.NotEqual(t, first.MessageDeduplicationID, second.MessageDeduplicationID)

		require.NotNil(t, calls[0].Name)
		assert.Equal(t, first.MessageDeduplicationID, *calls[0].Name)
	})
}

func TestRetrySchedulerCreateScheduleInputShape(t *testing.T) {
	client := &fakeSchedulerClient{}
	rs := newRetryScheduler(
		client,
		"arn:aws:sqs:us-east-1:123456789012:entry.fifo",
		"arn:aws:iam::123456789012:role/scheduler-exec",
		"https://sqs.us-east-1.amazonaws.com/123456789012/entry.fifo",
		idgen.NewRandom(),
	)

	msg := fifoMessage("payload", "group-1", map[string]string{"foo": "bar"})

	require.NoError(t, rs.schedule(context.Background(), msg, 2, time.Second))

	calls := client.createCalls()
	require.Len(t, calls, 1)
	in := calls[0]

	assert.Equal(t, types.ActionAfterCompletionDelete, in.ActionAfterCompletion)
	require.NotNil(t, in.FlexibleTimeWindow)
	assert.Equal(t, types.FlexibleTimeWindowModeOff, in.FlexibleTimeWindow.Mode)
	require.NotNil(t, in.Target)
	require.NotNil(t, in.Target.Arn)
	assert.Equal(t, universalSQSSendMessageARN, *in.Target.Arn)
	require.NotNil(t, in.Target.RoleArn)
	assert.Equal(t, "arn:aws:iam::123456789012:role/scheduler-exec", *in.Target.RoleArn)
	require.NotNil(t, in.Name)
	assert.NotEmpty(t, *in.Name)
}

func TestRetrySchedulerCreateFailureWrapsError(t *testing.T) {
	client := &fakeSchedulerClient{createErr: stderrors.New("create schedule failed")}
	rs := newRetryScheduler(client, "arn:target", "arn:role", "https://queue-url", idgen.NewRandom())

	msg := fifoMessage("payload", "group-1", nil)

	err := rs.schedule(context.Background(), msg, 1, time.Second)

	require.Error(t, err)
	assert.True(t, stderrors.Is(err, errors.ErrRetryScheduleCreate))
}

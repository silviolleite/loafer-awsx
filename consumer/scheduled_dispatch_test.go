package consumer

import (
	"context"
	"log/slog"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/require"

	"github.com/silviolleite/loafer-awsx/middleware"
	"github.com/silviolleite/loafer-awsx/router"
)

type schedSQS struct {
	deleteErr    error
	sendErr      error
	deleteInputs []*sqs.DeleteMessageInput
	sendInputs   []*sqs.SendMessageInput
	visInputs    []*sqs.ChangeMessageVisibilityInput
	mu           sync.Mutex
}

func (f *schedSQS) ReceiveMessage(_ context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	return &sqs.ReceiveMessageOutput{}, nil
}

func (f *schedSQS) DeleteMessage(_ context.Context, params *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	f.mu.Lock()
	f.deleteInputs = append(f.deleteInputs, params)
	err := f.deleteErr
	f.mu.Unlock()

	if err != nil {
		return nil, err
	}
	return &sqs.DeleteMessageOutput{}, nil
}

func (f *schedSQS) ChangeMessageVisibility(_ context.Context, params *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	f.mu.Lock()
	f.visInputs = append(f.visInputs, params)
	f.mu.Unlock()

	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *schedSQS) GetQueueUrl(_ context.Context, _ *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	return &sqs.GetQueueUrlOutput{}, nil
}

func (f *schedSQS) SendMessage(_ context.Context, params *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	f.mu.Lock()
	f.sendInputs = append(f.sendInputs, params)
	err := f.sendErr
	f.mu.Unlock()

	if err != nil {
		return nil, err
	}
	return &sqs.SendMessageOutput{}, nil
}

func (f *schedSQS) deletes() []*sqs.DeleteMessageInput {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*sqs.DeleteMessageInput, len(f.deleteInputs))
	copy(out, f.deleteInputs)
	return out
}

func (f *schedSQS) sends() []*sqs.SendMessageInput {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*sqs.SendMessageInput, len(f.sendInputs))
	copy(out, f.sendInputs)
	return out
}

func (f *schedSQS) visChanges() []*sqs.ChangeMessageVisibilityInput {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*sqs.ChangeMessageVisibilityInput, len(f.visInputs))
	copy(out, f.visInputs)
	return out
}

var _ SQSClient = (*schedSQS)(nil)

func newScheduledDispatcher(tb require.TestingT, cfg router.ScheduledRetryConfig, sqsClient SQSClient, sched SchedulerClient, log *slog.Logger) *dispatcher {
	handler := func(_ context.Context, _ middleware.Message) error { return nil }

	route, err := router.New("test", handler,
		router.WithScheduledRetry(
			router.WithSchedulerIdentity(cfg.TargetQueueARN, cfg.ExecutionRoleARN),
			router.WithScheduledDLQ(cfg.DLQQueueURL),
			router.WithMaxRetryCount(cfg.MaxRetryCount),
			router.WithBackoff(cfg.BaseBackoff, cfg.MaxBackoff),
		),
	)
	require.NoError(tb, err)

	vm := newVisibilityManager(sqsClient, "queue-url", 30, 2, log)

	return newDispatcher(sqsClient, route, "queue-url", vm, sched, log)
}

func schedMessage(retryCount int, groupID string) *message {
	return newMessage(types.Message{
		Body:          aws.String("body"),
		ReceiptHandle: aws.String("receipt-handle"),
		Attributes:    map[string]string{messageGroupIDKey: groupID},
		MessageAttributes: map[string]types.MessageAttributeValue{
			retryCountAttribute: {
				DataType:    aws.String(numberDataType),
				StringValue: aws.String(strconv.Itoa(retryCount)),
			},
		},
	})
}

package fake

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/silviolleite/loafer-awsx/consumer"
)

// Compile-time assertion that SQSClient satisfies the consumer.SQSClient
// interface.
var _ consumer.SQSClient = (*SQSClient)(nil)

// SQSClient is a configurable test double for the consumer.SQSClient interface.
// Each method delegates to a corresponding function field, letting tests
// program the response (or error) for every call. When a function field is nil
// the method returns a nil output and a nil error. Every call is recorded so
// tests can assert on the parameters the code under test sent.
//
// SQSClient is safe for concurrent use; the recorded call slices are guarded by
// an internal mutex.
type SQSClient struct {
	ReceiveMessageFunc           func(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessageFunc            func(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibilityFunc  func(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
	GetQueueUrlFunc              func(ctx context.Context, params *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)
	SendMessageFunc              func(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	receiveMessageCalls          []*sqs.ReceiveMessageInput
	deleteMessageCalls           []*sqs.DeleteMessageInput
	changeMessageVisibilityCalls []*sqs.ChangeMessageVisibilityInput
	getQueueURLCalls             []*sqs.GetQueueUrlInput
	sendMessageCalls             []*sqs.SendMessageInput
	mu                           sync.Mutex
}

// ReceiveMessage records the call and delegates to ReceiveMessageFunc, or
// returns a nil output and nil error when the function is not set.
func (c *SQSClient) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	c.mu.Lock()
	c.receiveMessageCalls = append(c.receiveMessageCalls, params)
	c.mu.Unlock()

	if c.ReceiveMessageFunc != nil {
		return c.ReceiveMessageFunc(ctx, params, optFns...)
	}
	return nil, nil
}

// DeleteMessage records the call and delegates to DeleteMessageFunc, or returns
// a nil output and nil error when the function is not set.
func (c *SQSClient) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	c.mu.Lock()
	c.deleteMessageCalls = append(c.deleteMessageCalls, params)
	c.mu.Unlock()

	if c.DeleteMessageFunc != nil {
		return c.DeleteMessageFunc(ctx, params, optFns...)
	}
	return nil, nil
}

// ChangeMessageVisibility records the call and delegates to
// ChangeMessageVisibilityFunc, or returns a nil output and nil error when the
// function is not set.
func (c *SQSClient) ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	c.mu.Lock()
	c.changeMessageVisibilityCalls = append(c.changeMessageVisibilityCalls, params)
	c.mu.Unlock()

	if c.ChangeMessageVisibilityFunc != nil {
		return c.ChangeMessageVisibilityFunc(ctx, params, optFns...)
	}
	return nil, nil
}

// GetQueueUrl records the call and delegates to GetQueueUrlFunc, or returns a
// nil output and nil error when the function is not set.
func (c *SQSClient) GetQueueUrl(ctx context.Context, params *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	c.mu.Lock()
	c.getQueueURLCalls = append(c.getQueueURLCalls, params)
	c.mu.Unlock()

	if c.GetQueueUrlFunc != nil {
		return c.GetQueueUrlFunc(ctx, params, optFns...)
	}
	return nil, nil
}

// SendMessage records the call and delegates to SendMessageFunc, or returns a
// nil output and nil error when the function is not set.
func (c *SQSClient) SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	c.mu.Lock()
	c.sendMessageCalls = append(c.sendMessageCalls, params)
	c.mu.Unlock()

	if c.SendMessageFunc != nil {
		return c.SendMessageFunc(ctx, params, optFns...)
	}
	return nil, nil
}

// ReceiveMessageCalls returns a copy of the inputs passed to ReceiveMessage, in
// call order.
func (c *SQSClient) ReceiveMessageCalls() []*sqs.ReceiveMessageInput {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*sqs.ReceiveMessageInput, len(c.receiveMessageCalls))
	copy(out, c.receiveMessageCalls)
	return out
}

// DeleteMessageCalls returns a copy of the inputs passed to DeleteMessage, in
// call order.
func (c *SQSClient) DeleteMessageCalls() []*sqs.DeleteMessageInput {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*sqs.DeleteMessageInput, len(c.deleteMessageCalls))
	copy(out, c.deleteMessageCalls)
	return out
}

// ChangeMessageVisibilityCalls returns a copy of the inputs passed to
// ChangeMessageVisibility, in call order.
func (c *SQSClient) ChangeMessageVisibilityCalls() []*sqs.ChangeMessageVisibilityInput {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*sqs.ChangeMessageVisibilityInput, len(c.changeMessageVisibilityCalls))
	copy(out, c.changeMessageVisibilityCalls)
	return out
}

// GetQueueURLCalls returns a copy of the inputs passed to GetQueueUrl, in call
// order.
func (c *SQSClient) GetQueueURLCalls() []*sqs.GetQueueUrlInput {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*sqs.GetQueueUrlInput, len(c.getQueueURLCalls))
	copy(out, c.getQueueURLCalls)
	return out
}

// SendMessageCalls returns a copy of the inputs passed to SendMessage, in call
// order.
func (c *SQSClient) SendMessageCalls() []*sqs.SendMessageInput {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*sqs.SendMessageInput, len(c.sendMessageCalls))
	copy(out, c.sendMessageCalls)
	return out
}

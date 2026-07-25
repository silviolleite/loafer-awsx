package fake

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/sns"

	"github.com/silviolleite/loafer-awsx/producer"
)

// Compile-time assertion that SNSClient satisfies the producer.SNSClient
// interface.
var _ producer.SNSClient = (*SNSClient)(nil)

// SNSClient is a configurable test double for the producer.SNSClient interface.
// Each method delegates to a corresponding function field, letting tests
// program the response (or error) for every call. When a function field is nil
// the method returns a nil output and a nil error. Every call is recorded so
// tests can assert on the parameters the code under test sent.
//
// SNSClient is safe for concurrent use; the recorded call slices are guarded by
// an internal mutex.
type SNSClient struct {
	PublishFunc       func(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error)
	PublishBatchFunc  func(ctx context.Context, params *sns.PublishBatchInput, optFns ...func(*sns.Options)) (*sns.PublishBatchOutput, error)
	publishCalls      []*sns.PublishInput
	publishBatchCalls []*sns.PublishBatchInput
	mu                sync.Mutex
}

// Publish records the call and delegates to PublishFunc, or returns a nil
// output and nil error when the function is not set.
func (c *SNSClient) Publish(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error) {
	c.mu.Lock()
	c.publishCalls = append(c.publishCalls, params)
	c.mu.Unlock()

	if c.PublishFunc != nil {
		return c.PublishFunc(ctx, params, optFns...)
	}
	return nil, nil
}

// PublishBatch records the call and delegates to PublishBatchFunc, or returns a
// nil output and nil error when the function is not set.
func (c *SNSClient) PublishBatch(ctx context.Context, params *sns.PublishBatchInput, optFns ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
	c.mu.Lock()
	c.publishBatchCalls = append(c.publishBatchCalls, params)
	c.mu.Unlock()

	if c.PublishBatchFunc != nil {
		return c.PublishBatchFunc(ctx, params, optFns...)
	}
	return nil, nil
}

// PublishCalls returns a copy of the inputs passed to Publish, in call order.
func (c *SNSClient) PublishCalls() []*sns.PublishInput {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*sns.PublishInput, len(c.publishCalls))
	copy(out, c.publishCalls)
	return out
}

// PublishBatchCalls returns a copy of the inputs passed to PublishBatch, in
// call order.
func (c *SNSClient) PublishBatchCalls() []*sns.PublishBatchInput {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*sns.PublishBatchInput, len(c.publishBatchCalls))
	copy(out, c.publishBatchCalls)
	return out
}

// Package benchmarks contains throughput micro-benchmarks that compare the
// message-processing overhead of loafer-awsx with github.com/justcodes/loafer-go
// under identical conditions.
//
// The benchmarks deliberately avoid the network: both libraries are driven by
// the same in-memory SQS client (benchClient) that serves a fixed number of
// prebuilt messages and reports completion once every message has been deleted.
// This isolates the libraries' own dispatch, worker-pool, and bookkeeping cost
// from AWS/network latency, so the comparison reflects library overhead only.
package benchmarks

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// benchQueueURL is the fixed URL the fake client resolves for every queue.
const benchQueueURL = "https://sqs.local/000000000000/bench-queue"

// receiveBatch mirrors the SQS maximum of ten messages per ReceiveMessage call,
// which both libraries request by default.
const receiveBatch = 10

// benchClient is an in-memory SQS client shared by both libraries. Its method
// set matches both loafergo.SQSClient and consumer.SQSClient, so a single value
// drives either library. It hands out exactly total messages across
// ReceiveMessage calls and closes done once total messages have been deleted.
type benchClient struct {
	body   []byte
	groups int64 // when > 0, messages carry a cycling MessageGroupId (FIFO mode)
	total  int64

	remaining int64 // messages left to hand out
	seq       int64 // unique receipt-handle sequence
	delivered int64 // messages deleted (fully processed)

	done      chan struct{}
	closeOnce sync.Once
}

// newBenchClient builds a client seeded with total messages. When groups is
// greater than zero, each message is tagged with a MessageGroupId drawn from
// that many groups, exercising the FIFO PerGroupID routing path.
func newBenchClient(total, groups int64) *benchClient {
	return &benchClient{
		body:      []byte(`{"id":"u-1","text":"benchmark message body"}`),
		groups:    groups,
		total:     total,
		remaining: total,
		done:      make(chan struct{}),
	}
}

// GetQueueUrl resolves every queue to the fixed benchmark URL.
func (c *benchClient) GetQueueUrl(_ context.Context, _ *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	return &sqs.GetQueueUrlOutput{QueueUrl: aws.String(benchQueueURL)}, nil
}

// ReceiveMessage hands out up to receiveBatch messages per call until the seeded
// budget is exhausted, after which it briefly idles and returns an empty batch
// so the caller's polling loop does not busy-spin before the context is
// canceled.
func (c *benchClient) ReceiveMessage(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	var take int64
	for {
		rem := atomic.LoadInt64(&c.remaining)
		if rem == 0 {
			select {
			case <-ctx.Done():
				return &sqs.ReceiveMessageOutput{}, ctx.Err()
			case <-time.After(time.Millisecond):
				return &sqs.ReceiveMessageOutput{}, nil
			}
		}

		take = rem
		if take > receiveBatch {
			take = receiveBatch
		}
		if atomic.CompareAndSwapInt64(&c.remaining, rem, rem-take) {
			break
		}
	}

	msgs := make([]types.Message, take)
	for i := range msgs {
		id := atomic.AddInt64(&c.seq, 1)
		m := types.Message{
			ReceiptHandle: aws.String(strconv.FormatInt(id, 10)),
			Body:          aws.String(string(c.body)),
		}
		if c.groups > 0 {
			m.Attributes = map[string]string{
				"MessageGroupId": "g-" + strconv.FormatInt(id%c.groups, 10),
			}
		}
		msgs[i] = m
	}

	return &sqs.ReceiveMessageOutput{Messages: msgs}, nil
}

// DeleteMessage records a fully processed message and signals completion once
// every seeded message has been deleted.
func (c *benchClient) DeleteMessage(_ context.Context, _ *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	if atomic.AddInt64(&c.delivered, 1) == c.total {
		c.closeOnce.Do(func() { close(c.done) })
	}
	return &sqs.DeleteMessageOutput{}, nil
}

// ChangeMessageVisibility is a no-op; the benchmarks complete well within the
// visibility window so no extension is ever attempted.
func (c *benchClient) ChangeMessageVisibility(_ context.Context, _ *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

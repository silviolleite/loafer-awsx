// Package benchmarks contains throughput micro-benchmarks that compare the
// message-processing overhead of loafer-awsx with github.com/justcodes/loafer-go
// under identical conditions.
//
// The benchmarks deliberately avoid the network: both libraries are driven by
// the same in-memory SQS client (benchClient) that serves a fixed number of
// prebuilt messages and reports completion once every message has been deleted.
// This isolates the libraries' own dispatch, worker-pool, and bookkeeping cost
// from AWS/network latency, so the comparison reflects library overhead only.
//
// The delete benchmarks additionally set a per-call delete latency on the
// client to simulate the SQS round trip, and read its delete counters to report
// how many delete requests (DeleteMessage and DeleteMessageBatch) each message
// cost and how many entries each batch request carried.
//
// The flow-control benchmarks set a per-call receive latency, read the receive
// and visibility-change counters, and have the client record the receive
// instant of every message to measure how long messages wait between being
// received and starting.
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

// receiveBatch mirrors the SQS maximum of ten messages per ReceiveMessage call.
// It is used when a caller does not set MaxNumberOfMessages.
const receiveBatch = 10

// benchClient is an in-memory SQS client shared by both libraries. Its method
// set matches both loafergo.SQSClient and consumer.SQSClient, so a single value
// drives either library. It also implements DeleteMessageBatch and
// ChangeMessageVisibilityBatch, so it satisfies consumer.BatchDeleteClient and
// consumer.BatchVisibilityClient. It hands out exactly total messages across
// ReceiveMessage calls and closes done once total messages have been deleted
// through either delete API.
//
// deleteLatency, when non-zero, is slept by every DeleteMessage and
// DeleteMessageBatch call to simulate the SQS round trip. deleteCalls counts
// requests across both delete APIs, while batchCalls and batchEntries count
// DeleteMessageBatch requests and the entries they carried. receiveLatency,
// when non-zero, is slept by every ReceiveMessage call that hands out messages.
// receiveCalls counts ReceiveMessage requests and visibilityCalls counts
// ChangeMessageVisibility and ChangeMessageVisibilityBatch requests. All
// counters are accessed atomically. When recordReceivedAt is set, receivedAt
// maps each receipt handle to the instant its ReceiveMessage call returned. It
// is off by default because the map grows with every message and would add
// its cost to benchmarks that never read it.
type benchClient struct {
	done       chan struct{}
	receivedAt sync.Map
	body       []byte

	receiveLatency   time.Duration
	receiveCalls     int64
	visibilityCalls  int64
	recordReceivedAt bool

	groups        int64 // when > 0, messages carry a cycling MessageGroupId (FIFO mode)
	total         int64
	deleteLatency time.Duration

	remaining int64 // messages left to hand out
	seq       int64 // unique receipt-handle sequence
	delivered int64 // messages deleted (fully processed)

	deleteCalls  int64 // DeleteMessage plus DeleteMessageBatch requests
	batchCalls   int64 // DeleteMessageBatch requests
	batchEntries int64 // entries across all DeleteMessageBatch requests

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
func (c *benchClient) ReceiveMessage(
	ctx context.Context, params *sqs.ReceiveMessageInput, _ ...func(*sqs.Options),
) (*sqs.ReceiveMessageOutput, error) {
	atomic.AddInt64(&c.receiveCalls, 1)

	limit := int64(receiveBatch)
	if params != nil && params.MaxNumberOfMessages > 0 {
		limit = min(limit, int64(params.MaxNumberOfMessages))
	}

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

		take = min(rem, limit)
		if atomic.CompareAndSwapInt64(&c.remaining, rem, rem-take) {
			break
		}
	}

	if err := sleepCtx(ctx, c.receiveLatency); err != nil {
		atomic.AddInt64(&c.remaining, take)
		return &sqs.ReceiveMessageOutput{}, err
	}

	now := time.Now()
	msgs := make([]types.Message, take)
	for i := range msgs {
		id := atomic.AddInt64(&c.seq, 1)
		handle := strconv.FormatInt(id, 10)
		if c.recordReceivedAt {
			c.receivedAt.Store(handle, now)
		}
		m := types.Message{
			ReceiptHandle: aws.String(handle),
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

// DeleteMessage waits deleteLatency, records a fully processed message, and
// signals completion once every seeded message has been deleted. It returns the
// context error without deleting when ctx ends during the simulated latency.
func (c *benchClient) DeleteMessage(
	ctx context.Context, _ *sqs.DeleteMessageInput, _ ...func(*sqs.Options),
) (*sqs.DeleteMessageOutput, error) {
	if err := c.simulateDeleteLatency(ctx); err != nil {
		return nil, err
	}
	atomic.AddInt64(&c.deleteCalls, 1)
	c.markDelivered(1)
	return &sqs.DeleteMessageOutput{}, nil
}

// DeleteMessageBatch waits deleteLatency, records every entry as a fully
// processed message, and signals completion once every seeded message has been
// deleted. Every entry is reported as successful. It returns the context error
// without deleting when ctx ends during the simulated latency.
func (c *benchClient) DeleteMessageBatch(
	ctx context.Context, params *sqs.DeleteMessageBatchInput, _ ...func(*sqs.Options),
) (*sqs.DeleteMessageBatchOutput, error) {
	if err := c.simulateDeleteLatency(ctx); err != nil {
		return nil, err
	}

	n := int64(len(params.Entries))
	atomic.AddInt64(&c.deleteCalls, 1)
	atomic.AddInt64(&c.batchCalls, 1)
	atomic.AddInt64(&c.batchEntries, n)

	successful := make([]types.DeleteMessageBatchResultEntry, len(params.Entries))
	for i, e := range params.Entries {
		successful[i] = types.DeleteMessageBatchResultEntry{Id: e.Id}
	}

	c.markDelivered(n)
	return &sqs.DeleteMessageBatchOutput{Successful: successful}, nil
}

// simulateDeleteLatency blocks for deleteLatency, returning early with the
// context error when ctx ends first. It returns immediately when no latency is
// configured.
func (c *benchClient) simulateDeleteLatency(ctx context.Context) error {
	return sleepCtx(ctx, c.deleteLatency)
}

// sleepCtx blocks for d, returning early with the context error when ctx ends
// first. It returns immediately when d is not positive.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// markDelivered adds n deleted messages to delivered and closes done the first
// time delivered reaches total.
func (c *benchClient) markDelivered(n int64) {
	if atomic.AddInt64(&c.delivered, n) >= c.total {
		c.closeOnce.Do(func() { close(c.done) })
	}
}

// ChangeMessageVisibility counts the request and otherwise does nothing.
func (c *benchClient) ChangeMessageVisibility(
	_ context.Context, _ *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options),
) (*sqs.ChangeMessageVisibilityOutput, error) {
	atomic.AddInt64(&c.visibilityCalls, 1)
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

// ChangeMessageVisibilityBatch counts the request and reports every entry as
// successful.
func (c *benchClient) ChangeMessageVisibilityBatch(
	_ context.Context, params *sqs.ChangeMessageVisibilityBatchInput, _ ...func(*sqs.Options),
) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
	atomic.AddInt64(&c.visibilityCalls, 1)
	successful := make([]types.ChangeMessageVisibilityBatchResultEntry, len(params.Entries))
	for i, e := range params.Entries {
		successful[i] = types.ChangeMessageVisibilityBatchResultEntry{Id: e.Id}
	}
	return &sqs.ChangeMessageVisibilityBatchOutput{Successful: successful}, nil
}

// waitSince reports how long ago the message with the given receipt handle was
// received, and false when the handle is unknown.
func (c *benchClient) waitSince(handle string) (time.Duration, bool) {
	v, ok := c.receivedAt.Load(handle)
	if !ok {
		return 0, false
	}
	at, ok := v.(time.Time)
	if !ok {
		return 0, false
	}
	return time.Since(at), true
}

// SendMessage is a no-op that satisfies the SendMessage method added to
// consumer.SQSClient for the Scheduled Retry model. The benchmarks only
// exercise the Visibility model, so it is never invoked.
func (c *benchClient) SendMessage(_ context.Context, _ *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	return &sqs.SendMessageOutput{}, nil
}

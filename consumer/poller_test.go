package consumer_test

import (
	"context"
	stderrors "errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/consumer"
	verrors "github.com/silviolleite/loafer-awsx/errors"
	"github.com/silviolleite/loafer-awsx/fake"
	"github.com/silviolleite/loafer-awsx/middleware"
	"github.com/silviolleite/loafer-awsx/router"
)

const (
	pollerQueueURL     = "https://sqs.us-east-1.amazonaws.com/123456789012/orders-queue"
	pollerRetryTimeout = time.Second
	pollerMaxSteps     = 10_000
)

var errPollerReceive = stderrors.New("receive throttled")

func pollerMessages(prefix string, first, n int) []types.Message {
	msgs := make([]types.Message, n)
	for i := range n {
		id := fmt.Sprintf("%s-%d", prefix, first+i)
		msgs[i] = types.Message{MessageId: aws.String(id), ReceiptHandle: aws.String(id), Body: aws.String(id)}
	}
	return msgs
}

func pollerClient(receive func(ctx context.Context, in *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error)) *fake.SQSClient {
	return &fake.SQSClient{
		GetQueueUrlFunc: func(context.Context, *sqs.GetQueueUrlInput, ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
			return &sqs.GetQueueUrlOutput{QueueUrl: aws.String(pollerQueueURL)}, nil
		},
		ReceiveMessageFunc: func(ctx context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			return receive(ctx, in)
		},
	}
}

func pollerDeletedHandles(client *fake.SQSClient) []string {
	var out []string
	for _, call := range client.DeleteMessageCalls() {
		out = append(out, aws.ToString(call.ReceiptHandle))
	}
	return out
}

func pollerVisibilityHandles(client *fake.SQSClient) []string {
	var out []string
	for _, call := range client.ChangeMessageVisibilityBatchCalls() {
		for _, entry := range call.Entries {
			out = append(out, aws.ToString(entry.ReceiptHandle))
		}
	}
	for _, call := range client.ChangeMessageVisibilityCalls() {
		out = append(out, aws.ToString(call.ReceiptHandle))
	}
	return out
}

func pollerLogs(h *fake.LogHandler, level slog.Level, msg string) []fake.LogRecord {
	var out []fake.LogRecord
	for _, rec := range h.Records() {
		if rec.Level == level && rec.Message == msg {
			out = append(out, rec)
		}
	}
	return out
}

func startPollerRun(c *consumer.Consumer) (context.CancelFunc, <-chan error) {
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- c.Run(ctx) }()
	return cancel, errc
}

type pollerStep struct {
	size int
	fail bool
}

type admissionModel struct {
	blocked    map[string]chan struct{}
	handled    map[string]int
	deleted    map[string]int
	script     []pollerStep
	order      []string
	violations []string
	capacity   int
	batch      int
	next       int
	received   int
	finished   int
	inflight   int
	parked     int
	mu         sync.Mutex
}

func newAdmissionModel(capacity, batch int, script []pollerStep) *admissionModel {
	return &admissionModel{
		blocked:  make(map[string]chan struct{}),
		handled:  make(map[string]int),
		deleted:  make(map[string]int),
		script:   script,
		capacity: capacity,
		batch:    batch,
	}
}

func (m *admissionModel) violatef(format string, args ...any) {
	m.violations = append(m.violations, fmt.Sprintf(format, args...))
}

func (m *admissionModel) receive(ctx context.Context, in *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	m.mu.Lock()
	if int(in.MaxNumberOfMessages) != m.batch {
		m.violatef("MaxNumberOfMessages = %d, want %d", in.MaxNumberOfMessages, m.batch)
	}
	if admitted := m.received - m.finished; admitted+m.inflight+m.batch > m.capacity {
		m.violatef("receive issued with admitted %d + in-flight %d + batch %d > capacity %d", admitted, m.inflight, m.batch, m.capacity)
	}
	m.inflight += m.batch

	if m.next >= len(m.script) {
		m.parked++
		m.mu.Unlock()
		<-ctx.Done()
		m.mu.Lock()
		m.parked--
		m.inflight -= m.batch
		m.mu.Unlock()
		return nil, ctx.Err()
	}

	step := m.script[m.next]
	m.next++
	m.inflight -= m.batch
	if step.fail {
		m.mu.Unlock()
		return nil, errPollerReceive
	}

	k := step.size % (m.batch + 1)
	msgs := pollerMessages("p3", m.received, k)
	m.received += k
	m.mu.Unlock()
	return &sqs.ReceiveMessageOutput{Messages: msgs}, nil
}

func (m *admissionModel) handler(ctx context.Context, msg middleware.Message) error {
	m.mu.Lock()
	if admitted := m.received - m.finished; admitted > m.capacity {
		m.violatef("admitted %d > capacity %d", admitted, m.capacity)
	}
	id := msg.Identifier()
	m.handled[id]++
	ch := make(chan struct{})
	m.blocked[id] = ch
	m.order = append(m.order, id)
	m.mu.Unlock()

	select {
	case <-ch:
	case <-ctx.Done():
	}
	return nil
}

func (m *admissionModel) deleteMessage(_ context.Context, in *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	m.mu.Lock()
	m.deleted[aws.ToString(in.ReceiptHandle)]++
	m.finished++
	m.mu.Unlock()
	return &sqs.DeleteMessageOutput{}, nil
}

func (m *admissionModel) releaseNext(pick int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.order) == 0 {
		return false
	}
	i := pick % len(m.order)
	id := m.order[i]
	m.order = slices.Delete(m.order, i, i+1)
	close(m.blocked[id])
	delete(m.blocked, id)
	return true
}

func (m *admissionModel) settled(wantParked int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.next >= len(m.script) && len(m.order) == 0 && m.parked == wantParked
}

func (m *admissionModel) fail(msg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.violatef("%s", msg)
}

func drawPollerScript(t *rapid.T) []pollerStep {
	return rapid.SliceOfN(rapid.Custom(func(t *rapid.T) pollerStep {
		return pollerStep{
			fail: rapid.Bool().Draw(t, "fail"),
			size: rapid.IntRange(0, 10).Draw(t, "size"),
		}
	}), 0, 12).Draw(t, "script")
}

// Feature: consumer-flow-control, Property 3: For any route configuration (WorkerPoolSize, Prefetch, MaxMessages, Pollers), any sequence of receive responses (sizes 0 to MaxNumberOfMessages, errors), and any handler latencies, the number of Admitted_Messages never exceeds WorkerPoolSize + Prefetch, every ReceiveMessage call carries MaxNumberOfMessages == min(MaxMessages, Prefetch + 1), and no receive is issued while fewer Credits than that are available.
func TestRun_PropertyAdmissionBoundAndRequestSize(t *testing.T) {
	defer goleak.VerifyNone(t)

	rapid.Check(t, func(rt *rapid.T) {
		workers := rapid.IntRange(1, 16).Draw(rt, "workers")
		prefetch := rapid.IntRange(0, 20).Draw(rt, "prefetch")
		maxMessages := rapid.IntRange(1, 10).Draw(rt, "maxMessages")
		pollers := rapid.IntRange(1, 4).Draw(rt, "pollers")
		script := drawPollerScript(rt)
		picks := rapid.SliceOfN(rapid.IntRange(0, 1<<10), 1, 16).Draw(rt, "picks")

		capacity := workers + prefetch
		batch := min(maxMessages, prefetch+1)
		wantParked := min(pollers, capacity/batch)
		model := newAdmissionModel(capacity, batch, script)

		client := pollerClient(model.receive)
		client.DeleteMessageFunc = model.deleteMessage
		route := newRoute(rt, model.handler,
			router.WithWorkerPoolSize(workers),
			router.WithPrefetch(prefetch),
			router.WithMaxMessages(int32(maxMessages)),
			router.WithPollers(pollers),
		)
		c := newConsumer(rt, client, route, consumer.WithRetryTimeout(pollerRetryTimeout))

		var runErr error
		synctest.Test(t, func(*testing.T) {
			cancel, errc := startPollerRun(c)
			for step := 0; ; step++ {
				synctest.Wait()
				if model.settled(wantParked) {
					break
				}
				if step >= pollerMaxSteps {
					model.fail("consumer made no progress")
					break
				}
				if model.releaseNext(picks[step%len(picks)]) {
					continue
				}
				time.Sleep(pollerRetryTimeout)
			}
			cancel()
			runErr = <-errc
		})

		require.NoError(rt, runErr)
		require.Empty(rt, model.violations, strings.Join(model.violations, "\n"))
		assert.Len(rt, client.ReceiveMessageCalls(), len(script)+wantParked)
		assert.Len(rt, model.handled, model.received)
		for id, n := range model.handled {
			assert.Equal(rt, 1, n, id)
			assert.Equal(rt, 1, model.deleted[id], id)
		}
		assert.Len(rt, model.deleted, model.received)
	})
}

type stuckModel struct {
	handled      map[string]int
	deleted      map[string]int
	stuckID      string
	received     []string
	script       []int
	next         int
	parked       int
	stuckIndex   int
	mu           sync.Mutex
	stuckStarted bool
}

type stuckSnapshot struct {
	deleted      map[string]int
	stuckID      string
	received     []string
	next         int
	parked       int
	stuckStarted bool
}

func (m *stuckModel) receive(ctx context.Context, _ *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	m.mu.Lock()
	if m.next >= len(m.script) {
		m.parked++
		m.mu.Unlock()
		<-ctx.Done()
		return nil, ctx.Err()
	}

	size := m.script[m.next]
	m.next++
	msgs := pollerMessages("p5", len(m.received), size)
	for _, msg := range msgs {
		if len(m.received) == m.stuckIndex {
			m.stuckID = aws.ToString(msg.ReceiptHandle)
		}
		m.received = append(m.received, aws.ToString(msg.ReceiptHandle))
	}
	m.mu.Unlock()
	return &sqs.ReceiveMessageOutput{Messages: msgs}, nil
}

func (m *stuckModel) handler(ctx context.Context, msg middleware.Message) error {
	m.mu.Lock()
	id := msg.Identifier()
	m.handled[id]++
	stuck := id == m.stuckID
	if stuck {
		m.stuckStarted = true
	}
	m.mu.Unlock()

	if stuck {
		<-ctx.Done()
	}
	return nil
}

func (m *stuckModel) deleteMessage(_ context.Context, in *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	m.mu.Lock()
	m.deleted[aws.ToString(in.ReceiptHandle)]++
	m.mu.Unlock()
	return &sqs.DeleteMessageOutput{}, nil
}

func (m *stuckModel) snapshot() stuckSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	deleted := make(map[string]int, len(m.deleted))
	for k, v := range m.deleted {
		deleted[k] = v
	}
	return stuckSnapshot{
		deleted:      deleted,
		stuckID:      m.stuckID,
		received:     slices.Clone(m.received),
		next:         m.next,
		parked:       m.parked,
		stuckStarted: m.stuckStarted,
	}
}

// Feature: consumer-flow-control, Property 5: For any WorkerPoolSize >= 2 and any script in which one handler blocks until the test ends, every other admitted message reaches its Outcome, and Pollers keep receiving while Credits are available.
func TestRun_PropertyParallelProgressDespiteStuckWorker(t *testing.T) {
	defer goleak.VerifyNone(t)

	rapid.Check(t, func(rt *rapid.T) {
		workers := rapid.IntRange(2, 32).Draw(rt, "workers")
		prefetch := rapid.IntRange(0, 20).Draw(rt, "prefetch")
		maxMessages := rapid.IntRange(1, 10).Draw(rt, "maxMessages")
		pollers := rapid.IntRange(1, 4).Draw(rt, "pollers")

		capacity := workers + prefetch
		batch := min(maxMessages, prefetch+1)
		script := rapid.SliceOfN(rapid.IntRange(0, batch), 1, 8).Draw(rt, "script")
		anchor := rapid.IntRange(0, len(script)-1).Draw(rt, "anchor")
		script[anchor] = max(script[anchor], 1)
		total := 0
		for _, n := range script {
			total += n
		}
		stuckIndex := rapid.IntRange(0, total-1).Draw(rt, "stuckIndex")
		freeBatches := (capacity - 1) / batch
		wantParked := min(pollers, freeBatches)

		model := &stuckModel{
			handled:    make(map[string]int),
			deleted:    make(map[string]int),
			script:     script,
			stuckIndex: stuckIndex,
		}
		client := pollerClient(model.receive)
		client.DeleteMessageFunc = model.deleteMessage
		route := newRoute(rt, model.handler,
			router.WithRunMode(router.Parallel),
			router.WithWorkerPoolSize(workers),
			router.WithPrefetch(prefetch),
			router.WithMaxMessages(int32(maxMessages)),
			router.WithPollers(pollers),
		)
		c := newConsumer(rt, client, route)

		var (
			before stuckSnapshot
			runErr error
		)
		synctest.Test(t, func(*testing.T) {
			cancel, errc := startPollerRun(c)
			synctest.Wait()
			before = model.snapshot()
			cancel()
			runErr = <-errc
		})

		require.NoError(rt, runErr)
		require.True(rt, before.stuckStarted)
		assert.NotContains(rt, before.deleted, before.stuckID)
		for _, id := range before.received {
			if id == before.stuckID {
				continue
			}
			assert.Equal(rt, 1, before.deleted[id], id)
		}
		assert.Len(rt, before.deleted, len(before.received)-1)
		assert.Equal(rt, wantParked, before.parked)
		if freeBatches >= 1 {
			assert.Equal(rt, len(script), before.next)
			assert.Len(rt, before.received, total)
		}
		for id, n := range model.handled {
			assert.Equal(rt, 1, n, id)
		}
	})
}

func TestRun_ReceiveErrorIsolatedToOnePoller(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		name     string
		opts     []consumer.Option
		wantWait time.Duration
	}{
		{name: "custom retry timeout", opts: []consumer.Option{consumer.WithRetryTimeout(time.Second)}, wantWait: time.Second},
		{name: "sub-second retry timeout", opts: []consumer.Option{consumer.WithRetryTimeout(250 * time.Millisecond)}, wantWait: 250 * time.Millisecond},
		{name: "default retry timeout", wantWait: 5 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var (
					mu    sync.Mutex
					times []time.Time
				)
				client := pollerClient(func(ctx context.Context, _ *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
					mu.Lock()
					call := len(times)
					times = append(times, time.Now())
					mu.Unlock()

					switch call {
					case 0:
						return nil, errPollerReceive
					case 1:
						return &sqs.ReceiveMessageOutput{Messages: pollerMessages("iso", 1, 1)}, nil
					case 3:
						return &sqs.ReceiveMessageOutput{Messages: pollerMessages("iso", 2, 1)}, nil
					default:
						<-ctx.Done()
						return nil, ctx.Err()
					}
				})
				logs := fake.NewLogHandler()
				route := newRoute(t, nilHandler,
					router.WithWorkerPoolSize(2),
					router.WithPrefetch(0),
					router.WithMaxMessages(1),
					router.WithPollers(2),
				)
				opts := append([]consumer.Option{consumer.WithLogger(slog.New(logs))}, tt.opts...)
				c := newConsumer(t, client, route, opts...)
				start := time.Now()

				cancel, errc := startPollerRun(c)
				synctest.Wait()

				require.Len(t, client.ReceiveMessageCalls(), 3)
				assert.Equal(t, []string{"iso-1"}, pollerDeletedHandles(client))
				errs := pollerLogs(logs, slog.LevelError, "failed to receive messages")
				require.Len(t, errs, 1)
				logged, ok := errs[0].Attrs["error"].(error)
				require.True(t, ok)
				assert.ErrorIs(t, logged, verrors.ErrGetMessage)
				assert.ErrorIs(t, logged, errPollerReceive)
				assert.Equal(t, pollerQueueURL, errs[0].Attrs["queue_url"])

				time.Sleep(tt.wantWait - time.Nanosecond)
				synctest.Wait()
				assert.Len(t, client.ReceiveMessageCalls(), 3)

				time.Sleep(time.Nanosecond)
				synctest.Wait()
				assert.Len(t, client.ReceiveMessageCalls(), 5)
				assert.ElementsMatch(t, []string{"iso-1", "iso-2"}, pollerDeletedHandles(client))

				cancel()
				require.NoError(t, <-errc)

				mu.Lock()
				defer mu.Unlock()
				require.Len(t, times, 5)
				for _, at := range times[:3] {
					assert.Equal(t, start, at)
				}
				assert.Equal(t, start.Add(tt.wantWait), times[3])
				assert.Len(t, pollerLogs(logs, slog.LevelError, "failed to receive messages"), 1)
			})
		})
	}
}

func TestRun_OversizedReceiveAdmitsOnlyRequestedBatch(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		name              string
		workers           int
		prefetch          int
		maxMessages       int32
		extra             int
		waitTimeSeconds   int32
		visibilityTimeout int32
		wantBatch         int
	}{
		{name: "max messages below prefetch", workers: 4, prefetch: 6, maxMessages: 2, extra: 3, waitTimeSeconds: 20, visibilityTimeout: 45, wantBatch: 2},
		{name: "zero prefetch receives one", workers: 4, maxMessages: 10, extra: 2, waitTimeSeconds: 0, visibilityTimeout: 30, wantBatch: 1},
		{name: "prefetch bounds the batch", workers: 2, prefetch: 3, maxMessages: 10, extra: 1, waitTimeSeconds: 5, visibilityTimeout: 300, wantBatch: 4},
		{name: "full batch of ten plus one", workers: 5, prefetch: 10, maxMessages: 10, extra: 1, waitTimeSeconds: 10, visibilityTimeout: 60, wantBatch: 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				msgs := pollerMessages("over", 0, tt.wantBatch+tt.extra)
				var (
					mu      sync.Mutex
					served  bool
					handled []string
				)
				client := pollerClient(func(ctx context.Context, _ *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
					mu.Lock()
					first := !served
					served = true
					mu.Unlock()
					if first {
						return &sqs.ReceiveMessageOutput{Messages: msgs}, nil
					}
					<-ctx.Done()
					return nil, ctx.Err()
				})
				handler := func(_ context.Context, m middleware.Message) error {
					mu.Lock()
					handled = append(handled, m.Identifier())
					mu.Unlock()
					return nil
				}
				logs := fake.NewLogHandler()
				route := newRoute(t, handler,
					router.WithWorkerPoolSize(tt.workers),
					router.WithPrefetch(tt.prefetch),
					router.WithMaxMessages(tt.maxMessages),
					router.WithWaitTimeSeconds(tt.waitTimeSeconds),
					router.WithVisibilityTimeout(tt.visibilityTimeout),
				)
				c := newConsumer(t, client, route, consumer.WithLogger(slog.New(logs)))

				cancel, errc := startPollerRun(c)
				synctest.Wait()
				cancel()
				require.NoError(t, <-errc)

				var admitted, extras []string
				for i, m := range msgs {
					if i < tt.wantBatch {
						admitted = append(admitted, aws.ToString(m.ReceiptHandle))
						continue
					}
					extras = append(extras, aws.ToString(m.ReceiptHandle))
				}

				mu.Lock()
				assert.ElementsMatch(t, admitted, handled)
				mu.Unlock()
				assert.ElementsMatch(t, admitted, pollerDeletedHandles(client))

				warns := pollerLogs(logs, slog.LevelWarn, "received more messages than requested; leaving message for redelivery")
				var warned []string
				for _, w := range warns {
					warned = append(warned, fmt.Sprint(w.Attrs["receipt_handle"]))
					assert.Equal(t, pollerQueueURL, w.Attrs["queue_url"])
				}
				assert.Equal(t, extras, warned)

				visibility := pollerVisibilityHandles(client)
				for _, id := range extras {
					assert.NotContains(t, visibility, id)
				}

				calls := client.ReceiveMessageCalls()
				require.Len(t, calls, 2)
				for _, in := range calls {
					assert.Equal(t, pollerQueueURL, aws.ToString(in.QueueUrl))
					assert.Equal(t, int32(tt.wantBatch), in.MaxNumberOfMessages)
					assert.Equal(t, tt.waitTimeSeconds, in.WaitTimeSeconds)
					assert.Equal(t, tt.visibilityTimeout, in.VisibilityTimeout)
					assert.Equal(t, []string{"All"}, in.MessageAttributeNames)
					assert.Equal(t, []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll}, in.MessageSystemAttributeNames)
				}
			})
		})
	}
}

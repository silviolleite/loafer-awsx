package consumer

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/logger"
	"github.com/silviolleite/loafer-awsx/middleware"
	"github.com/silviolleite/loafer-awsx/router"
)

func (h *captureHandler) warnAttrs() []map[string]string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []map[string]string
	for _, r := range h.records {
		if r.Level != slog.LevelWarn {
			continue
		}
		attrs := make(map[string]string)
		r.Attrs(func(a slog.Attr) bool {
			attrs[a.Key] = a.Value.String()
			return true
		})
		out = append(out, attrs)
	}
	return out
}

func TestDispatcherWorkSourceByRunMode(t *testing.T) {
	tests := []struct {
		name        string
		mode        router.Mode
		isGroupMode bool
	}{
		{name: "per group id uses group scheduler", mode: router.PerGroupID, isGroupMode: true},
		{name: "parallel uses shared queue", mode: router.Parallel, isGroupMode: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDispatcher(t, &fakeSQSClient{}, logger.NewNoOp(), nilHandler,
				router.WithRunMode(tt.mode),
			)

			_, isGroup := d.src.(*groupScheduler)
			_, isShared := d.src.(*sharedQueue)
			assert.Equal(t, tt.isGroupMode, isGroup)
			assert.Equal(t, !tt.isGroupMode, isShared)
		})
	}
}

func TestDispatcherHoldLogsAndFinishes(t *testing.T) {
	capture := &captureHandler{}
	client := &fakeSQSClient{}
	d := newTestDispatcher(t, client, slog.New(capture), nilHandler,
		router.WithRunMode(router.PerGroupID),
	)

	msg := buildGroupMessage(t, "receipt-held", "group-1", nil)
	require.NoError(t, d.credits.Acquire(context.Background(), 1))
	d.visibility.track(msg)

	d.hold([]*message{msg})

	assert.Empty(t, client.deletes())
	assert.Empty(t, client.calls())
	assert.Equal(t, d.credits.Capacity(), d.credits.Available())
	assert.Zero(t, len(d.visibility.entries))

	warns := capture.warnAttrs()
	require.Len(t, warns, 1)
	assert.Equal(t, "receipt-held", warns[0]["receipt_handle"])
	assert.Equal(t, "group-1", warns[0]["group_id"])
}

func newQueueDispatcher(
	tb require.TestingT, client SQSClient, log *slog.Logger, queueURL string, handler middleware.Handler, opts ...router.Option,
) *dispatcher {
	route, err := router.New("queue", handler, opts...)
	require.NoError(tb, err)

	vm := newVisibilityManager(client, queueURL, route.VisibilityTimeout(), route.ExtensionLimit(), logger.NewNoOp())

	return newDispatcher(client, route, queueURL, vm, nil, log)
}

func TestDispatcherHoldReleasesOnlyOnFIFOQueue(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		name := rapid.StringMatching(`[A-Za-z0-9_-]{1,75}`).Draw(rt, "queueName")
		isFIFO := rapid.Bool().Draw(rt, "fifo")
		queueURL := "https://sqs.us-east-1.amazonaws.com/123456789012/" + name
		if isFIFO {
			queueURL += ".fifo"
		}
		n := rapid.IntRange(1, 25).Draw(rt, "held")

		capture := &captureHandler{}
		client := &fakeSQSClient{}
		d := newQueueDispatcher(rt, client, slog.New(capture), queueURL, nilHandler,
			router.WithRunMode(router.PerGroupID),
			router.WithWorkerPoolSize(n),
		)
		msgs := make([]*message, n)
		handles := make([]string, n)
		for i := range msgs {
			handles[i] = "held-" + strconv.Itoa(i)
			msgs[i] = buildGroupMessage(rt, handles[i], "group-"+strconv.Itoa(i%3), nil)
		}
		d.visibility.start(context.Background())
		require.NoError(rt, d.credits.Acquire(context.Background(), n))
		for _, m := range msgs {
			d.visibility.track(m)
		}

		d.hold(msgs)
		d.visibility.close()

		if isFIFO {
			assert.Equal(rt, make([]int32, n), client.calls())
			assert.ElementsMatch(rt, handles, client.handles())
		} else {
			assert.Empty(rt, client.calls())
		}
		assert.Empty(rt, client.deletes())
		assert.Zero(rt, trackedCount(d))
		assert.Equal(rt, d.credits.Capacity(), d.credits.Available())

		warns := capture.warns()
		require.Len(rt, warns, n)
		for i, w := range warns {
			assert.Equal(rt, loggedWarn{
				message:  holdWarn,
				queueURL: queueURL,
				receipt:  handles[i],
				groupID:  "group-" + strconv.Itoa(i%3),
			}, w)
		}
	})
}

func TestDispatcherPerGroupIDHeldTailByQueueType(t *testing.T) {
	tests := []struct {
		name      string
		queueURL  string
		wantCalls []int32
	}{
		{name: "standard queue keeps held messages invisible", queueURL: "queue-url"},
		{name: "fifo queue releases held messages", queueURL: "queue-url.fifo", wantCalls: []int32{0, 0, 0}},
		{name: "fifo marker before the end is a standard queue", queueURL: "queue.fifo-url"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				rec := &handlerRecorder{}
				handler := func(ctx context.Context, m middleware.Message) error {
					_ = rec.handle(ctx, m)
					if m.(*message).Identifier() == "msg-1" {
						return errors.New("boom")
					}
					return nil
				}
				capture := &captureHandler{}
				client := &fakeSQSClient{}
				d := newQueueDispatcher(t, client, slog.New(capture), tt.queueURL, handler,
					router.WithRunMode(router.PerGroupID),
					router.WithWorkerPoolSize(2),
				)

				runToCompletion(t, d, []*message{
					buildGroupMessage(t, "msg-1", "g", nil),
					buildGroupMessage(t, "msg-2", "g", nil),
					buildGroupMessage(t, "msg-3", "g", nil),
					buildGroupMessage(t, "msg-4", "g", nil),
				})

				assert.Equal(t, []string{"msg-1"}, rec.snapshot())
				assert.Empty(t, client.deletes())
				assert.Equal(t, tt.wantCalls, nilIfEmptyInt32(client.calls()))
				if tt.wantCalls != nil {
					assert.ElementsMatch(t, []string{"msg-2", "msg-3", "msg-4"}, client.handles())
				}
				assert.Equal(t, []loggedWarn{
					{message: holdWarn, queueURL: tt.queueURL, receipt: "msg-2", groupID: "g"},
					{message: holdWarn, queueURL: tt.queueURL, receipt: "msg-3", groupID: "g"},
					{message: holdWarn, queueURL: tt.queueURL, receipt: "msg-4", groupID: "g"},
				}, capture.warns())
				assert.Zero(t, trackedCount(d))
				assert.Equal(t, d.credits.Capacity(), d.credits.Available())
			})
		})
	}
}

func TestDispatcherProcessReportsGroupFailure(t *testing.T) {
	backoffHandler := func(_ context.Context, m middleware.Message) error {
		m.(*message).Backoff(20 * time.Second)
		return nil
	}

	tests := []struct {
		name        string
		handler     middleware.Handler
		wantDeletes []string
		wantCalls   []int32
		wantFailed  bool
	}{
		{name: "error fails the group", handler: errorHandler, wantFailed: true},
		{name: "backoff fails the group", handler: backoffHandler, wantFailed: true, wantCalls: []int32{20}},
		{name: "success keeps the group", handler: nilHandler, wantDeletes: []string{"receipt"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &fakeSQSClient{}
			d := newTestDispatcher(t, client, logger.NewNoOp(), tt.handler,
				router.WithRunMode(router.PerGroupID),
			)

			d.visibility.start(context.Background())
			failed := d.process(context.Background(), buildGroupMessage(t, "receipt", "group-1", nil))
			d.visibility.close()

			assert.Equal(t, tt.wantFailed, failed)
			assert.Equal(t, tt.wantDeletes, nilIfEmpty(client.deletes()))
			assert.Equal(t, tt.wantCalls, nilIfEmptyInt32(client.calls()))
		})
	}
}

func TestDispatcherParallelNeverHolds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		handler := func(_ context.Context, m middleware.Message) error {
			if m.(*message).Identifier() == "a-1" {
				return errors.New("boom")
			}
			return nil
		}

		client := &fakeSQSClient{}
		d := newTestDispatcher(t, client, logger.NewNoOp(), handler,
			router.WithRunMode(router.Parallel),
			router.WithWorkerPoolSize(1),
		)

		runToCompletion(t, d, []*message{
			buildGroupMessage(t, "a-1", "g", nil),
			buildGroupMessage(t, "a-2", "g", nil),
		})

		assert.Equal(t, []string{"a-2"}, client.deletes())
	})
}

func TestDispatcherPerGroupIDHoldsTailAfterHeadFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		var called []string
		handler := func(_ context.Context, m middleware.Message) error {
			id := m.(*message).Identifier()
			mu.Lock()
			called = append(called, id)
			mu.Unlock()
			if id == "msg-1" {
				return errors.New("boom")
			}
			return nil
		}

		capture := &captureHandler{}
		client := &fakeSQSClient{}
		d := newTestDispatcher(t, client, slog.New(capture), handler,
			router.WithRunMode(router.PerGroupID),
			router.WithWorkerPoolSize(2),
		)

		msgs := make([]*message, 0, 4)
		for _, id := range []string{"msg-1", "msg-2", "msg-3", "msg-4"} {
			msgs = append(msgs, buildGroupMessage(t, id, "g", nil))
		}
		runToCompletion(t, d, msgs)

		mu.Lock()
		got := append([]string(nil), called...)
		mu.Unlock()

		assert.Equal(t, []string{"msg-1"}, got)
		assert.Empty(t, client.deletes())
		assert.Empty(t, client.calls())
		assert.Len(t, capture.warnAttrs(), 3)
		assert.Equal(t, d.credits.Capacity(), d.credits.Available())
	})
}

func TestDispatcherPerGroupIDIndependentGroups(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		handler := func(_ context.Context, m middleware.Message) error {
			if m.(*message).Identifier() == "a-1" {
				return errors.New("boom")
			}
			return nil
		}

		client := &fakeSQSClient{}
		d := newTestDispatcher(t, client, logger.NewNoOp(), handler,
			router.WithRunMode(router.PerGroupID),
			router.WithWorkerPoolSize(2),
		)

		runToCompletion(t, d, []*message{
			buildGroupMessage(t, "a-1", "A", nil),
			buildGroupMessage(t, "b-1", "B", nil),
			buildGroupMessage(t, "a-2", "A", nil),
			buildGroupMessage(t, "b-2", "B", nil),
		})

		assert.Equal(t, []string{"b-1", "b-2"}, client.deletes())
	})
}

func TestDispatcherPerGroupIDPreservesOrderAcrossWorkers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		called := map[string][]string{}
		handler := func(_ context.Context, m middleware.Message) error {
			msg := m.(*message)
			group := msg.SystemAttributeByKey(messageGroupIDKey)
			mu.Lock()
			called[group] = append(called[group], msg.Identifier())
			mu.Unlock()
			return nil
		}

		client := &fakeSQSClient{}
		d := newTestDispatcher(t, client, logger.NewNoOp(), handler,
			router.WithRunMode(router.PerGroupID),
			router.WithWorkerPoolSize(4),
		)

		var msgs []*message
		for i := range 3 {
			for _, g := range []string{"A", "B"} {
				msgs = append(msgs, buildGroupMessage(t, g+"-"+string(rune('1'+i)), g, nil))
			}
		}
		runToCompletion(t, d, msgs)

		assert.Equal(t, []string{"A-1", "A-2", "A-3"}, called["A"])
		assert.Equal(t, []string{"B-1", "B-2", "B-3"}, called["B"])
		assert.Len(t, client.deletes(), 6)
	})
}

func TestDispatcherReleasesMessagesTakenAfterCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var ran atomic.Bool
		handler := func(_ context.Context, _ middleware.Message) error {
			ran.Store(true)
			return nil
		}

		client := &fakeSQSClient{}
		d := newTestDispatcher(t, client, logger.NewNoOp(), handler,
			router.WithRunMode(router.PerGroupID),
			router.WithWorkerPoolSize(1),
		)

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		d.visibility.start(context.Background())
		require.NoError(t, d.credits.Acquire(context.Background(), 2))
		d.admit(buildGroupMessage(t, "r-1", "g", nil))
		d.admit(buildGroupMessage(t, "r-2", "h", nil))

		d.worker(ctx)
		d.releasePending()
		d.visibility.close()

		assert.False(t, ran.Load())
		assert.Empty(t, client.deletes())
		assert.Equal(t, []int32{0, 0}, client.calls())
		assert.Equal(t, d.credits.Capacity(), d.credits.Available())
	})
}

func nilIfEmpty(s []string) []string {
	if len(s) == 0 {
		return nil
	}
	return s
}

func nilIfEmptyInt32(s []int32) []int32 {
	if len(s) == 0 {
		return nil
	}
	return s
}

type loggedWarn struct {
	message  string
	queueURL string
	receipt  string
	groupID  string
}

func (h *captureHandler) warns() []loggedWarn {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []loggedWarn
	for _, r := range h.records {
		if r.Level != slog.LevelWarn {
			continue
		}
		w := loggedWarn{message: r.Message}
		r.Attrs(func(a slog.Attr) bool {
			switch a.Key {
			case "queue_url":
				w.queueURL = a.Value.String()
			case "receipt_handle":
				w.receipt = a.Value.String()
			case "group_id":
				w.groupID = a.Value.String()
			}
			return true
		})
		out = append(out, w)
	}
	return out
}

type hookedSQSClient struct {
	*fakeSQSClient
	onDelete func(ctx context.Context, handle string)
}

func (c *hookedSQSClient) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	c.onDelete(ctx, aws.ToString(params.ReceiptHandle))
	return c.fakeSQSClient.DeleteMessage(ctx, params, optFns...)
}

type handlerRecorder struct {
	called []string
	mu     sync.Mutex
}

func (r *handlerRecorder) handle(_ context.Context, m middleware.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.called = append(r.called, m.(*message).Identifier())
	return nil
}

func (r *handlerRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.called...)
}

func isTracked(d *dispatcher, msg *message) bool {
	d.visibility.mu.Lock()
	defer d.visibility.mu.Unlock()
	_, ok := d.visibility.entries[msg]
	return ok
}

func trackedCount(d *dispatcher) int {
	d.visibility.mu.Lock()
	defer d.visibility.mu.Unlock()
	return len(d.visibility.entries)
}

const skipExpiredWarn = "skipping message whose visibility timeout expired before it started"

const holdWarn = "holding message to preserve FIFO group order; earlier message in group failed"

func TestDispatcherSkipsMessageExpiredBeforeStart(t *testing.T) {
	tests := []struct {
		name        string
		opts        []router.Option
		wantCalled  []string
		wantDeletes []string
		wantWarns   []loggedWarn
	}{
		{
			name:        "per group id under the visibility model holds the rest of the group",
			opts:        []router.Option{router.WithRunMode(router.PerGroupID)},
			wantCalled:  []string{"h-1"},
			wantDeletes: []string{"h-1"},
			wantWarns: []loggedWarn{
				{message: skipExpiredWarn, queueURL: "queue-url", receipt: "g-1", groupID: "g"},
				{message: holdWarn, queueURL: "queue-url", receipt: "g-2", groupID: "g"},
				{message: holdWarn, queueURL: "queue-url", receipt: "g-3", groupID: "g"},
			},
		},
		{
			name: "per group id under the scheduled retry model keeps the group running",
			opts: []router.Option{
				router.WithRunMode(router.PerGroupID),
				router.WithRetryModel(router.ScheduledRetryModel),
			},
			wantCalled:  []string{"g-2", "g-3", "h-1"},
			wantDeletes: []string{"g-2", "g-3", "h-1"},
			wantWarns: []loggedWarn{
				{message: skipExpiredWarn, queueURL: "queue-url", receipt: "g-1", groupID: "g"},
			},
		},
		{
			name:        "parallel skips only the expired message",
			opts:        []router.Option{router.WithRunMode(router.Parallel)},
			wantCalled:  []string{"g-2", "g-3", "h-1"},
			wantDeletes: []string{"g-2", "g-3", "h-1"},
			wantWarns: []loggedWarn{
				{message: skipExpiredWarn, queueURL: "queue-url", receipt: "g-1", groupID: "g"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				rec := &handlerRecorder{}
				capture := &captureHandler{}
				client := &fakeSQSClient{}
				d := newTestDispatcher(t, client, slog.New(capture), rec.handle,
					append([]router.Option{router.WithWorkerPoolSize(2)}, tt.opts...)...,
				)
				expired := buildGroupMessage(t, "g-1", "g", nil)
				expired.receivedAt = time.Now().Add(-13 * time.Hour)

				runToCompletion(t, d, []*message{
					expired,
					buildGroupMessage(t, "g-2", "g", nil),
					buildGroupMessage(t, "h-1", "h", nil),
					buildGroupMessage(t, "g-3", "g", nil),
				})

				assert.ElementsMatch(t, tt.wantCalled, rec.snapshot())
				assert.ElementsMatch(t, tt.wantDeletes, client.deletes())
				assert.Empty(t, client.calls())
				assert.Equal(t, tt.wantWarns, capture.warns())
				assert.Zero(t, trackedCount(d))
				assert.Equal(t, d.credits.Capacity(), d.credits.Available())
			})
		})
	}
}

func TestDispatcherWorkerSkipsOnlyPastDeadline(t *testing.T) {
	tests := []struct {
		name    string
		age     time.Duration
		wantRun bool
	}{
		{name: "received just now runs", age: 0, wantRun: true},
		{name: "one nanosecond before the deadline runs", age: 30*time.Second - time.Nanosecond, wantRun: true},
		{name: "at the deadline is skipped", age: 30 * time.Second},
		{name: "past the deadline is skipped", age: 31 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				rec := &handlerRecorder{}
				capture := &captureHandler{}
				client := &fakeSQSClient{}
				d := newTestDispatcher(t, client, slog.New(capture), rec.handle,
					router.WithRunMode(router.PerGroupID),
					router.WithVisibilityTimeout(30),
				)
				msg := buildGroupMessage(t, "m-1", "g", nil)
				msg.receivedAt = time.Now().Add(-tt.age)
				ctx, cancel := context.WithCancel(t.Context())
				require.NoError(t, d.credits.Acquire(ctx, 1))
				d.admit(msg)

				d.startWorkers(ctx)
				synctest.Wait()
				cancel()
				d.stopWorkers()

				if tt.wantRun {
					assert.Equal(t, []string{"m-1"}, rec.snapshot())
					assert.Equal(t, []string{"m-1"}, client.deletes())
					assert.Empty(t, capture.warns())
				} else {
					assert.Empty(t, rec.snapshot())
					assert.Empty(t, client.deletes())
					assert.Equal(t, []loggedWarn{
						{message: skipExpiredWarn, queueURL: "queue-url", receipt: "m-1", groupID: "g"},
					}, capture.warns())
				}
				assert.Empty(t, client.calls())
				assert.Zero(t, trackedCount(d))
				assert.Equal(t, d.credits.Capacity(), d.credits.Available())
			})
		})
	}
}

func TestDispatcherWorkerSkipsWithoutExtensionLeft(t *testing.T) {
	tests := []struct {
		name    string
		age     time.Duration
		limit   int
		wantRun bool
	}{
		{name: "no extension left and half a second remaining is skipped", age: 29*time.Second + 500*time.Millisecond},
		{name: "no extension left and just under one second remaining is skipped", age: 29*time.Second + time.Nanosecond},
		{name: "no extension left and one second remaining runs", age: 29 * time.Second, wantRun: true},
		{name: "extension left and half a second remaining runs", age: 29*time.Second + 500*time.Millisecond, limit: 1, wantRun: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				rec := &handlerRecorder{}
				capture := &captureHandler{}
				client := &fakeSQSClient{}
				d := newTestDispatcher(t, client, slog.New(capture), rec.handle,
					router.WithRunMode(router.PerGroupID),
					router.WithVisibilityTimeout(30),
					router.WithExtensionLimit(tt.limit),
				)
				msg := buildGroupMessage(t, "m-1", "g", nil)
				msg.receivedAt = time.Now().Add(-tt.age)
				ctx, cancel := context.WithCancel(t.Context())
				require.NoError(t, d.credits.Acquire(ctx, 1))
				d.admit(msg)

				d.startWorkers(ctx)
				synctest.Wait()
				cancel()
				d.stopWorkers()

				if tt.wantRun {
					assert.Equal(t, []string{"m-1"}, rec.snapshot())
					assert.Equal(t, []string{"m-1"}, client.deletes())
					assert.Empty(t, capture.warns())
				} else {
					assert.Empty(t, rec.snapshot())
					assert.Empty(t, client.deletes())
					assert.Equal(t, []loggedWarn{
						{message: skipExpiredWarn, queueURL: "queue-url", receipt: "m-1", groupID: "g"},
					}, capture.warns())
				}
				assert.Empty(t, client.calls())
				assert.Zero(t, trackedCount(d))
				assert.Equal(t, d.credits.Capacity(), d.credits.Available())
			})
		})
	}
}

type outcomeObservation struct {
	ctx       context.Context
	tracked   bool
	available int
	calls     int
}

func TestDispatcherProcessUntracksBeforeOutcome(t *testing.T) {
	tests := []struct {
		handler     middleware.Handler
		name        string
		receive     string
		opts        []router.Option
		wantDeletes []string
		viaDLQ      bool
	}{
		{name: "success under the visibility model", handler: nilHandler, wantDeletes: []string{"m-1"}},
		{
			name:        "success under the scheduled retry model",
			handler:     nilHandler,
			opts:        []router.Option{router.WithRetryModel(router.ScheduledRetryModel)},
			wantDeletes: []string{"m-1"},
		},
		{
			name:    "exhausted error observed for the dlq",
			handler: errorHandler,
			receive: "3",
			viaDLQ:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var d *dispatcher
			var msg *message
			var obs outcomeObservation
			observe := func(ctx context.Context) {
				obs = outcomeObservation{
					ctx:       ctx,
					tracked:   isTracked(d, msg),
					available: d.credits.Available(),
					calls:     obs.calls + 1,
				}
			}
			opts := tt.opts
			if opts == nil {
				opts = []router.Option{
					router.WithDLQ(3, router.WithOnDLQ(func(ctx context.Context, _ middleware.Message) { observe(ctx) })),
				}
			}
			client := &hookedSQSClient{
				fakeSQSClient: &fakeSQSClient{},
				onDelete:      func(ctx context.Context, _ string) { observe(ctx) },
			}
			d = newTestDispatcher(t, client, logger.NewNoOp(), tt.handler, opts...)
			msg = buildDLQMessage("m-1", tt.receive)
			require.NoError(t, d.credits.Acquire(context.Background(), 1))
			d.visibility.track(msg)
			require.True(t, isTracked(d, msg))

			d.process(context.Background(), msg)

			require.Equal(t, 1, obs.calls)
			assert.False(t, obs.tracked)
			assert.Equal(t, d.credits.Capacity()-1, obs.available)
			assert.Equal(t, tt.wantDeletes, nilIfEmpty(client.deletes()))
			assert.Empty(t, client.calls())
			assert.Equal(t, d.credits.Capacity(), d.credits.Available())
		})
	}
}

type ctxKey struct{}

func TestOutcomeContext(t *testing.T) {
	tests := []struct {
		newCtx     func() (context.Context, context.CancelFunc)
		name       string
		isDetached bool
	}{
		{
			name: "live context is returned unchanged",
			newCtx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
		},
		{
			name: "live context with a deadline is returned unchanged",
			newCtx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), time.Hour)
			},
		},
		{
			name: "canceled context is detached and bounded",
			newCtx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, cancel
			},
			isDetached: true,
		},
		{
			name: "expired context is detached and bounded",
			newCtx: func() (context.Context, context.CancelFunc) {
				return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			},
			isDetached: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				parent, cancelParent := tt.newCtx()
				defer cancelParent()
				parent = context.WithValue(parent, ctxKey{}, tt.name)

				got, cancel := outcomeContext(parent)

				assert.Equal(t, tt.name, got.Value(ctxKey{}))
				require.NoError(t, got.Err())
				if !tt.isDetached {
					assert.Equal(t, parent, got)
					cancel()
					assert.NoError(t, got.Err())
					return
				}
				deadline, ok := got.Deadline()
				require.True(t, ok)
				assert.Equal(t, time.Now().Add(outcomeTimeout), deadline)
				assert.LessOrEqual(t, outcomeTimeout, 5*time.Second)
				cancel()
				assert.ErrorIs(t, got.Err(), context.Canceled)
			})
		})
	}
}

func TestDispatcherProcessAppliesOutcomeAfterRunContextCanceled(t *testing.T) {
	tests := []struct {
		name        string
		receive     string
		opts        []router.Option
		wantDeletes []string
		fail        bool
		cancelRun   bool
	}{
		{name: "success under the visibility model", cancelRun: true, wantDeletes: []string{"m-1"}},
		{
			name:        "success under the scheduled retry model",
			cancelRun:   true,
			opts:        []router.Option{router.WithRetryModel(router.ScheduledRetryModel)},
			wantDeletes: []string{"m-1"},
		},
		{name: "exhausted error observed for the dlq", cancelRun: true, fail: true, receive: "3"},
		{name: "live run context is used as is", wantDeletes: []string{"m-1"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var obs outcomeObservation
				var observedErr error
				var observedDeadline time.Time
				var hasDeadline bool
				observe := func(ctx context.Context) {
					obs.ctx = ctx
					obs.calls++
					observedErr = ctx.Err()
					observedDeadline, hasDeadline = ctx.Deadline()
				}
				runCtx, cancelRun := context.WithCancel(context.WithValue(t.Context(), ctxKey{}, tt.name))
				defer cancelRun()
				handler := func(context.Context, middleware.Message) error {
					if tt.cancelRun {
						cancelRun()
					}
					if tt.fail {
						return errors.New("boom")
					}
					return nil
				}
				opts := tt.opts
				if opts == nil {
					opts = []router.Option{
						router.WithDLQ(3, router.WithOnDLQ(func(ctx context.Context, _ middleware.Message) { observe(ctx) })),
					}
				}
				client := &hookedSQSClient{
					fakeSQSClient: &fakeSQSClient{},
					onDelete:      func(ctx context.Context, _ string) { observe(ctx) },
				}
				d := newTestDispatcher(t, client, logger.NewNoOp(), handler, opts...)
				msg := buildDLQMessage("m-1", tt.receive)
				require.NoError(t, d.credits.Acquire(context.Background(), 1))
				d.visibility.track(msg)
				start := time.Now()

				d.process(runCtx, msg)

				require.Equal(t, 1, obs.calls)
				require.NoError(t, observedErr)
				assert.Equal(t, tt.name, obs.ctx.Value(ctxKey{}))
				if tt.cancelRun {
					require.True(t, hasDeadline)
					assert.Equal(t, start.Add(outcomeTimeout), observedDeadline)
					assert.ErrorIs(t, obs.ctx.Err(), context.Canceled)
				} else {
					assert.False(t, hasDeadline)
					assert.Equal(t, runCtx, obs.ctx)
				}
				assert.Equal(t, tt.wantDeletes, nilIfEmpty(client.deletes()))
				assert.Equal(t, d.credits.Capacity(), d.credits.Available())
			})
		})
	}
}

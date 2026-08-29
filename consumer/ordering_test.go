package consumer

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

func TestDispatcherOrderedGroupsGate(t *testing.T) {
	tests := []struct {
		name string
		mode router.Mode
		want bool
	}{
		{name: "per group id visibility", mode: router.PerGroupID, want: true},
		{name: "parallel visibility", mode: router.Parallel, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDispatcher(t, &fakeSQSClient{}, logger.NewNoOp(), nilHandler,
				router.WithRunMode(tt.mode),
			)

			assert.Equal(t, tt.want, d.orderedGroups())
			if tt.want {
				assert.NotNil(t, d.newBatchBarrier())
			} else {
				assert.Nil(t, d.newBatchBarrier())
			}
		})
	}
}

func TestDispatcherOrderedGroupsScheduledExcluded(t *testing.T) {
	client := &schedSQS{}
	route, err := router.New("queue", nilHandler,
		router.WithRunMode(router.PerGroupID),
		router.WithScheduledRetry(
			router.WithSchedulerIdentity("arn:aws:sqs:us-east-1:1:queue.fifo", "arn:aws:iam::1:role/r"),
			router.WithScheduledDLQ("https://sqs.us-east-1.amazonaws.com/1/dlq"),
			router.WithMaxRetryCount(3),
			router.WithBackoff(time.Second, 10*time.Second),
		),
	)
	require.NoError(t, err)

	vm := newVisibilityManager(client, "queue-url", 30, 2, logger.NewNoOp())
	d := newDispatcher(client, route, "queue-url", vm, nil, logger.NewNoOp())

	assert.False(t, d.orderedGroups())
	assert.Nil(t, d.newBatchBarrier())
}

func TestDispatcherProcessHoldsBackFailedGroup(t *testing.T) {
	var ran atomic.Bool
	handler := func(_ context.Context, _ middleware.Message) error {
		ran.Store(true)
		return nil
	}

	capture := &captureHandler{}
	client := &fakeSQSClient{}
	d := newTestDispatcher(t, client, slog.New(capture), handler,
		router.WithRunMode(router.PerGroupID),
	)

	barrier := newGroupBarrier()
	barrier.fail("group-key")

	msg := buildGroupMessage(t, "receipt-held", "group-1", nil)
	msg.barrier = barrier
	msg.groupKey = "group-key"

	d.process(context.Background(), msg)
	d.wg.Wait()

	assert.False(t, ran.Load())
	assert.Empty(t, client.deletes())
	assert.Empty(t, client.calls())

	warns := capture.warnAttrs()
	require.Len(t, warns, 1)
	assert.Equal(t, "receipt-held", warns[0]["receipt_handle"])
	assert.Equal(t, "group-1", warns[0]["group_id"])
}

func TestDispatcherProcessErrorMarksGroupFailed(t *testing.T) {
	client := &fakeSQSClient{}
	d := newTestDispatcher(t, client, logger.NewNoOp(), errorHandler,
		router.WithRunMode(router.PerGroupID),
	)

	barrier := newGroupBarrier()
	msg := buildGroupMessage(t, "receipt-err", "group-1", nil)
	msg.barrier = barrier
	msg.groupKey = "group-key"

	d.process(context.Background(), msg)
	d.wg.Wait()

	assert.True(t, barrier.failed("group-key"))
	assert.Empty(t, client.deletes())
}

func TestDispatcherProcessBackoffMarksGroupFailed(t *testing.T) {
	handler := func(_ context.Context, m middleware.Message) error {
		m.(*message).Backoff(20 * time.Second)
		return nil
	}

	client := &fakeSQSClient{}
	d := newTestDispatcher(t, client, logger.NewNoOp(), handler,
		router.WithRunMode(router.PerGroupID),
	)

	barrier := newGroupBarrier()
	msg := buildGroupMessage(t, "receipt-backoff", "group-1", nil)
	msg.barrier = barrier
	msg.groupKey = "group-key"

	d.process(context.Background(), msg)
	d.wg.Wait()

	assert.True(t, barrier.failed("group-key"))
	assert.Empty(t, client.deletes())
	assert.Equal(t, []int32{20}, client.calls())
}

func TestDispatcherProcessSuccessDoesNotMarkGroup(t *testing.T) {
	client := &fakeSQSClient{}
	d := newTestDispatcher(t, client, logger.NewNoOp(), nilHandler,
		router.WithRunMode(router.PerGroupID),
	)

	barrier := newGroupBarrier()
	msg := buildGroupMessage(t, "receipt-ok", "group-1", nil)
	msg.barrier = barrier
	msg.groupKey = "group-key"

	d.process(context.Background(), msg)
	d.wg.Wait()

	assert.False(t, barrier.failed("group-key"))
	assert.Equal(t, []string{"receipt-ok"}, client.deletes())
}

func TestDispatcherParallelIgnoresBarrier(t *testing.T) {
	client := &fakeSQSClient{}
	d := newTestDispatcher(t, client, logger.NewNoOp(), nilHandler,
		router.WithRunMode(router.Parallel),
	)

	barrier := newGroupBarrier()
	barrier.fail("group-key")

	msg := testMessage("receipt-parallel")
	msg.barrier = barrier
	msg.groupKey = "group-key"

	d.process(context.Background(), msg)
	d.wg.Wait()

	assert.Equal(t, []string{"receipt-parallel"}, client.deletes())
}

func TestDispatcherPerGroupIDHoldsTailAfterHeadFails(t *testing.T) {
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

	client := &fakeSQSClient{}
	d := newTestDispatcher(t, client, logger.NewNoOp(), handler,
		router.WithRunMode(router.PerGroupID),
		router.WithWorkerPoolSize(1),
	)

	barrier := newGroupBarrier()
	for _, id := range []string{"msg-1", "msg-2", "msg-3", "msg-4"} {
		msg := buildGroupMessage(t, id, "g", nil)
		msg.barrier = barrier
		msg.groupKey = "g"
		d.process(context.Background(), msg)
		d.wg.Wait()
	}

	mu.Lock()
	got := append([]string(nil), called...)
	mu.Unlock()

	assert.Equal(t, []string{"msg-1"}, got)
	assert.Empty(t, client.deletes())
	assert.True(t, barrier.failed("g"))
}

func TestDispatcherPerGroupIDIndependentGroups(t *testing.T) {
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

	barrier := newGroupBarrier()
	steps := []struct {
		receipt string
		key     string
	}{
		{receipt: "a-1", key: "A"},
		{receipt: "b-1", key: "B"},
		{receipt: "a-2", key: "A"},
	}
	for _, s := range steps {
		msg := buildGroupMessage(t, s.receipt, s.key, nil)
		msg.barrier = barrier
		msg.groupKey = s.key
		d.process(context.Background(), msg)
		d.wg.Wait()
	}

	assert.Equal(t, []string{"b-1"}, client.deletes())
	assert.True(t, barrier.failed("A"))
	assert.False(t, barrier.failed("B"))
}

func TestDispatcherPerGroupIDEndToEndPreservesOrder(t *testing.T) {
	var mu sync.Mutex
	var called []string
	handler := func(_ context.Context, m middleware.Message) error {
		id := m.(*message).Identifier()
		mu.Lock()
		called = append(called, id)
		mu.Unlock()
		if id == "m1" {
			return errors.New("boom")
		}
		return nil
	}

	client := &fakeSQSClient{}
	d := newTestDispatcher(t, client, logger.NewNoOp(), handler,
		router.WithRunMode(router.PerGroupID),
		router.WithWorkerPoolSize(1),
		router.WithMaxMessages(10),
	)

	barrier := d.newBatchBarrier()
	require.NotNil(t, barrier)

	ctx := context.Background()
	d.start(ctx)
	for _, id := range []string{"m1", "m2", "m3", "m4"} {
		msg := buildGroupMessage(t, id, "g", nil)
		msg.barrier = barrier
		d.dispatch(ctx, msg)
	}
	d.stop()

	mu.Lock()
	got := append([]string(nil), called...)
	mu.Unlock()

	assert.Equal(t, []string{"m1"}, got)
	assert.Empty(t, client.deletes())
}

package consumer

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
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
	require.NoError(t, d.credits.acquire(context.Background(), 1))
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
		require.NoError(t, d.credits.acquire(context.Background(), 2))
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

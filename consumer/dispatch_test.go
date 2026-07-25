package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/logger"
	"github.com/silviolleite/loafer-awsx/middleware"
	"github.com/silviolleite/loafer-awsx/router"
)

func newTestDispatcher(tb require.TestingT, client SQSClient, log *slog.Logger, handler middleware.Handler, opts ...router.Option) *dispatcher {
	route, err := router.New("queue", handler, opts...)
	require.NoError(tb, err)

	vm := newVisibilityManager(client, "queue-url", route.VisibilityTimeout(), route.ExtensionLimit(), logger.NewNoOp())
	vm.sleepInterval = time.Hour

	return newDispatcher(client, route, "queue-url", vm, log)
}

func buildGroupMessage(tb require.TestingT, receipt, groupID string, custom map[string]string) *message {
	attrs := make(map[string]messageAttribute, len(custom))
	for k, v := range custom {
		attrs[k] = messageAttribute{Type: "String", Value: v}
	}

	envelope := map[string]any{
		"Timestamp":         time.Time{},
		"Message":           "",
		"MessageAttributes": attrs,
	}
	body, err := json.Marshal(envelope)
	require.NoError(tb, err)

	return newMessage(types.Message{
		ReceiptHandle: aws.String(receipt),
		Attributes:    map[string]string{messageGroupIDKey: groupID},
		Body:          aws.String(string(body)),
	})
}

func nilHandler(_ context.Context, _ middleware.Message) error { return nil }

func errorHandler(_ context.Context, _ middleware.Message) error { return errors.New("handler boom") }

func buildDLQMessage(receipt, receiveCount string) *message {
	attrs := map[string]string{}
	if receiveCount != "" {
		attrs[approximateReceiveCountKey] = receiveCount
	}
	return newMessage(types.Message{
		ReceiptHandle: aws.String(receipt),
		Attributes:    attrs,
	})
}

func TestDispatcherProcessDeletesOnSuccess(t *testing.T) {
	client := &fakeSQSClient{}
	d := newTestDispatcher(t, client, logger.NewNoOp(), nilHandler)

	msg := testMessage("receipt-success")
	d.process(context.Background(), msg)
	d.wg.Wait()

	assert.Equal(t, []string{"receipt-success"}, client.deletes())
	assert.Empty(t, client.calls())
}

func TestDispatcherProcessLeavesMessageOnError(t *testing.T) {
	handlerErr := errors.New("handler boom")
	handler := func(_ context.Context, _ middleware.Message) error { return handlerErr }

	capture := &captureHandler{}
	client := &fakeSQSClient{}
	d := newTestDispatcher(t, client, slog.New(capture), handler)

	msg := buildGroupMessage(t, "receipt-error", "group-7", nil)
	d.process(context.Background(), msg)
	d.wg.Wait()

	assert.Empty(t, client.deletes())
	assert.Empty(t, client.calls())

	errs := capture.errorAttrs()
	require.Len(t, errs, 1)
	assert.Equal(t, "receipt-error", errs[0]["receipt_handle"])
	assert.Equal(t, "group-7", errs[0]["group_id"])
	assert.Contains(t, errs[0], "body")
	assert.Contains(t, errs[0]["error"], "handler boom")
}

func TestDispatcherProcessChangesVisibilityOnBackoff(t *testing.T) {
	tests := []struct {
		name    string
		backoff time.Duration
		want    int32
	}{
		{name: "positive backoff", backoff: 45 * time.Second, want: 45},
		{name: "negative backoff clamped", backoff: -3 * time.Second, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := func(_ context.Context, m middleware.Message) error {
				m.(*message).Backoff(tt.backoff)
				return nil
			}

			client := &fakeSQSClient{}
			d := newTestDispatcher(t, client, logger.NewNoOp(), handler)

			msg := testMessage("receipt-backoff")
			d.process(context.Background(), msg)
			d.wg.Wait()

			assert.Empty(t, client.deletes())
			assert.Equal(t, []int32{tt.want}, client.calls())
			assert.Equal(t, []string{"receipt-backoff"}, client.handles())
		})
	}
}

func TestDispatcherProcessLeavesMessageOnBackoffWithError(t *testing.T) {
	handler := func(_ context.Context, m middleware.Message) error {
		m.(*message).Backoff(30 * time.Second)
		return errors.New("still failing")
	}

	client := &fakeSQSClient{}
	d := newTestDispatcher(t, client, logger.NewNoOp(), handler)

	msg := testMessage("receipt-backoff-error")
	d.process(context.Background(), msg)
	d.wg.Wait()

	assert.Empty(t, client.deletes())
	assert.Equal(t, []int32{30}, client.calls())
}

func TestDispatcherProcessLogsDeleteFailure(t *testing.T) {
	capture := &captureHandler{}
	client := &fakeSQSClient{deleteErr: errors.New("delete failed")}
	d := newTestDispatcher(t, client, slog.New(capture), nilHandler)

	msg := testMessage("receipt-del-fail")
	d.process(context.Background(), msg)
	d.wg.Wait()

	assert.Equal(t, []string{"receipt-del-fail"}, client.deletes())
	assert.Equal(t, 1, capture.errorCount())
}

func TestDispatcherAppliesMiddlewareInOrder(t *testing.T) {
	var order []string
	record := func(name string) middleware.Middleware {
		return func(next middleware.Handler) middleware.Handler {
			return func(ctx context.Context, msg middleware.Message) error {
				order = append(order, name)
				return next(ctx, msg)
			}
		}
	}

	handler := func(_ context.Context, _ middleware.Message) error {
		order = append(order, "handler")
		return nil
	}

	route, err := router.New("queue", handler, router.WithMiddleware(record("route")))
	require.NoError(t, err)

	client := &fakeSQSClient{}
	vm := newVisibilityManager(client, "queue-url", route.VisibilityTimeout(), route.ExtensionLimit(), logger.NewNoOp())
	vm.sleepInterval = time.Hour
	d := newDispatcher(client, route, "queue-url", vm, logger.NewNoOp(), record("global"))

	d.process(context.Background(), testMessage("receipt-mw"))
	d.wg.Wait()

	assert.Equal(t, []string{"global", "route", "handler"}, order)
}

func TestNewDispatcherNilLoggerDefaultsToNoOp(t *testing.T) {
	client := &fakeSQSClient{}
	route, err := router.New("queue", nilHandler)
	require.NoError(t, err)
	vm := newVisibilityManager(client, "queue-url", route.VisibilityTimeout(), route.ExtensionLimit(), logger.NewNoOp())

	d := newDispatcher(client, route, "queue-url", vm, nil)

	require.NotNil(t, d.log)
	assert.Equal(t, route.WorkerPoolSize(), d.workerPoolSize)
	assert.Equal(t, int(route.MaxMessages()), d.bufferSize)
}

func TestDispatcherParallelDistributesAcrossWorkers(t *testing.T) {
	client := &fakeSQSClient{}
	d := newTestDispatcher(t, client, logger.NewNoOp(), nilHandler,
		router.WithRunMode(router.Parallel),
		router.WithWorkerPoolSize(3),
	)

	d.channels = make([]chan *message, d.workerPoolSize)
	for i := range d.channels {
		d.channels[i] = make(chan *message, 4)
	}

	sequence := []int{0, 1, 2, 0, 1, 2}
	var cursor int
	d.randIntN = func(int) int {
		idx := sequence[cursor]
		cursor++
		return idx
	}

	for range sequence {
		d.dispatch(context.Background(), testMessage("receipt"))
	}

	for i := range d.channels {
		assert.Len(t, d.channels[i], 2)
	}
}

func TestDispatcherParallelUsesInjectedSelector(t *testing.T) {
	client := &fakeSQSClient{}
	d := newTestDispatcher(t, client, logger.NewNoOp(), nilHandler,
		router.WithRunMode(router.Parallel),
		router.WithWorkerPoolSize(4),
	)
	d.randIntN = func(n int) int {
		assert.Equal(t, 4, n)
		return 2
	}

	assert.Equal(t, 2, d.workerIndex(testMessage("receipt")))
}

func TestDispatcherEndToEndProcessesAllMessages(t *testing.T) {
	client := &fakeSQSClient{}
	d := newTestDispatcher(t, client, logger.NewNoOp(), nilHandler,
		router.WithWorkerPoolSize(3),
		router.WithMaxMessages(5),
	)

	ctx := context.Background()
	d.start(ctx)
	for i := 0; i < 12; i++ {
		d.dispatch(ctx, testMessage("receipt"))
	}
	d.stop()

	assert.Len(t, client.deletes(), 12)
}

func TestDispatcherDispatchStopsOnContextCancel(t *testing.T) {
	client := &fakeSQSClient{}
	d := newTestDispatcher(t, client, logger.NewNoOp(), nilHandler,
		router.WithWorkerPoolSize(1),
		router.WithMaxMessages(1),
	)

	d.channels = []chan *message{make(chan *message, 1)}
	d.channels[0] <- testMessage("blocker")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.NotPanics(t, func() {
		d.dispatch(ctx, testMessage("receipt"))
	})
	assert.Len(t, d.channels[0], 1)
}

func TestDispatcherPerGroupIDConsistencyProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		poolSize := rapid.IntRange(1, 16).Draw(rt, "poolSize")
		groupID := rapid.String().Draw(rt, "groupID")

		fieldNames := rapid.SliceOfDistinct(
			rapid.StringMatching(`[a-z][a-z0-9]{0,6}`),
			func(s string) string { return s },
		).Draw(rt, "fields")

		custom := make(map[string]string, len(fieldNames))
		for _, name := range fieldNames {
			custom[name] = rapid.String().Draw(rt, "value-"+name)
		}

		opts := []router.Option{
			router.WithRunMode(router.PerGroupID),
			router.WithWorkerPoolSize(poolSize),
		}
		if len(fieldNames) > 0 {
			opts = append(opts, router.WithCustomGroupFields(fieldNames...))
		}

		d := newTestDispatcher(rt, &fakeSQSClient{}, logger.NewNoOp(), nilHandler, opts...)

		first := buildGroupMessage(rt, "receipt-a", groupID, custom)
		second := buildGroupMessage(rt, "receipt-b", groupID, custom)

		idx1 := d.workerIndex(first)
		idx2 := d.workerIndex(second)

		assert.Equal(rt, idx1, idx2)
		assert.GreaterOrEqual(rt, idx1, 0)
		assert.Less(rt, idx1, poolSize)
	})
}

func TestDispatcherPerGroupIDDifferentKeysStayInRange(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		poolSize := rapid.IntRange(1, 8).Draw(rt, "poolSize")
		groupID := rapid.String().Draw(rt, "groupID")

		d := newTestDispatcher(rt, &fakeSQSClient{}, logger.NewNoOp(), nilHandler,
			router.WithRunMode(router.PerGroupID),
			router.WithWorkerPoolSize(poolSize),
		)

		idx := d.workerIndex(buildGroupMessage(rt, "receipt", groupID, nil))

		assert.GreaterOrEqual(rt, idx, 0)
		assert.Less(rt, idx, poolSize)
	})
}

func TestDispatcherProcessObservesDLQWhenExhausted(t *testing.T) {
	var (
		metricRoute string
		onDLQMsg    middleware.Message
		onDLQCalls  int
	)
	onDLQ := func(_ context.Context, m middleware.Message) {
		onDLQCalls++
		onDLQMsg = m
	}

	capture := &captureHandler{}
	client := &fakeSQSClient{}
	d := newTestDispatcher(t, client, slog.New(capture), errorHandler,
		router.WithDLQ(3, router.WithOnDLQ(onDLQ)),
	)
	d.dlqMetric = func(route string) { metricRoute = route }

	msg := buildDLQMessage("receipt-dlq", "3")
	d.process(context.Background(), msg)
	d.wg.Wait()

	assert.Empty(t, client.deletes())
	assert.Empty(t, client.calls())

	errs := capture.errorAttrs()
	require.Len(t, errs, 1)
	assert.Equal(t, "queue", errs[0]["queue_name"])
	assert.Equal(t, "receipt-dlq", errs[0]["message_id"])
	assert.Equal(t, "3", errs[0]["receive_count"])

	assert.Equal(t, "queue", metricRoute)
	assert.Equal(t, 1, onDLQCalls)
	assert.Equal(t, msg, onDLQMsg)
}

func TestDispatcherProcessDLQBelowThresholdSkipsSignals(t *testing.T) {
	var metricCalls, onDLQCalls int

	capture := &captureHandler{}
	client := &fakeSQSClient{}
	d := newTestDispatcher(t, client, slog.New(capture), errorHandler,
		router.WithDLQ(5, router.WithOnDLQ(func(context.Context, middleware.Message) { onDLQCalls++ })),
	)
	d.dlqMetric = func(string) { metricCalls++ }

	msg := buildDLQMessage("receipt-below", "2")
	d.process(context.Background(), msg)
	d.wg.Wait()

	assert.Empty(t, client.deletes())
	assert.Equal(t, 0, metricCalls)
	assert.Equal(t, 0, onDLQCalls)

	errs := capture.errorAttrs()
	require.Len(t, errs, 1)
	assert.Equal(t, "receipt-below", errs[0]["receipt_handle"])
	assert.Contains(t, errs[0], "body")
}

func TestDispatcherProcessNoDLQConfiguredSkipsSignals(t *testing.T) {
	var metricCalls int

	capture := &captureHandler{}
	client := &fakeSQSClient{}
	d := newTestDispatcher(t, client, slog.New(capture), errorHandler)
	d.dlqMetric = func(string) { metricCalls++ }

	msg := buildDLQMessage("receipt-nodlq", "10")
	d.process(context.Background(), msg)
	d.wg.Wait()

	assert.Empty(t, client.deletes())
	assert.Equal(t, 0, metricCalls)

	errs := capture.errorAttrs()
	require.Len(t, errs, 1)
	assert.Equal(t, "receipt-nodlq", errs[0]["receipt_handle"])
}

func TestDispatcherProcessDLQWithoutMetricOrCallback(t *testing.T) {
	capture := &captureHandler{}
	client := &fakeSQSClient{}
	d := newTestDispatcher(t, client, slog.New(capture), errorHandler, router.WithDLQ(1))

	msg := buildDLQMessage("receipt-plain-dlq", "1")
	assert.NotPanics(t, func() {
		d.process(context.Background(), msg)
		d.wg.Wait()
	})

	assert.Empty(t, client.deletes())
	errs := capture.errorAttrs()
	require.Len(t, errs, 1)
	assert.Equal(t, "receipt-plain-dlq", errs[0]["message_id"])
}

func TestDispatcherProcessDLQReceiveCountParsing(t *testing.T) {
	tests := []struct {
		name         string
		receiveCount string
	}{
		{name: "missing attribute", receiveCount: ""},
		{name: "non numeric", receiveCount: "not-a-number"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var metricCalls int

			capture := &captureHandler{}
			client := &fakeSQSClient{}
			d := newTestDispatcher(t, client, slog.New(capture), errorHandler, router.WithDLQ(1))
			d.dlqMetric = func(string) { metricCalls++ }

			msg := buildDLQMessage("receipt", tt.receiveCount)
			d.process(context.Background(), msg)
			d.wg.Wait()

			assert.Empty(t, client.deletes())
			assert.Equal(t, 0, metricCalls)

			errs := capture.errorAttrs()
			require.Len(t, errs, 1)
			assert.Equal(t, "receipt", errs[0]["receipt_handle"])
		})
	}
}

func TestDispatcherProcessDLQConfiguredSuccessDeletes(t *testing.T) {
	var metricCalls, onDLQCalls int

	client := &fakeSQSClient{}
	d := newTestDispatcher(t, client, logger.NewNoOp(), nilHandler,
		router.WithDLQ(1, router.WithOnDLQ(func(context.Context, middleware.Message) { onDLQCalls++ })),
	)
	d.dlqMetric = func(string) { metricCalls++ }

	msg := buildDLQMessage("receipt-ok", "9")
	d.process(context.Background(), msg)
	d.wg.Wait()

	assert.Equal(t, []string{"receipt-ok"}, client.deletes())
	assert.Equal(t, 0, metricCalls)
	assert.Equal(t, 0, onDLQCalls)
}

func TestDispatcherProcessDLQThresholdProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		maxReceive := rapid.IntRange(1, 20).Draw(rt, "max")
		receiveCount := rapid.IntRange(0, 25).Draw(rt, "receiveCount")

		var metricCalls, onDLQCalls int

		capture := &captureHandler{}
		client := &fakeSQSClient{}
		d := newTestDispatcher(rt, client, slog.New(capture), errorHandler,
			router.WithDLQ(maxReceive, router.WithOnDLQ(func(context.Context, middleware.Message) { onDLQCalls++ })),
		)
		d.dlqMetric = func(string) { metricCalls++ }

		msg := buildDLQMessage("receipt", strconv.Itoa(receiveCount))
		d.process(context.Background(), msg)
		d.wg.Wait()

		if receiveCount >= maxReceive {
			assert.Equal(rt, 1, metricCalls)
			assert.Equal(rt, 1, onDLQCalls)
		} else {
			assert.Equal(rt, 0, metricCalls)
			assert.Equal(rt, 0, onDLQCalls)
		}

		assert.Empty(rt, client.deletes())
		require.Len(rt, capture.errorAttrs(), 1)
	})
}

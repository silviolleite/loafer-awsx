package consumer_test

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
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

type stubClient struct {
	getErr        error
	receiveFn     func(call int) (*sqs.ReceiveMessageOutput, error)
	onDelete      func()
	queueName     string
	queueURL      string
	receiveInputs []sqs.ReceiveMessageInput
	deleted       []string
	changeVis     []int32
	getCalls      int
	receiveCalls  int
	mu            sync.Mutex
}

func (s *stubClient) ReceiveMessage(_ context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	s.mu.Lock()
	call := s.receiveCalls
	s.receiveCalls++
	s.receiveInputs = append(s.receiveInputs, *in)
	fn := s.receiveFn
	s.mu.Unlock()

	if fn != nil {
		return fn(call)
	}
	return &sqs.ReceiveMessageOutput{}, nil
}

func (s *stubClient) DeleteMessage(_ context.Context, in *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	s.mu.Lock()
	if in.ReceiptHandle != nil {
		s.deleted = append(s.deleted, *in.ReceiptHandle)
	}
	onDelete := s.onDelete
	s.mu.Unlock()

	if onDelete != nil {
		onDelete()
	}
	return &sqs.DeleteMessageOutput{}, nil
}

func (s *stubClient) ChangeMessageVisibility(_ context.Context, in *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.changeVis = append(s.changeVis, in.VisibilityTimeout)
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func (s *stubClient) GetQueueUrl(_ context.Context, in *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getCalls++
	if in.QueueName != nil {
		s.queueName = *in.QueueName
	}
	if s.getErr != nil {
		return nil, s.getErr
	}
	if s.queueURL == "" {
		return &sqs.GetQueueUrlOutput{}, nil
	}
	return &sqs.GetQueueUrlOutput{QueueUrl: aws.String(s.queueURL)}, nil
}

func (s *stubClient) SendMessage(_ context.Context, _ *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	return &sqs.SendMessageOutput{}, nil
}

func (s *stubClient) receives() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.receiveCalls
}

func (s *stubClient) firstReceiveInput() sqs.ReceiveMessageInput {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.receiveInputs[0]
}

func (s *stubClient) queueNameSeen() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.queueName
}

func (s *stubClient) deletedHandles() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.deleted))
	copy(out, s.deleted)
	return out
}

func (s *stubClient) resolveCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getCalls
}

func (s *stubClient) visibilityChanges() []int32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int32, len(s.changeVis))
	copy(out, s.changeVis)
	return out
}

type countingHandler struct {
	records []slog.Record
	mu      sync.Mutex
}

func (h *countingHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *countingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *countingHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }

func (h *countingHandler) WithGroup(_ string) slog.Handler { return h }

func (h *countingHandler) errorCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	var n int
	for _, r := range h.records {
		if r.Level == slog.LevelError {
			n++
		}
	}
	return n
}

type recorder struct {
	handled []string
	mu      sync.Mutex
}

func (r *recorder) handler() middleware.Handler {
	return func(_ context.Context, m middleware.Message) error {
		r.mu.Lock()
		r.handled = append(r.handled, m.Identifier())
		r.mu.Unlock()
		return nil
	}
}

func (r *recorder) handledIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.handled))
	copy(out, r.handled)
	return out
}

func newRoute(tb require.TestingT, handler middleware.Handler, opts ...router.Option) *router.Route {
	route, err := router.New("orders-queue", handler, opts...)
	require.NoError(tb, err)
	return route
}

func newConsumer(tb require.TestingT, client consumer.SQSClient, route *router.Route, opts ...consumer.Option) *consumer.Consumer {
	c, err := consumer.New(client, route, opts...)
	require.NoError(tb, err)
	return c
}

type recorderFunc struct {
	dlq        func(string)
	success    func(string)
	retry      func(string)
	deadLetter func(string)
}

func (r recorderFunc) IncDLQ(route string) {
	if r.dlq != nil {
		r.dlq(route)
	}
}

func (r recorderFunc) IncSuccess(route string) {
	if r.success != nil {
		r.success(route)
	}
}

func (r recorderFunc) IncRetry(route string) {
	if r.retry != nil {
		r.retry(route)
	}
}

func (r recorderFunc) IncDeadLetter(route string) {
	if r.deadLetter != nil {
		r.deadLetter(route)
	}
}

func messageBatch(handles ...string) *sqs.ReceiveMessageOutput {
	msgs := make([]types.Message, len(handles))
	for i, h := range handles {
		msgs[i] = types.Message{ReceiptHandle: aws.String(h)}
	}
	return &sqs.ReceiveMessageOutput{Messages: msgs}
}

func rawMessage(handle, body string) types.Message {
	return types.Message{
		ReceiptHandle: aws.String(handle),
		Body:          aws.String(body),
	}
}

func nilHandler(_ context.Context, _ middleware.Message) error { return nil }

func scheduledRoute(tb require.TestingT, handler middleware.Handler) *router.Route {
	return newRoute(tb, handler,
		router.WithScheduledRetry(
			router.WithSchedulerIdentity("arn:aws:sqs:us-east-1:123456789012:orders.fifo", "arn:aws:iam::123456789012:role/scheduler"),
			router.WithScheduledDLQ("https://sqs.us-east-1.amazonaws.com/123456789012/orders-dlq"),
			router.WithMaxRetryCount(5),
			router.WithBackoff(1000*time.Millisecond, 2000*time.Millisecond),
		),
	)
}

func dlqReuseCounter(tb require.TestingT, reg *prometheus.Registry) *prometheus.CounterVec {
	counter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "loafer_messages_dlq_total",
		Help: "Total messages observed as exhausted (receive count reached maxReceiveCount; redriven by AWS SQS)",
	}, []string{"route"})

	err := reg.Register(counter)
	if err == nil {
		return counter
	}

	var are prometheus.AlreadyRegisteredError
	require.ErrorAs(tb, err, &are)
	existing, ok := are.ExistingCollector.(*prometheus.CounterVec)
	require.True(tb, ok)
	return existing
}

func dlqCounterValue(tb require.TestingT, reg *prometheus.Registry, route string) float64 {
	families, err := reg.Gather()
	require.NoError(tb, err)

	for _, family := range families {
		if family.GetName() != "loafer_messages_dlq_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			if metricHasRoute(metric, route) {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func metricHasRoute(metric *dto.Metric, route string) bool {
	for _, label := range metric.GetLabel() {
		if label.GetName() == "route" && label.GetValue() == route {
			return true
		}
	}
	return false
}

func TestNewReturnsErrorWhenClientNil(t *testing.T) {
	c, err := consumer.New(nil, newRoute(t, nilHandler))

	require.Error(t, err)
	assert.Nil(t, c)
	assert.True(t, stderrors.Is(err, verrors.ErrNoSQSClient))
}

func TestNewReturnsErrorWhenRouteNil(t *testing.T) {
	c, err := consumer.New(&stubClient{}, nil)

	require.Error(t, err)
	assert.Nil(t, c)
	assert.True(t, stderrors.Is(err, verrors.ErrNoRoute))
}

func TestRunReturnsErrorWhenQueueResolveFails(t *testing.T) {
	client := &stubClient{getErr: stderrors.New("access denied")}
	c := newConsumer(t, client, newRoute(t, nilHandler))

	err := c.Run(context.Background())

	require.Error(t, err)
	assert.True(t, stderrors.Is(err, verrors.ErrQueueResolve))
	assert.Equal(t, 0, client.receives())
}

func TestRunReturnsErrorWhenQueueURLMissing(t *testing.T) {
	client := &stubClient{}
	c := newConsumer(t, client, newRoute(t, nilHandler))

	err := c.Run(context.Background())

	require.Error(t, err)
	assert.True(t, stderrors.Is(err, verrors.ErrQueueResolve))
	assert.Equal(t, 0, client.receives())
}

func TestRunResolvesQueueURLBeforePolling(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx, cancel := context.WithCancel(context.Background())
	client := &stubClient{queueURL: "https://sqs/orders"}
	client.receiveFn = func(int) (*sqs.ReceiveMessageOutput, error) {
		cancel()
		return &sqs.ReceiveMessageOutput{}, nil
	}

	c := newConsumer(t, client, newRoute(t, nilHandler))

	require.NoError(t, c.Run(ctx))
	assert.Equal(t, "orders-queue", client.queueNameSeen())
	in := client.firstReceiveInput()
	require.NotNil(t, in.QueueUrl)
	assert.Equal(t, "https://sqs/orders", *in.QueueUrl)
}

func TestRunReceiveUsesConfiguredParameters(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx, cancel := context.WithCancel(context.Background())
	client := &stubClient{queueURL: "https://sqs/orders"}
	client.receiveFn = func(int) (*sqs.ReceiveMessageOutput, error) {
		cancel()
		return &sqs.ReceiveMessageOutput{}, nil
	}

	route := newRoute(t, nilHandler, router.WithMaxMessages(7), router.WithWaitTimeSeconds(15), router.WithVisibilityTimeout(45))
	c := newConsumer(t, client, route)

	require.NoError(t, c.Run(ctx))

	in := client.firstReceiveInput()
	assert.Equal(t, int32(7), in.MaxNumberOfMessages)
	assert.Equal(t, int32(15), in.WaitTimeSeconds)
	assert.Equal(t, int32(45), in.VisibilityTimeout)
	assert.Equal(t, []string{"All"}, in.MessageAttributeNames)
	assert.Equal(t, []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll}, in.MessageSystemAttributeNames)
	require.NotNil(t, in.QueueUrl)
	assert.Equal(t, "https://sqs/orders", *in.QueueUrl)
}

func TestRunRetriesAfterReceiveError(t *testing.T) {
	defer goleak.VerifyNone(t)

	rec := &recorder{}
	logs := &countingHandler{}
	deleted := newCountdown(1)
	ctx, cancel := context.WithCancel(context.Background())
	client := &stubClient{queueURL: "https://sqs/orders", onDelete: deleted.tick}
	client.receiveFn = func(call int) (*sqs.ReceiveMessageOutput, error) {
		switch call {
		case 0:
			return nil, stderrors.New("throttled")
		case 1:
			return messageBatch("receipt-1"), nil
		default:
			<-deleted.done
			cancel()
			return &sqs.ReceiveMessageOutput{}, nil
		}
	}

	c := newConsumer(t, client, newRoute(t, rec.handler()),
		consumer.WithRetryTimeout(5*time.Millisecond),
		consumer.WithLogger(slog.New(logs)),
	)

	require.NoError(t, c.Run(ctx))
	assert.GreaterOrEqual(t, client.receives(), 3)
	assert.Equal(t, []string{"receipt-1"}, client.deletedHandles())
	assert.Equal(t, 1, logs.errorCount())
}

func TestRunRetryWaitHonorsContextCancellation(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx, cancel := context.WithCancel(context.Background())
	client := &stubClient{queueURL: "https://sqs/orders"}
	client.receiveFn = func(int) (*sqs.ReceiveMessageOutput, error) {
		return nil, stderrors.New("throttled")
	}

	c := newConsumer(t, client, newRoute(t, nilHandler),
		consumer.WithRetryTimeout(30*time.Second),
		consumer.WithLogger(slog.New(&countingHandler{})),
	)

	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	require.Eventually(t, func() bool { return client.receives() >= 1 }, time.Second, time.Millisecond)
	cancel()

	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

func TestRunReturnsWhenReceiveErrorsAfterCancellation(t *testing.T) {
	defer goleak.VerifyNone(t)

	logs := &countingHandler{}
	ctx, cancel := context.WithCancel(context.Background())
	client := &stubClient{queueURL: "https://sqs/orders"}
	client.receiveFn = func(int) (*sqs.ReceiveMessageOutput, error) {
		cancel()
		return nil, stderrors.New("closed")
	}

	c := newConsumer(t, client, newRoute(t, nilHandler),
		consumer.WithLogger(slog.New(logs)),
	)

	require.NoError(t, c.Run(ctx))
	assert.Equal(t, 0, logs.errorCount())
}

func TestRunContextCancellationStopsPolling(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx, cancel := context.WithCancel(context.Background())
	client := &stubClient{queueURL: "https://sqs/orders"}
	client.receiveFn = func(int) (*sqs.ReceiveMessageOutput, error) {
		cancel()
		return &sqs.ReceiveMessageOutput{}, nil
	}

	c := newConsumer(t, client, newRoute(t, nilHandler))

	require.NoError(t, c.Run(ctx))
	assert.GreaterOrEqual(t, client.receives(), 1)
}

func TestRunCompletesInFlightMessagesBeforeReturn(t *testing.T) {
	defer goleak.VerifyNone(t)

	const workers = 2
	rec := &recorder{}
	record := rec.handler()
	running := newCountdown(workers)
	handler := func(ctx context.Context, m middleware.Message) error {
		running.tick()
		<-ctx.Done()
		return record(ctx, m)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &stubClient{queueURL: "https://sqs/orders"}
	client.receiveFn = func(call int) (*sqs.ReceiveMessageOutput, error) {
		if call == 0 {
			return messageBatch("receipt-1", "receipt-2", "receipt-3", "receipt-4", "receipt-5"), nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}

	c := newConsumer(t, client, newRoute(t, handler, router.WithWorkerPoolSize(workers)))

	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	<-running.done
	cancel()

	require.NoError(t, <-done)
	assert.ElementsMatch(t, []string{"receipt-1", "receipt-2"}, rec.handledIDs())
	assert.ElementsMatch(t, []string{"receipt-1", "receipt-2"}, client.deletedHandles())
	assert.Equal(t, []int32{0, 0, 0}, client.visibilityChanges())
}

func TestRunAppliesGlobalMiddlewareOutermost(t *testing.T) {
	defer goleak.VerifyNone(t)

	var mu sync.Mutex
	var order []string
	record := func(name string) middleware.Middleware {
		return func(next middleware.Handler) middleware.Handler {
			return func(ctx context.Context, msg middleware.Message) error {
				mu.Lock()
				order = append(order, name)
				mu.Unlock()
				return next(ctx, msg)
			}
		}
	}

	handler := func(ctx context.Context, msg middleware.Message) error {
		mu.Lock()
		order = append(order, "handler")
		mu.Unlock()
		return nil
	}

	deleted := newCountdown(1)
	ctx, cancel := context.WithCancel(context.Background())
	client := &stubClient{queueURL: "https://sqs/orders", onDelete: deleted.tick}
	client.receiveFn = func(call int) (*sqs.ReceiveMessageOutput, error) {
		if call == 0 {
			return messageBatch("receipt-1"), nil
		}
		<-deleted.done
		cancel()
		return &sqs.ReceiveMessageOutput{}, nil
	}

	route := newRoute(t, handler, router.WithMiddleware(record("route")))
	c := newConsumer(t, client, route, consumer.WithGlobalMiddleware(record("global")))

	require.NoError(t, c.Run(ctx))
	assert.Equal(t, []string{"global", "route", "handler"}, order)
}

func TestNewIgnoresNilOptionAndNilLogger(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx, cancel := context.WithCancel(context.Background())
	client := &stubClient{queueURL: "https://sqs/orders"}
	client.receiveFn = func(int) (*sqs.ReceiveMessageOutput, error) {
		cancel()
		return &sqs.ReceiveMessageOutput{}, nil
	}

	c := newConsumer(t, client, newRoute(t, nilHandler),
		nil,
		consumer.WithLogger(nil),
	)

	require.NoError(t, c.Run(ctx))
}

func TestNewRejectsNonPositiveRetryTimeout(t *testing.T) {
	tests := []struct {
		opt  consumer.Option
		name string
	}{
		{name: "zero", opt: consumer.WithRetryTimeout(0)},
		{name: "negative", opt: consumer.WithRetryTimeout(-time.Second)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := consumer.New(&stubClient{queueURL: "https://sqs/orders"}, newRoute(t, nilHandler), tt.opt)

			assert.Nil(t, c)
			assert.ErrorIs(t, err, verrors.ErrInvalidOption)
		})
	}
}

func TestRunConsumesStandardQueueRawMessagesInParallel(t *testing.T) {
	defer goleak.VerifyNone(t)

	type order struct {
		ID    string `json:"id"`
		Total int    `json:"total"`
	}

	var (
		mu       sync.Mutex
		decoded  []order
		groupIDs []string
	)
	handler := func(_ context.Context, m middleware.Message) error {
		var o order
		if err := json.Unmarshal(m.Body(), &o); err != nil {
			return err
		}
		mu.Lock()
		decoded = append(decoded, o)
		groupIDs = append(groupIDs, m.SystemAttributeByKey("MessageGroupId"))
		mu.Unlock()
		return nil
	}

	deleted := newCountdown(3)
	ctx, cancel := context.WithCancel(context.Background())
	client := &stubClient{queueURL: "https://sqs/orders", onDelete: deleted.tick}
	client.receiveFn = func(call int) (*sqs.ReceiveMessageOutput, error) {
		if call == 0 {
			return &sqs.ReceiveMessageOutput{Messages: []types.Message{
				rawMessage("receipt-1", `{"id":"a","total":10}`),
				rawMessage("receipt-2", `{"id":"b","total":20}`),
				rawMessage("receipt-3", `{"id":"c","total":30}`),
			}}, nil
		}
		<-deleted.done
		cancel()
		return &sqs.ReceiveMessageOutput{}, nil
	}

	route := newRoute(t, handler, router.WithWorkerPoolSize(3))
	assert.Equal(t, router.Parallel, route.RunMode())
	c := newConsumer(t, client, route)

	require.NoError(t, c.Run(ctx))

	assert.ElementsMatch(t, []string{"receipt-1", "receipt-2", "receipt-3"}, client.deletedHandles())
	assert.ElementsMatch(t, []order{
		{ID: "a", Total: 10},
		{ID: "b", Total: 20},
		{ID: "c", Total: 30},
	}, decoded)
	assert.Equal(t, []string{"", "", ""}, groupIDs)
}

func TestRunProcessesEveryReceivedMessageProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		defer goleak.VerifyNone(rt)

		n := rapid.IntRange(1, 10).Draw(rt, "n")
		handles := make([]string, n)
		for i := range handles {
			handles[i] = "receipt-" + strconv.Itoa(i)
		}

		rec := &recorder{}
		deleted := newCountdown(n)
		ctx, cancel := context.WithCancel(context.Background())
		client := &stubClient{queueURL: "https://sqs/orders", onDelete: deleted.tick}
		client.receiveFn = func(call int) (*sqs.ReceiveMessageOutput, error) {
			if call == 0 {
				return messageBatch(handles...), nil
			}
			<-deleted.done
			cancel()
			return &sqs.ReceiveMessageOutput{}, nil
		}

		c := newConsumer(rt, client, newRoute(rt, rec.handler()))

		require.NoError(rt, c.Run(ctx))
		assert.ElementsMatch(rt, handles, client.deletedHandles())
	})
}

func TestWithMetricsNilIgnored(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx, cancel := context.WithCancel(context.Background())
	client := &stubClient{queueURL: "https://sqs/orders"}
	client.receiveFn = func(int) (*sqs.ReceiveMessageOutput, error) {
		cancel()
		return &sqs.ReceiveMessageOutput{}, nil
	}

	c := newConsumer(t, client, newRoute(t, nilHandler), consumer.WithMetrics(nil))

	require.NoError(t, c.Run(ctx))
}

func TestRunIncrementsDLQMetricWhenExhausted(t *testing.T) {
	defer goleak.VerifyNone(t)

	reg := prometheus.NewRegistry()
	metrics := middleware.Metrics("orders-queue", middleware.WithMetricsRegisterer(reg))
	dlqCounter := dlqReuseCounter(t, reg)

	handler := func(context.Context, middleware.Message) error { return stderrors.New("boom") }

	observed := newCountdown(1)
	ctx, cancel := context.WithCancel(context.Background())
	client := &stubClient{queueURL: "https://sqs/orders"}
	client.receiveFn = func(call int) (*sqs.ReceiveMessageOutput, error) {
		if call == 0 {
			return &sqs.ReceiveMessageOutput{Messages: []types.Message{
				{
					ReceiptHandle: aws.String("receipt-exhausted"),
					Attributes:    map[string]string{"ApproximateReceiveCount": "5"},
				},
			}}, nil
		}
		<-observed.done
		cancel()
		return &sqs.ReceiveMessageOutput{}, nil
	}

	route := newRoute(t, handler,
		router.WithMiddleware(metrics),
		router.WithDLQ(5),
	)
	c := newConsumer(t, client, route,
		consumer.WithMetrics(recorderFunc{dlq: func(routeName string) {
			dlqCounter.WithLabelValues(routeName).Inc()
			observed.tick()
		}}),
	)

	require.NoError(t, c.Run(ctx))

	assert.Empty(t, client.deletedHandles())
	assert.Equal(t, float64(1), dlqCounterValue(t, reg, "orders-queue"))
}

func TestRunLeavesExhaustedMessageForRedrive(t *testing.T) {
	defer goleak.VerifyNone(t)

	var onDLQCalls int
	handler := func(context.Context, middleware.Message) error { return stderrors.New("boom") }

	observed := newCountdown(1)
	ctx, cancel := context.WithCancel(context.Background())
	client := &stubClient{queueURL: "https://sqs/orders"}
	client.receiveFn = func(call int) (*sqs.ReceiveMessageOutput, error) {
		if call == 0 {
			return &sqs.ReceiveMessageOutput{Messages: []types.Message{
				{
					ReceiptHandle: aws.String("receipt-exhausted"),
					Attributes:    map[string]string{"ApproximateReceiveCount": "7"},
				},
			}}, nil
		}
		<-observed.done
		cancel()
		return &sqs.ReceiveMessageOutput{}, nil
	}

	route := newRoute(t, handler, router.WithDLQ(5, router.WithOnDLQ(func(context.Context, middleware.Message) {
		onDLQCalls++
		observed.tick()
	})))
	c := newConsumer(t, client, route)

	require.NoError(t, c.Run(ctx))

	assert.Empty(t, client.deletedHandles())
	assert.Equal(t, 1, onDLQCalls)
}

func TestNewVisibilityModelRequiresNoSchedulerOrDLQConfig(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx, cancel := context.WithCancel(context.Background())
	client := &stubClient{queueURL: "https://sqs/orders"}
	client.receiveFn = func(int) (*sqs.ReceiveMessageOutput, error) {
		cancel()
		return &sqs.ReceiveMessageOutput{}, nil
	}

	route := newRoute(t, nilHandler)
	assert.Equal(t, router.VisibilityRetryModel, route.RetryModel())
	assert.Nil(t, route.ScheduledRetry())

	c := newConsumer(t, client, route)

	require.NoError(t, c.Run(ctx))
	assert.Empty(t, client.visibilityChanges())
	assert.Empty(t, client.deletedHandles())
}

func TestRunVisibilityModelBackoffExtendsVisibilityAndLeavesMessage(t *testing.T) {
	defer goleak.VerifyNone(t)

	client := &stubClient{queueURL: "https://sqs/orders"}
	processed := make(chan struct{})

	handler := func(_ context.Context, m middleware.Message) error {
		m.(consumer.Message).Backoff(45 * time.Second)
		deadline := time.Now().Add(2 * time.Second)
		for len(client.visibilityChanges()) == 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		close(processed)
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	client.receiveFn = func(call int) (*sqs.ReceiveMessageOutput, error) {
		if call == 0 {
			return messageBatch("receipt-backoff"), nil
		}
		<-processed
		cancel()
		return &sqs.ReceiveMessageOutput{}, nil
	}

	c := newConsumer(t, client, newRoute(t, handler))

	require.NoError(t, c.Run(ctx))
	assert.Equal(t, []int32{45}, client.visibilityChanges())
	assert.Empty(t, client.deletedHandles())
}

func TestRunReturnsErrorWhenScheduledModelMissingSchedulerClient(t *testing.T) {
	defer goleak.VerifyNone(t)

	client := &stubClient{queueURL: "https://sqs/orders"}
	c := newConsumer(t, client, scheduledRoute(t, nilHandler))

	err := c.Run(context.Background())

	require.Error(t, err)
	assert.True(t, stderrors.Is(err, verrors.ErrNoSchedulerClient))
	assert.Equal(t, 0, client.resolveCalls())
	assert.Equal(t, 0, client.receives())
}

var errBackoffHandler = stderrors.New("backoff handler failed")

type backoffScript struct {
	delays []time.Duration
	fail   bool
}

type backoffScenario struct {
	scripts    []backoffScript
	retryModel router.RetryModel
	batch      bool
}

type countdown struct {
	done      chan struct{}
	remaining atomic.Int64
}

func newCountdown(n int) *countdown {
	c := &countdown{done: make(chan struct{})}
	c.remaining.Store(int64(n))
	if n == 0 {
		close(c.done)
	}
	return c
}

func (c *countdown) tick() {
	if c.remaining.Add(-1) == 0 {
		close(c.done)
	}
}

func drawBackoffDelay(t *rapid.T, label string) time.Duration {
	ranges := [][2]time.Duration{
		{-48 * time.Hour, -time.Nanosecond},
		{0, 0},
		{time.Nanosecond, time.Second - time.Nanosecond},
		{time.Second, 12 * time.Hour},
		{12*time.Hour + time.Nanosecond, 30 * 24 * time.Hour},
	}
	r := rapid.SampledFrom(ranges).Draw(t, label+"-range")
	return time.Duration(rapid.Int64Range(int64(r[0]), int64(r[1])).Draw(t, label))
}

func drawBackoffScenario(t *rapid.T) backoffScenario {
	s := backoffScenario{
		retryModel: rapid.SampledFrom([]router.RetryModel{router.VisibilityRetryModel, router.ScheduledRetryModel}).Draw(t, "retryModel"),
		batch:      rapid.Bool().Draw(t, "batch"),
	}
	n := rapid.IntRange(1, 10).Draw(t, "messages")
	s.scripts = make([]backoffScript, n)
	for i := range s.scripts {
		calls := rapid.IntRange(1, 20).Draw(t, "calls")
		delays := make([]time.Duration, calls)
		for j := range delays {
			delays[j] = drawBackoffDelay(t, "delay")
		}
		s.scripts[i] = backoffScript{delays: delays, fail: rapid.Bool().Draw(t, "fail")}
	}
	return s
}

func backoffHandle(i int) string {
	return "receipt-backoff-" + strconv.Itoa(i)
}

func expectedBackoffTimeout(delay time.Duration) int32 {
	return int32(min(max(int64(delay/time.Second), 0), 43200))
}

func (s backoffScenario) handler(handlers *countdown, notBackedOff *atomic.Int64) middleware.Handler {
	scripts := make(map[string]backoffScript, len(s.scripts))
	for i, script := range s.scripts {
		scripts[backoffHandle(i)] = script
	}
	return func(_ context.Context, m middleware.Message) error {
		defer handlers.tick()
		msg, ok := m.(consumer.Message)
		if !ok {
			notBackedOff.Add(1)
			return errBackoffHandler
		}
		script := scripts[msg.Identifier()]
		for _, d := range script.delays {
			msg.Backoff(d)
		}
		if !msg.BackedOff() {
			notBackedOff.Add(1)
		}
		if script.fail {
			return errBackoffHandler
		}
		return nil
	}
}

func (s backoffScenario) route(tb require.TestingT, handler middleware.Handler) *router.Route {
	opts := []router.Option{router.WithExtensionLimit(0)}
	if s.retryModel == router.ScheduledRetryModel {
		opts = append(opts, router.WithScheduledRetry(
			router.WithSchedulerIdentity("arn:aws:sqs:us-east-1:123456789012:orders.fifo", "arn:aws:iam::123456789012:role/scheduler"),
			router.WithScheduledDLQ("https://sqs.us-east-1.amazonaws.com/123456789012/orders-dlq"),
			router.WithMaxRetryCount(5),
			router.WithBackoff(time.Second, 2*time.Second),
		))
	}
	return newRoute(tb, handler, opts...)
}

func runBackoffScenario(tb require.TestingT, s backoffScenario) (*fake.SQSClient, *fake.SchedulerClient) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	n := len(s.scripts)
	handlers := newCountdown(n)
	deletes := newCountdown(0)
	if s.retryModel == router.ScheduledRetryModel {
		deletes = newCountdown(n)
	}

	msgs := make([]types.Message, n)
	for i := range msgs {
		msgs[i] = types.Message{ReceiptHandle: aws.String(backoffHandle(i)), Body: aws.String("body-" + strconv.Itoa(i))}
	}

	var receives atomic.Int64
	client := &fake.SQSClient{
		GetQueueUrlFunc: func(_ context.Context, _ *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
			return &sqs.GetQueueUrlOutput{QueueUrl: aws.String(commitQueueURL)}, nil
		},
		ReceiveMessageFunc: func(_ context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			if receives.Add(1) == 1 {
				return &sqs.ReceiveMessageOutput{Messages: msgs}, nil
			}
			<-handlers.done
			<-deletes.done
			cancel()
			return &sqs.ReceiveMessageOutput{}, nil
		},
		DeleteMessageFunc: func(_ context.Context, _ *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
			deletes.tick()
			return &sqs.DeleteMessageOutput{}, nil
		},
	}
	sched := &fake.SchedulerClient{
		CreateScheduleFunc: func(_ context.Context, _ *scheduler.CreateScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.CreateScheduleOutput, error) {
			return &scheduler.CreateScheduleOutput{}, nil
		},
	}

	var sqsClient consumer.SQSClient = client
	if !s.batch {
		sqsClient = standardClient(client)
	}

	var notBackedOff atomic.Int64
	c := newConsumer(tb, sqsClient, s.route(tb, s.handler(handlers, &notBackedOff)), consumer.WithSchedulerClient(sched))
	require.NoError(tb, c.Run(ctx))
	require.Zero(tb, notBackedOff.Load())
	return client, sched
}

func visibilityChangesByHandle(client *fake.SQSClient) map[string][]int32 {
	out := make(map[string][]int32)
	for _, call := range client.ChangeMessageVisibilityBatchCalls() {
		for _, entry := range call.Entries {
			handle := aws.ToString(entry.ReceiptHandle)
			out[handle] = append(out[handle], entry.VisibilityTimeout)
		}
	}
	for _, call := range client.ChangeMessageVisibilityCalls() {
		handle := aws.ToString(call.ReceiptHandle)
		out[handle] = append(out[handle], call.VisibilityTimeout)
	}
	return out
}

func deletedHandleCounts(client *fake.SQSClient) map[string]int {
	out := make(map[string]int)
	for _, call := range client.DeleteMessageCalls() {
		out[aws.ToString(call.ReceiptHandle)]++
	}
	return out
}

// Feature: consumer-flow-control, Property 18: For any sequence of Backoff calls in a handler, every call returns without blocking; under the Visibility_Retry_Model exactly one visibility change is sent, with timeout clamp(last delay seconds, 0, 43200), and the message is not deleted; under the Scheduled_Retry_Model no visibility change is sent.
func TestRun_PropertyBackoffSemantics(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		defer goleak.VerifyNone(rt)

		s := drawBackoffScenario(rt)
		client, sched := runBackoffScenario(rt, s)

		changes := visibilityChangesByHandle(client)
		deleted := deletedHandleCounts(client)
		assert.Empty(rt, client.DeleteMessageBatchCalls())

		if s.retryModel == router.ScheduledRetryModel {
			assert.Empty(rt, changes)
			assert.Len(rt, sched.CreateScheduleCalls(), len(s.scripts))
			for i := range s.scripts {
				assert.Equal(rt, 1, deleted[backoffHandle(i)], backoffHandle(i))
			}
			return
		}

		assert.Empty(rt, deleted)
		assert.Empty(rt, sched.CreateScheduleCalls())
		assert.Len(rt, changes, len(s.scripts))
		for i, script := range s.scripts {
			want := expectedBackoffTimeout(script.delays[len(script.delays)-1])
			assert.Equal(rt, []int32{want}, changes[backoffHandle(i)], backoffHandle(i))
		}
		if s.batch {
			assert.Empty(rt, client.ChangeMessageVisibilityCalls())
		} else {
			assert.Empty(rt, client.ChangeMessageVisibilityBatchCalls())
		}
	})
}

func TestRun_BackoffThousandCallsReturns(t *testing.T) {
	defer goleak.VerifyNone(t)

	delays := make([]time.Duration, 1000)
	for i := range delays {
		delays[i] = time.Duration(i) * time.Second
	}
	s := backoffScenario{
		scripts:    []backoffScript{{delays: delays}},
		retryModel: router.VisibilityRetryModel,
		batch:      true,
	}

	client, _ := runBackoffScenario(t, s)

	assert.Equal(t, map[string][]int32{backoffHandle(0): {999}}, visibilityChangesByHandle(client))
	assert.Empty(t, client.DeleteMessageCalls())
	assert.Empty(t, client.ChangeMessageVisibilityCalls())
}

func TestRun_PerGroupIDHoldsSameReceiveTailAfterFailure(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		defer goleak.VerifyNone(rt)

		n := rapid.IntRange(2, 10).Draw(rt, "messages")
		workers := rapid.IntRange(1, 4).Draw(rt, "workers")
		batch := make([]types.Message, n)
		for i := range batch {
			batch[i] = types.Message{
				ReceiptHandle: aws.String("receipt-" + strconv.Itoa(i)),
				Body:          aws.String(strconv.Itoa(i)),
				Attributes:    map[string]string{"MessageGroupId": "group"},
			}
		}

		var handled atomic.Int64
		handler := func(_ context.Context, m middleware.Message) error {
			handled.Add(1)
			if string(m.Body()) == "0" {
				return errCommitHandler
			}
			return nil
		}

		client := &fake.SQSClient{
			GetQueueUrlFunc: func(context.Context, *sqs.GetQueueUrlInput, ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
				return &sqs.GetQueueUrlOutput{QueueUrl: aws.String(commitQueueURL)}, nil
			},
		}
		route := newRoute(rt, handler, router.WithRunMode(router.PerGroupID), router.WithWorkerPoolSize(workers), router.WithMaxMessages(10))
		c := newConsumer(rt, standardClient(client), route)

		var runErr error
		synctest.Test(t, func(*testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			runErr = runQuiescentBatches(ctx, c, client, [][]types.Message{batch}, cancel)
		})

		require.NoError(rt, runErr)
		assert.Equal(rt, int64(1), handled.Load())
		assert.Empty(rt, deleteMessageHandles(client.DeleteMessageCalls()))
	})
}

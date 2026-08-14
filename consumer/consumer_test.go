package consumer_test

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
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
	"github.com/silviolleite/loafer-awsx/middleware"
	"github.com/silviolleite/loafer-awsx/router"
)

type stubClient struct {
	getErr        error
	receiveFn     func(call int) (*sqs.ReceiveMessageOutput, error)
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
	defer s.mu.Unlock()
	if in.ReceiptHandle != nil {
		s.deleted = append(s.deleted, *in.ReceiptHandle)
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
	delay   time.Duration
	mu      sync.Mutex
}

func (r *recorder) handler() middleware.Handler {
	return func(_ context.Context, m middleware.Message) error {
		if r.delay > 0 {
			time.Sleep(r.delay)
		}
		r.mu.Lock()
		r.handled = append(r.handled, m.Identifier())
		r.mu.Unlock()
		return nil
	}
}

func (r *recorder) handledCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.handled)
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
	ctx, cancel := context.WithCancel(context.Background())
	client := &stubClient{queueURL: "https://sqs/orders"}
	client.receiveFn = func(call int) (*sqs.ReceiveMessageOutput, error) {
		switch call {
		case 0:
			return nil, stderrors.New("throttled")
		case 1:
			return messageBatch("receipt-1"), nil
		default:
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

	rec := &recorder{delay: 10 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	client := &stubClient{queueURL: "https://sqs/orders"}
	client.receiveFn = func(call int) (*sqs.ReceiveMessageOutput, error) {
		if call == 0 {
			return messageBatch("receipt-1", "receipt-2", "receipt-3"), nil
		}
		cancel()
		return &sqs.ReceiveMessageOutput{}, nil
	}

	c := newConsumer(t, client, newRoute(t, rec.handler()))

	require.NoError(t, c.Run(ctx))
	assert.Equal(t, 3, rec.handledCount())
	assert.ElementsMatch(t, []string{"receipt-1", "receipt-2", "receipt-3"}, client.deletedHandles())
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

	ctx, cancel := context.WithCancel(context.Background())
	client := &stubClient{queueURL: "https://sqs/orders"}
	client.receiveFn = func(call int) (*sqs.ReceiveMessageOutput, error) {
		if call == 0 {
			return messageBatch("receipt-1"), nil
		}
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

	ctx, cancel := context.WithCancel(context.Background())
	client := &stubClient{queueURL: "https://sqs/orders"}
	client.receiveFn = func(call int) (*sqs.ReceiveMessageOutput, error) {
		if call == 0 {
			return &sqs.ReceiveMessageOutput{Messages: []types.Message{
				rawMessage("receipt-1", `{"id":"a","total":10}`),
				rawMessage("receipt-2", `{"id":"b","total":20}`),
				rawMessage("receipt-3", `{"id":"c","total":30}`),
			}}, nil
		}
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
		ctx, cancel := context.WithCancel(context.Background())
		client := &stubClient{queueURL: "https://sqs/orders"}
		client.receiveFn = func(call int) (*sqs.ReceiveMessageOutput, error) {
			if call == 0 {
				return messageBatch(handles...), nil
			}
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
		cancel()
		return &sqs.ReceiveMessageOutput{}, nil
	}

	route := newRoute(t, handler,
		router.WithMiddleware(metrics),
		router.WithDLQ(5),
	)
	c := newConsumer(t, client, route,
		consumer.WithMetrics(recorderFunc{dlq: func(routeName string) { dlqCounter.WithLabelValues(routeName).Inc() }}),
	)

	require.NoError(t, c.Run(ctx))

	assert.Empty(t, client.deletedHandles())
	assert.Equal(t, float64(1), dlqCounterValue(t, reg, "orders-queue"))
}

func TestRunLeavesExhaustedMessageForRedrive(t *testing.T) {
	defer goleak.VerifyNone(t)

	var onDLQCalls int
	handler := func(context.Context, middleware.Message) error { return stderrors.New("boom") }

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
		cancel()
		return &sqs.ReceiveMessageOutput{}, nil
	}

	route := newRoute(t, handler, router.WithDLQ(5, router.WithOnDLQ(func(context.Context, middleware.Message) { onDLQCalls++ })))
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

package broker_test

import (
	"context"
	stderrors "errors"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/broker"
	verrors "github.com/silviolleite/loafer-awsx/errors"
	"github.com/silviolleite/loafer-awsx/middleware"
	"github.com/silviolleite/loafer-awsx/router"
)

type fakeClient struct {
	resolveErr   map[string]error
	getCalls     map[string]int
	receiveCalls map[string]int
	receiveFn    func(ctx context.Context, queueURL string, call int) (*sqs.ReceiveMessageOutput, error)
	deleted      []string
	mu           sync.Mutex
	finished     int32
}

func newFakeClient() *fakeClient {
	return &fakeClient{
		resolveErr:   map[string]error{},
		getCalls:     map[string]int{},
		receiveCalls: map[string]int{},
	}
}

func (f *fakeClient) GetQueueUrl(_ context.Context, in *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := aws.ToString(in.QueueName)
	f.getCalls[name]++
	if err := f.resolveErr[name]; err != nil {
		return nil, err
	}
	return &sqs.GetQueueUrlOutput{QueueUrl: aws.String("https://sqs/" + name)}, nil
}

func (f *fakeClient) ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	url := aws.ToString(in.QueueUrl)
	f.mu.Lock()
	call := f.receiveCalls[url]
	f.receiveCalls[url]++
	fn := f.receiveFn
	f.mu.Unlock()

	if fn != nil {
		return fn(ctx, url, call)
	}

	<-ctx.Done()
	atomic.AddInt32(&f.finished, 1)
	return &sqs.ReceiveMessageOutput{}, ctx.Err()
}

func (f *fakeClient) DeleteMessage(_ context.Context, in *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if in.ReceiptHandle != nil {
		f.deleted = append(f.deleted, *in.ReceiptHandle)
	}
	return &sqs.DeleteMessageOutput{}, nil
}

func (f *fakeClient) ChangeMessageVisibility(_ context.Context, _ *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *fakeClient) SendMessage(_ context.Context, _ *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	return &sqs.SendMessageOutput{}, nil
}

func (f *fakeClient) resolveCalls(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.getCalls[name]
}

func (f *fakeClient) totalReceives() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	var n int
	for _, c := range f.receiveCalls {
		n += c
	}
	return n
}

func (f *fakeClient) finishedCount() int32 {
	return atomic.LoadInt32(&f.finished)
}

type capturingHandler struct {
	records []slog.Record
	mu      sync.Mutex
}

func (h *capturingHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *capturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *capturingHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }

func (h *capturingHandler) WithGroup(_ string) slog.Handler { return h }

func (h *capturingHandler) errorCount() int {
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

func noopLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func nilHandler(_ context.Context, _ middleware.Message) error { return nil }

func newRoute(tb require.TestingT, name string, handler middleware.Handler, opts ...router.Option) *router.Route {
	route, err := router.New(name, handler, opts...)
	require.NoError(tb, err)
	return route
}

func batchOnce(handles ...string) func(ctx context.Context, _ string, call int) (*sqs.ReceiveMessageOutput, error) {
	msgs := make([]types.Message, len(handles))
	for i, h := range handles {
		msgs[i] = types.Message{ReceiptHandle: aws.String(h)}
	}
	return func(ctx context.Context, _ string, call int) (*sqs.ReceiveMessageOutput, error) {
		if call == 0 {
			return &sqs.ReceiveMessageOutput{Messages: msgs}, nil
		}
		<-ctx.Done()
		return &sqs.ReceiveMessageOutput{}, ctx.Err()
	}
}

func runAsync(ctx context.Context, b *broker.Broker) <-chan error {
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	return done
}

func TestNewReturnsErrNoRouteWhenRoutesEmpty(t *testing.T) {
	tests := []struct {
		name   string
		routes []*router.Route
	}{
		{name: "nil routes", routes: nil},
		{name: "empty routes", routes: []*router.Route{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, err := broker.New(newFakeClient(), tc.routes)

			require.Error(t, err)
			assert.True(t, stderrors.Is(err, verrors.ErrNoRoute))
			assert.Nil(t, b)
		})
	}
}

func TestNewReturnsErrNoSQSClientWhenClientNil(t *testing.T) {
	b, err := broker.New(nil, []*router.Route{newRoute(t, "orders", nilHandler)})

	require.Error(t, err)
	assert.True(t, stderrors.Is(err, verrors.ErrNoSQSClient))
	assert.Nil(t, b)
}

func TestNewSucceedsWithRoutes(t *testing.T) {
	b, err := broker.New(newFakeClient(), []*router.Route{newRoute(t, "orders", nilHandler)})

	require.NoError(t, err)
	require.NotNil(t, b)
}

func TestRunStartsOneConsumerPerRoute(t *testing.T) {
	defer goleak.VerifyNone(t)

	client := newFakeClient()
	routes := []*router.Route{
		newRoute(t, "orders", nilHandler),
		newRoute(t, "payments", nilHandler),
		newRoute(t, "shipments", nilHandler),
	}

	b, err := broker.New(client, routes, broker.WithLogger(noopLogger()))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := runAsync(ctx, b)

	require.Eventually(t, func() bool { return client.totalReceives() >= len(routes) }, time.Second, time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}

	assert.Equal(t, 1, client.resolveCalls("orders"))
	assert.Equal(t, 1, client.resolveCalls("payments"))
	assert.Equal(t, 1, client.resolveCalls("shipments"))
}

func TestRunReturnsNilOnCleanShutdown(t *testing.T) {
	defer goleak.VerifyNone(t)

	client := newFakeClient()
	b, err := broker.New(client, []*router.Route{newRoute(t, "orders", nilHandler)}, broker.WithLogger(noopLogger()))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := runAsync(ctx, b)

	require.Eventually(t, func() bool { return client.totalReceives() >= 1 }, time.Second, time.Millisecond)
	cancel()

	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

func TestRunFailsFastOnQueueResolveError(t *testing.T) {
	defer goleak.VerifyNone(t)

	logs := &capturingHandler{}
	client := newFakeClient()
	client.resolveErr["payments"] = stderrors.New("access denied")

	routes := []*router.Route{
		newRoute(t, "orders", nilHandler),
		newRoute(t, "payments", nilHandler),
	}

	b, err := broker.New(client, routes, broker.WithLogger(slog.New(logs)))
	require.NoError(t, err)

	err = b.Run(context.Background())

	require.Error(t, err)
	assert.True(t, stderrors.Is(err, verrors.ErrQueueResolve))
	assert.GreaterOrEqual(t, logs.errorCount(), 1)
}

func TestRunFailsFastWhenConsumerCreationFails(t *testing.T) {
	defer goleak.VerifyNone(t)

	logs := &capturingHandler{}
	client := newFakeClient()

	routes := []*router.Route{
		newRoute(t, "orders", nilHandler),
		nil,
	}

	b, err := broker.New(client, routes, broker.WithLogger(slog.New(logs)))
	require.NoError(t, err)

	err = b.Run(context.Background())

	require.Error(t, err)
	assert.True(t, stderrors.Is(err, verrors.ErrNoRoute))
	assert.GreaterOrEqual(t, logs.errorCount(), 1)
}

func TestRunAppliesGlobalMiddleware(t *testing.T) {
	defer goleak.VerifyNone(t)

	var invoked int32
	mw := func(next middleware.Handler) middleware.Handler {
		return func(ctx context.Context, msg middleware.Message) error {
			atomic.AddInt32(&invoked, 1)
			return next(ctx, msg)
		}
	}

	client := newFakeClient()
	client.receiveFn = batchOnce("receipt-1")

	b, err := broker.New(client,
		[]*router.Route{newRoute(t, "orders", nilHandler)},
		broker.WithLogger(noopLogger()),
		broker.WithGlobalMiddleware(mw),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := runAsync(ctx, b)

	require.Eventually(t, func() bool { return atomic.LoadInt32(&invoked) >= 1 }, time.Second, time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}

	assert.GreaterOrEqual(t, atomic.LoadInt32(&invoked), int32(1))
}

func TestNewIgnoresNilOptionAndNilLogger(t *testing.T) {
	defer goleak.VerifyNone(t)

	client := newFakeClient()
	b, err := broker.New(client,
		[]*router.Route{newRoute(t, "orders", nilHandler)},
		nil,
		broker.WithLogger(nil),
		broker.WithLogger(noopLogger()),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := runAsync(ctx, b)

	require.Eventually(t, func() bool { return client.totalReceives() >= 1 }, time.Second, time.Millisecond)
	cancel()

	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

func TestNewRejectsNonPositiveDurations(t *testing.T) {
	tests := []struct {
		opt  broker.Option
		name string
	}{
		{name: "zero retry timeout", opt: broker.WithRetryTimeout(0)},
		{name: "negative retry timeout", opt: broker.WithRetryTimeout(-time.Second)},
		{name: "zero shutdown timeout", opt: broker.WithShutdownTimeout(0)},
		{name: "negative shutdown timeout", opt: broker.WithShutdownTimeout(-time.Second)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := broker.New(newFakeClient(),
				[]*router.Route{newRoute(t, "orders", nilHandler)},
				tt.opt,
			)

			assert.Nil(t, b)
			assert.ErrorIs(t, err, verrors.ErrInvalidOption)
		})
	}
}

func TestRunHonorsCustomRetryAndShutdownTimeouts(t *testing.T) {
	defer goleak.VerifyNone(t)

	client := newFakeClient()
	b, err := broker.New(client,
		[]*router.Route{newRoute(t, "orders", nilHandler)},
		broker.WithLogger(noopLogger()),
		broker.WithRetryTimeout(10*time.Millisecond),
		broker.WithShutdownTimeout(2*time.Second),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := runAsync(ctx, b)

	require.Eventually(t, func() bool { return client.totalReceives() >= 1 }, time.Second, time.Millisecond)
	cancel()

	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

func TestRunReturnsWhenShutdownTimeoutExceeded(t *testing.T) {
	logs := &capturingHandler{}
	client := newFakeClient()
	client.receiveFn = func(ctx context.Context, _ string, _ int) (*sqs.ReceiveMessageOutput, error) {
		<-ctx.Done()
		time.Sleep(80 * time.Millisecond)
		atomic.AddInt32(&client.finished, 1)
		return &sqs.ReceiveMessageOutput{}, ctx.Err()
	}

	b, err := broker.New(client,
		[]*router.Route{newRoute(t, "orders", nilHandler)},
		broker.WithLogger(slog.New(logs)),
		broker.WithShutdownTimeout(time.Millisecond),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := runAsync(ctx, b)

	require.Eventually(t, func() bool { return client.totalReceives() >= 1 }, time.Second, time.Millisecond)
	cancel()

	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after shutdown timeout")
	}

	assert.GreaterOrEqual(t, logs.errorCount(), 1)
	require.Eventually(t, func() bool { return client.finishedCount() >= 1 }, time.Second, time.Millisecond)
}

func TestRunWaitsIndefinitelyForSlowConsumerByDefault(t *testing.T) {
	defer goleak.VerifyNone(t)

	logs := &capturingHandler{}
	client := newFakeClient()
	const drainDelay = 200 * time.Millisecond
	client.receiveFn = func(ctx context.Context, _ string, _ int) (*sqs.ReceiveMessageOutput, error) {
		<-ctx.Done()
		time.Sleep(drainDelay)
		atomic.AddInt32(&client.finished, 1)
		return &sqs.ReceiveMessageOutput{}, ctx.Err()
	}

	b, err := broker.New(client,
		[]*router.Route{newRoute(t, "orders", nilHandler)},
		broker.WithLogger(slog.New(logs)),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := runAsync(ctx, b)

	require.Eventually(t, func() bool { return client.totalReceives() >= 1 }, time.Second, time.Millisecond)
	cancel()

	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after slow consumer drained")
	}

	assert.Equal(t, int32(1), client.finishedCount())
	assert.Equal(t, 0, logs.errorCount())
}

func TestRunStartsExactlyOneConsumerPerRouteProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		defer goleak.VerifyNone(rt)

		n := rapid.IntRange(1, 5).Draw(rt, "n")
		routes := make([]*router.Route, n)
		names := make([]string, n)
		for i := range routes {
			names[i] = "queue-" + strconv.Itoa(i)
			routes[i] = newRoute(rt, names[i], nilHandler)
		}

		client := newFakeClient()
		b, err := broker.New(client, routes, broker.WithLogger(noopLogger()))
		require.NoError(rt, err)

		ctx, cancel := context.WithCancel(context.Background())
		done := runAsync(ctx, b)

		require.Eventually(rt, func() bool { return client.totalReceives() >= n }, time.Second, time.Millisecond)
		cancel()

		select {
		case err := <-done:
			require.NoError(rt, err)
		case <-time.After(5 * time.Second):
			rt.Fatal("Run did not return after context cancellation")
		}

		for _, name := range names {
			assert.Equal(rt, 1, client.resolveCalls(name))
		}
	})
}

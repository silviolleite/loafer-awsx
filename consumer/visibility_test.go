package consumer

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/silviolleite/loafer-awsx/logger"
)

type fakeSQSClient struct {
	onChangeVisibility func()
	changeVisErr       error
	deleteErr          error
	visibilityCalls    []int32
	receiptHandles     []string
	deletedHandles     []string
	mu                 sync.Mutex
}

func (f *fakeSQSClient) ReceiveMessage(_ context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	return &sqs.ReceiveMessageOutput{}, nil
}

func (f *fakeSQSClient) DeleteMessage(_ context.Context, params *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	f.mu.Lock()
	if params.ReceiptHandle != nil {
		f.deletedHandles = append(f.deletedHandles, *params.ReceiptHandle)
	}
	err := f.deleteErr
	f.mu.Unlock()

	if err != nil {
		return nil, err
	}
	return &sqs.DeleteMessageOutput{}, nil
}

func (f *fakeSQSClient) ChangeMessageVisibility(_ context.Context, params *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	f.mu.Lock()
	f.visibilityCalls = append(f.visibilityCalls, params.VisibilityTimeout)
	if params.ReceiptHandle != nil {
		f.receiptHandles = append(f.receiptHandles, *params.ReceiptHandle)
	}
	cb := f.onChangeVisibility
	err := f.changeVisErr
	f.mu.Unlock()

	if cb != nil {
		cb()
	}
	if err != nil {
		return nil, err
	}
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *fakeSQSClient) GetQueueUrl(_ context.Context, _ *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	return &sqs.GetQueueUrlOutput{}, nil
}

func (f *fakeSQSClient) SendMessage(_ context.Context, _ *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	return &sqs.SendMessageOutput{}, nil
}

func (f *fakeSQSClient) calls() []int32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]int32, len(f.visibilityCalls))
	copy(out, f.visibilityCalls)
	return out
}

func (f *fakeSQSClient) handles() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.receiptHandles))
	copy(out, f.receiptHandles)
	return out
}

func (f *fakeSQSClient) deletes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.deletedHandles))
	copy(out, f.deletedHandles)
	return out
}

type captureHandler struct {
	records []slog.Record
	mu      sync.Mutex
}

func (h *captureHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *captureHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }

func (h *captureHandler) WithGroup(_ string) slog.Handler { return h }

func (h *captureHandler) errorCount() int {
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

func (h *captureHandler) errorAttrs() []map[string]string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []map[string]string
	for _, r := range h.records {
		if r.Level != slog.LevelError {
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

func testMessage(receipt string) *message {
	return newMessage(types.Message{ReceiptHandle: aws.String(receipt)})
}

func runManager(v *visibilityManager, ctx context.Context, msg *message) chan struct{} {
	done := make(chan struct{})
	go func() {
		v.run(ctx, msg)
		close(done)
	}()
	return done
}

func waitDone(t *testing.T, done chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("visibility manager did not stop")
	}
}

func TestVisibilityManagerExtensionSequence(t *testing.T) {
	tests := []struct {
		name              string
		want              []int32
		visibilityTimeout int32
		extensionLimit    int
	}{
		{
			name:              "first sets route value subsequent increment",
			visibilityTimeout: 11,
			extensionLimit:    2,
			want:              []int32{11, 22, 33},
		},
		{
			name:              "zero extension limit renews once",
			visibilityTimeout: 30,
			extensionLimit:    0,
			want:              []int32{30},
		},
		{
			name:              "single extension",
			visibilityTimeout: 20,
			extensionLimit:    1,
			want:              []int32{20, 40},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &fakeSQSClient{}
			v := newVisibilityManager(client, "queue-url", tt.visibilityTimeout, tt.extensionLimit, logger.NewNoOp())
			v.sleepInterval = time.Millisecond

			msg := testMessage("receipt-1")
			v.run(context.Background(), msg)

			assert.Equal(t, tt.want, client.calls())
		})
	}
}

func TestVisibilityManagerReceiptHandle(t *testing.T) {
	client := &fakeSQSClient{}
	v := newVisibilityManager(client, "queue-url", 11, 0, logger.NewNoOp())
	v.sleepInterval = time.Millisecond

	v.run(context.Background(), testMessage("receipt-xyz"))

	assert.Equal(t, []string{"receipt-xyz"}, client.handles())
}

func TestVisibilityManagerClampsMaximum(t *testing.T) {
	client := &fakeSQSClient{}
	v := newVisibilityManager(client, "queue-url", 30000, 2, logger.NewNoOp())
	v.sleepInterval = time.Millisecond

	v.run(context.Background(), testMessage("receipt-1"))

	assert.Equal(t, []int32{30000, 43200, 43200}, client.calls())
}

func TestVisibilityManagerDispatchStops(t *testing.T) {
	client := &fakeSQSClient{}
	v := newVisibilityManager(client, "queue-url", 3600, 5, logger.NewNoOp())

	msg := testMessage("receipt-1")
	msg.Dispatch()

	done := runManager(v, context.Background(), msg)
	waitDone(t, done)

	assert.Empty(t, client.calls())
}

func TestVisibilityManagerBackoffStops(t *testing.T) {
	tests := []struct {
		name    string
		backoff time.Duration
		want    int32
	}{
		{name: "positive backoff", backoff: 45 * time.Second, want: 45},
		{name: "negative backoff clamped to minimum", backoff: -5 * time.Second, want: 0},
		{name: "excessive backoff clamped to maximum", backoff: 13 * time.Hour, want: 43200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &fakeSQSClient{}
			v := newVisibilityManager(client, "queue-url", 3600, 5, logger.NewNoOp())

			msg := testMessage("receipt-1")
			msg.Backoff(tt.backoff)

			done := runManager(v, context.Background(), msg)
			waitDone(t, done)

			assert.Equal(t, []int32{tt.want}, client.calls())
		})
	}
}

func TestVisibilityManagerScheduledIgnoresBackoff(t *testing.T) {
	tests := []struct {
		name string
		stop func(msg *message, cancel context.CancelFunc)
	}{
		{
			name: "returns on dispatch",
			stop: func(msg *message, _ context.CancelFunc) { msg.Dispatch() },
		},
		{
			name: "returns on context cancellation",
			stop: func(_ *message, cancel context.CancelFunc) { cancel() },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &fakeSQSClient{}
			v := newScheduledVisibilityManager(client, "queue-url", 3600, 5, logger.NewNoOp())
			v.sleepInterval = time.Hour

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			msg := testMessage("receipt-1")
			msg.Backoff(45 * time.Second)
			tt.stop(msg, cancel)

			done := runManager(v, ctx, msg)
			waitDone(t, done)

			assert.Empty(t, client.calls())
			assert.True(t, msg.BackedOff())
		})
	}
}

func TestVisibilityManagerContextCancellationStops(t *testing.T) {
	client := &fakeSQSClient{}
	v := newVisibilityManager(client, "queue-url", 3600, 5, logger.NewNoOp())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := runManager(v, ctx, testMessage("receipt-1"))
	waitDone(t, done)

	assert.Empty(t, client.calls())
}

func TestVisibilityManagerChangeVisibilityErrorIsLogged(t *testing.T) {
	handler := &captureHandler{}
	client := &fakeSQSClient{changeVisErr: errors.New("api failure")}
	v := newVisibilityManager(client, "queue-url", 11, 2, slog.New(handler))
	v.sleepInterval = time.Millisecond

	v.run(context.Background(), testMessage("receipt-1"))

	assert.Equal(t, []int32{11, 22, 33}, client.calls())
	assert.Equal(t, 3, handler.errorCount())
}

func TestVisibilityManagerNilLoggerDefaultsToNoOp(t *testing.T) {
	client := &fakeSQSClient{}
	v := newVisibilityManager(client, "queue-url", 11, 0, nil)
	v.sleepInterval = time.Millisecond

	require.NotNil(t, v.log)
	assert.NotPanics(t, func() {
		v.run(context.Background(), testMessage("receipt-1"))
	})
	assert.Equal(t, []int32{11}, client.calls())
}

func TestVisibilityManagerIntervalDefault(t *testing.T) {
	tests := []struct {
		name              string
		visibilityTimeout int32
		injected          time.Duration
		want              time.Duration
	}{
		{name: "derived from visibility timeout", visibilityTimeout: 30, want: 20 * time.Second},
		{name: "guarded when below margin", visibilityTimeout: 5, want: time.Second},
		{name: "injected interval takes precedence", visibilityTimeout: 30, injected: 5 * time.Millisecond, want: 5 * time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := newVisibilityManager(&fakeSQSClient{}, "queue-url", tt.visibilityTimeout, 0, logger.NewNoOp())
			v.sleepInterval = tt.injected
			assert.Equal(t, tt.want, v.interval())
		})
	}
}

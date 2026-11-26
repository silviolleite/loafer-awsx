package consumer

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/require"
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

func runToCompletion(t *testing.T, d *dispatcher, msgs []*message) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	d.visibility.start(context.WithoutCancel(ctx))
	for _, m := range msgs {
		require.NoError(t, d.credits.acquire(ctx, 1))
		d.admit(m)
	}
	d.startWorkers(ctx)
	synctest.Wait()
	cancel()
	d.stopWorkers()
	d.releasePending()
	d.visibility.close()
}

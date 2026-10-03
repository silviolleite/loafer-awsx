package consumer

import (
	"context"
	"log/slog"
)

// MaxDeleteBatch exposes maxDeleteBatch to external tests.
const MaxDeleteBatch = maxDeleteBatch

// DeleteRequestTimeout exposes deleteRequestTimeout to external tests.
const DeleteRequestTimeout = deleteRequestTimeout

// DeleteBatcher exposes the unexported deleteBatcher to external tests.
type DeleteBatcher struct {
	b *deleteBatcher
}

// NewDeleteBatcher wraps newDeleteBatcher.
func NewDeleteBatcher(client BatchDeleteClient, queueURL string, capacity int, log *slog.Logger) *DeleteBatcher {
	return &DeleteBatcher{b: newDeleteBatcher(client, queueURL, capacity, log)}
}

// Start wraps deleteBatcher.start.
func (d *DeleteBatcher) Start(ctx context.Context, senders int) {
	d.b.start(ctx, senders)
}

// Enqueue wraps deleteBatcher.enqueue.
func (d *DeleteBatcher) Enqueue(handle string) {
	d.b.enqueue(handle)
}

// Close wraps deleteBatcher.close.
func (d *DeleteBatcher) Close() {
	d.b.close()
}

// Send wraps deleteBatcher.send.
func (d *DeleteBatcher) Send(ctx context.Context, batch []string) {
	d.b.send(ctx, batch)
}

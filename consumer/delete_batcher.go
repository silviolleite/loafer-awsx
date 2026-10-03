package consumer

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/silviolleite/loafer-awsx/logger"
)

const (
	// maxDeleteBatch is the largest number of entries AWS SQS accepts in one
	// DeleteMessageBatch request.
	maxDeleteBatch = 10

	// deleteRequestTimeout bounds every DeleteMessageBatch request, so a stalled
	// request can never block shutdown indefinitely.
	deleteRequestTimeout = 5 * time.Second
)

// deleteBatcher removes committed messages through DeleteMessageBatch requests
// formed opportunistically. Workers hand it receipt handles with enqueue; a
// fixed set of sender goroutines each block until a handle arrives, drain
// whatever else is already queued (up to maxDeleteBatch) without waiting, and
// send one request. No timer is used, so a batch grows only while earlier
// requests are in flight.
//
// The handles channel is owned by the batcher: it is closed exactly once, by
// close, which the owner must call only after every producer has stopped.
// Senders exit when the channel is closed and drained, so no pending delete is
// dropped during a graceful shutdown.
type deleteBatcher struct {
	client   BatchDeleteClient
	log      *slog.Logger
	handles  chan string
	queueURL string
	wg       sync.WaitGroup
}

// newDeleteBatcher returns a batcher for queueURL whose pending-delete queue
// holds up to capacity receipt handles. A nil log is replaced with a no-op
// logger. Senders are not started until start is called.
func newDeleteBatcher(client BatchDeleteClient, queueURL string, capacity int, log *slog.Logger) *deleteBatcher {
	if log == nil {
		log = logger.NewNoOp()
	}
	return &deleteBatcher{
		client:   client,
		log:      log,
		handles:  make(chan string, capacity),
		queueURL: queueURL,
	}
}

// start launches the given number of sender goroutines. ctx is used as the
// parent of every DeleteMessageBatch request; the caller must pass a context
// that is not canceled by the consumer run context (for example
// context.WithoutCancel), so deletes committed during shutdown are still sent.
// Senders exit only when close is called.
func (b *deleteBatcher) start(ctx context.Context, senders int) {
	for range senders {
		b.wg.Add(1)
		go b.run(ctx)
	}
}

// enqueue hands a receipt handle to the senders. It blocks while the queue is
// full, which applies backpressure to the committing worker. It must not be
// called after close.
func (b *deleteBatcher) enqueue(handle string) {
	b.handles <- handle
}

// close stops accepting receipt handles and waits until every sender has sent
// the remaining ones and exited. It must be called exactly once, after every
// producer calling enqueue has stopped.
func (b *deleteBatcher) close() {
	close(b.handles)
	b.wg.Wait()
}

// run is the sender loop. It blocks until a receipt handle arrives, performs a
// non-blocking drain of up to maxDeleteBatch handles in total, and sends them in
// one request. It has no ctx.Done() case by design: it exits only when the
// handles channel is closed and drained, which guarantees pending deletes are
// never dropped.
func (b *deleteBatcher) run(ctx context.Context) {
	defer b.wg.Done()
	batch := make([]string, 0, maxDeleteBatch)
	for first := range b.handles {
		batch = append(batch[:0], first)
	drain:
		for len(batch) < maxDeleteBatch {
			select {
			case h, ok := <-b.handles:
				if !ok {
					break drain
				}
				batch = append(batch, h)
			default:
				break drain
			}
		}
		b.send(ctx, batch)
	}
}

// send deletes batch with one DeleteMessageBatch request bounded by
// deleteRequestTimeout. Entry Ids are the decimal batch indexes, which are
// unique and valid per AWS rules. Failures are logged and swallowed: a message
// whose delete failed reappears after its visibility timeout.
func (b *deleteBatcher) send(ctx context.Context, batch []string) {
	entries := make([]types.DeleteMessageBatchRequestEntry, len(batch))
	for i, handle := range batch {
		entries[i] = types.DeleteMessageBatchRequestEntry{
			Id:            aws.String(strconv.Itoa(i)),
			ReceiptHandle: aws.String(handle),
		}
	}

	out, err := b.request(ctx, entries)
	if err != nil {
		for _, handle := range batch {
			b.log.Error("failed to delete message",
				slog.String("queue_url", b.queueURL),
				slog.String("receipt_handle", handle),
				slog.Any("error", err),
			)
		}
		return
	}
	if out == nil {
		return
	}
	for i := range out.Failed {
		b.reportFailed(&out.Failed[i], batch)
	}
}

// request issues one DeleteMessageBatch call for entries under a context
// bounded by deleteRequestTimeout.
func (b *deleteBatcher) request(
	ctx context.Context,
	entries []types.DeleteMessageBatchRequestEntry,
) (*sqs.DeleteMessageBatchOutput, error) {
	ctx, cancel := context.WithTimeout(ctx, deleteRequestTimeout)
	defer cancel()
	return b.client.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(b.queueURL),
		Entries:  entries,
	})
}

// reportFailed logs one Error record for a failed entry of a response. The
// entry Id is mapped back to the receipt handle at that index of batch; an Id
// that does not match a sent entry is logged raw, without a receipt handle.
func (b *deleteBatcher) reportFailed(entry *types.BatchResultErrorEntry, batch []string) {
	id := aws.ToString(entry.Id)
	attrs := []any{slog.String("queue_url", b.queueURL)}
	if i, ok := batchIndex(id, len(batch)); ok {
		attrs = append(attrs, slog.String("receipt_handle", batch[i]))
	} else {
		attrs = append(attrs, slog.String("id", id))
	}
	attrs = append(attrs,
		slog.String("code", aws.ToString(entry.Code)),
		slog.Bool("sender_fault", entry.SenderFault),
		slog.String("error", aws.ToString(entry.Message)),
	)
	b.log.Error("failed to delete message", attrs...)
}

// batchIndex parses id as an entry Id produced by send and reports whether it
// names a valid index into a batch of size n. Only the canonical decimal form
// is accepted, so an Id such as "01" or "+1" never matches.
func batchIndex(id string, n int) (int, bool) {
	i, err := strconv.Atoi(id)
	if err != nil || i < 0 || i >= n || strconv.Itoa(i) != id {
		return 0, false
	}
	return i, true
}

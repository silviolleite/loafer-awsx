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
	// maxVisibilityBatch is the AWS SQS limit on entries per
	// ChangeMessageVisibilityBatch request.
	maxVisibilityBatch = 10

	// visibilityRequestTimeout bounds every visibility request, including the
	// releases sent during graceful shutdown.
	visibilityRequestTimeout = 5 * time.Second
)

// visibilityChange is one requested visibility timeout for one message.
type visibilityChange struct {
	msg     *message
	timeout int32
}

// visibilityFailure describes why one visibility change failed. code and
// senderFault are set only when AWS reported the failure per entry.
type visibilityFailure struct {
	err         error
	code        string
	message     string
	senderFault bool
	isPerEntry  bool
}

// logAttrs returns the slog attributes that describe the failure.
func (f *visibilityFailure) logAttrs() []any {
	if !f.isPerEntry {
		return []any{slog.Any("error", f.err)}
	}
	return []any{
		slog.String("code", f.code),
		slog.Bool("sender_fault", f.senderFault),
		slog.String("error", f.message),
	}
}

// visibilitySender sends up to maxVisibilityBatch visibility changes. It
// returns one entry per change, in the same order: nil when the change
// succeeded, or the failure to report.
type visibilitySender interface {
	send(ctx context.Context, changes []visibilityChange) []*visibilityFailure
}

// newVisibilitySender returns a batchVisibilitySender when client implements
// BatchVisibilityClient, and a singleVisibilitySender otherwise.
func newVisibilitySender(client SQSClient, queueURL string, log *slog.Logger) visibilitySender {
	if log == nil {
		log = logger.NewNoOp()
	}
	if bc, ok := client.(BatchVisibilityClient); ok {
		return &batchVisibilitySender{client: bc, queueURL: queueURL, log: log}
	}
	return &singleVisibilitySender{client: client, queueURL: queueURL}
}

// batchVisibilitySender sends changes through one ChangeMessageVisibilityBatch
// request.
type batchVisibilitySender struct {
	client   BatchVisibilityClient
	log      *slog.Logger
	queueURL string
}

// send builds one batch request from changes. A request error fails every
// change. A nil output, or an entry missing from Failed, counts as success.
// Failed entries whose Id does not match a sent entry are logged and ignored.
func (s *batchVisibilitySender) send(ctx context.Context, changes []visibilityChange) []*visibilityFailure {
	results := make([]*visibilityFailure, len(changes))
	if len(changes) == 0 {
		return results
	}

	entries := make([]types.ChangeMessageVisibilityBatchRequestEntry, len(changes))
	for i, change := range changes {
		entries[i] = types.ChangeMessageVisibilityBatchRequestEntry{
			Id:                aws.String(strconv.Itoa(i)),
			ReceiptHandle:     aws.String(change.msg.Identifier()),
			VisibilityTimeout: change.timeout,
		}
	}

	ctx, cancel := context.WithTimeout(ctx, visibilityRequestTimeout)
	defer cancel()

	out, err := s.client.ChangeMessageVisibilityBatch(ctx, &sqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: aws.String(s.queueURL),
		Entries:  entries,
	})
	if err != nil {
		for i := range results {
			results[i] = &visibilityFailure{err: err}
		}
		return results
	}
	if out == nil {
		return results
	}

	for i := range out.Failed {
		entry := &out.Failed[i]
		idx, ok := batchIndex(aws.ToString(entry.Id), len(changes))
		if !ok {
			s.log.Error("failed to change message visibility",
				slog.String("queue_url", s.queueURL),
				slog.String("id", aws.ToString(entry.Id)),
				slog.String("code", aws.ToString(entry.Code)),
			)
			continue
		}
		results[idx] = &visibilityFailure{
			code:        aws.ToString(entry.Code),
			message:     aws.ToString(entry.Message),
			senderFault: entry.SenderFault,
			isPerEntry:  true,
		}
	}

	return results
}

// singleVisibilitySender sends one ChangeMessageVisibility request per change.
// It serves SQS clients that do not implement BatchVisibilityClient.
type singleVisibilitySender struct {
	client   SQSClient
	queueURL string
}

// send issues one request per change, all at once, each bounded by its own
// timeout, so a batch of changes costs one round trip instead of one per
// change. It returns once every request has finished.
func (s *singleVisibilitySender) send(ctx context.Context, changes []visibilityChange) []*visibilityFailure {
	results := make([]*visibilityFailure, len(changes))
	var wg sync.WaitGroup
	for i, change := range changes {
		wg.Go(func() {
			if err := s.sendOne(ctx, change); err != nil {
				results[i] = &visibilityFailure{err: err}
			}
		})
	}
	wg.Wait()
	return results
}

// sendOne issues a single ChangeMessageVisibility request.
func (s *singleVisibilitySender) sendOne(ctx context.Context, change visibilityChange) error {
	ctx, cancel := context.WithTimeout(ctx, visibilityRequestTimeout)
	defer cancel()

	_, err := s.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(s.queueURL),
		ReceiptHandle:     aws.String(change.msg.Identifier()),
		VisibilityTimeout: change.timeout,
	})
	return err
}

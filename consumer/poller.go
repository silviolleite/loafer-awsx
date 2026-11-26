package consumer

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/silviolleite/loafer-awsx/errors"
)

// allAttributes requests every custom message attribute on each receive.
var allAttributes = []string{"All"}

// allSystemAttributes requests every system attribute on each receive.
var allSystemAttributes = []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll}

// runPollers runs Route.Pollers() receive loops and waits for all of them to
// return, which happens once ctx is canceled.
func (c *Consumer) runPollers(ctx context.Context, queueURL string, d *dispatcher) {
	var wg sync.WaitGroup
	for range max(c.route.Pollers(), 1) {
		wg.Go(func() { c.poll(ctx, queueURL, d) })
	}
	wg.Wait()
}

// poll runs one receive loop until ctx is canceled. Before each receive it
// acquires one receive batch, min(MaxMessages, Prefetch+1), of credits and
// asks for exactly that many messages, so the consumer never holds more messages than its credit capacity.
// Credits not used by the response are returned at once.
//
// The receive request sets VisibilityTimeout to the route value so each message
// starts hidden for the configured duration, and every message is stamped with
// the instant the response returned: the visibility manager extends it from
// that instant. A receive error returns every credit, is logged, and is
// followed by a retry-timeout wait that honors ctx cancellation.
func (c *Consumer) poll(ctx context.Context, queueURL string, d *dispatcher) {
	for {
		if err := d.credits.acquire(ctx, int(d.receiveBatch)); err != nil {
			return
		}

		out, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:                    aws.String(queueURL),
			MaxNumberOfMessages:         d.receiveBatch,
			WaitTimeSeconds:             c.route.WaitTimeSeconds(),
			VisibilityTimeout:           c.route.VisibilityTimeout(),
			MessageAttributeNames:       allAttributes,
			MessageSystemAttributeNames: allSystemAttributes,
		})
		if err != nil {
			d.credits.release(int(d.receiveBatch))
			if !c.retryAfterError(ctx, queueURL, err) {
				return
			}
			continue
		}

		c.admitReceived(queueURL, d, out, time.Now())
	}
}

// admitReceived admits up to one receive batch of the returned messages and
// returns the credits the response did not use. Messages beyond the requested
// batch, which SQS never returns, are logged and left to become visible again.
func (c *Consumer) admitReceived(queueURL string, d *dispatcher, out *sqs.ReceiveMessageOutput, receivedAt time.Time) {
	var received []types.Message
	if out != nil {
		received = out.Messages
	}

	batch := int(d.receiveBatch)
	n := min(len(received), batch)
	d.credits.release(batch - n)

	for i := n; i < len(received); i++ {
		c.log.Warn("received more messages than requested; leaving message for redelivery",
			slog.String("queue_url", queueURL),
			slog.String("receipt_handle", aws.ToString(received[i].ReceiptHandle)),
		)
	}

	msgs := make([]*message, n)
	for i := range n {
		msgs[i] = newMessage(received[i])
		msgs[i].receivedAt = receivedAt
	}
	d.admit(msgs...)
}

// retryAfterError handles a failed receive. When ctx is already canceled it
// returns false immediately without logging so a shutdown-induced error is not
// reported as a failure. Otherwise it logs the error and waits the configured
// retry timeout, returning true to retry or false if ctx is canceled during the
// wait.
func (c *Consumer) retryAfterError(ctx context.Context, queueURL string, err error) bool {
	if ctx.Err() != nil {
		return false
	}

	c.log.Error("failed to receive messages",
		slog.String("queue_url", queueURL),
		slog.Any("error", errors.Wrap(errors.ErrGetMessage, err)),
	)

	timer := time.NewTimer(c.retryTimeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

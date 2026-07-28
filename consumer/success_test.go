package consumer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/router"
)

// Feature: fifo-scheduled-retry, Property 9: Success deletes without publishing
func TestScheduledSuccessDeletesWithoutPublishing(t *testing.T) {
	log := discardLogger()

	rapid.Check(t, func(t *rapid.T) {
		retryCount := rapid.IntRange(0, 1000).Draw(t, "retryCount")
		groupID := rapid.StringMatching(`[A-Za-z0-9_-]{1,64}`).Draw(t, "groupID")

		sqsClient := &schedSQS{}
		sched := &fakeSchedulerClient{}

		cfg := router.ScheduledRetryConfig{
			TargetQueueARN:   "arn:aws:sqs:us-east-1:123456789012:entry.fifo",
			ExecutionRoleARN: "arn:aws:iam::123456789012:role/scheduler-exec",
			DLQQueueURL:      "https://sqs.us-east-1.amazonaws.com/123456789012/dlq.fifo",
			MaxRetryCount:    500,
			BaseBackoff:      time.Millisecond,
			MaxBackoff:       time.Second,
		}

		d := newScheduledDispatcher(t, cfg, sqsClient, sched, log)

		msg := schedMessage(retryCount, groupID)
		d.processScheduled(context.Background(), msg, nil)

		assert.Len(t, sqsClient.deletes(), 1)
		assert.Empty(t, sqsClient.sends())
		assert.Empty(t, sched.createCalls())
	})
}

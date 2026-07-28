package consumer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/router"
)

// Feature: fifo-scheduled-retry, Property 8: Backoff requests are treated as failures
func TestScheduledBackoffTreatedAsFailure(t *testing.T) {
	log := discardLogger()

	rapid.Check(t, func(t *rapid.T) {
		max := rapid.IntRange(0, 500).Draw(t, "max")
		next := rapid.IntRange(1, 1000).Draw(t, "next")

		sqsClient := &schedSQS{}
		sched := &fakeSchedulerClient{}

		cfg := router.ScheduledRetryConfig{
			TargetQueueARN:   "arn:aws:sqs:us-east-1:123456789012:entry.fifo",
			ExecutionRoleARN: "arn:aws:iam::123456789012:role/scheduler-exec",
			DLQQueueURL:      "https://sqs.us-east-1.amazonaws.com/123456789012/dlq.fifo",
			MaxRetryCount:    max,
			BaseBackoff:      time.Millisecond,
			MaxBackoff:       time.Second,
		}

		d := newScheduledDispatcher(t, cfg, sqsClient, sched, log)

		msg := schedMessage(next-1, "group-1")
		msg.Backoff(time.Second)

		d.processScheduled(context.Background(), msg, nil)

		assert.Empty(t, sqsClient.visChanges())

		schedules := len(sched.createCalls())
		sends := len(sqsClient.sends())

		if next <= max {
			assert.Equal(t, 1, schedules)
			assert.Equal(t, 0, sends)
		} else {
			assert.Equal(t, 0, schedules)
			assert.Equal(t, 1, sends)
		}
	})
}

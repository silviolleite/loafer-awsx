package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/router"
)

// Feature: fifo-scheduled-retry, Property 10: Exactly one metric per outcome when enabled
func TestScheduledPerOutcomeMetrics(t *testing.T) {
	log := discardLogger()

	rapid.Check(t, func(t *rapid.T) {
		outcome := rapid.SampledFrom([]string{"success", "retry", "deadletter"}).Draw(t, "outcome")
		enabled := rapid.Bool().Draw(t, "enabled")

		max := 100
		if outcome == "deadletter" {
			max = 0
		}

		cfg := router.ScheduledRetryConfig{
			TargetQueueARN:   "arn:aws:sqs:us-east-1:123456789012:entry.fifo",
			ExecutionRoleARN: "arn:aws:iam::123456789012:role/scheduler-exec",
			DLQQueueURL:      "https://sqs.us-east-1.amazonaws.com/123456789012/dlq.fifo",
			MaxRetryCount:    max,
			BaseBackoff:      time.Millisecond,
			MaxBackoff:       time.Second,
		}

		sqsClient := &schedSQS{}
		sched := &fakeSchedulerClient{}

		d := newScheduledDispatcher(t, cfg, sqsClient, sched, log)

		var successCount, retryCount, deadCount int
		var successName, retryName, deadName string
		if enabled {
			d.metrics = stubRecorder{
				success: func(routeName string) {
					successCount++
					successName = routeName
				},
				retry: func(routeName string) {
					retryCount++
					retryName = routeName
				},
				deadLetter: func(routeName string) {
					deadCount++
					deadName = routeName
				},
			}
		}

		msg := schedMessage(0, "group-1")

		var handlerErr error
		if outcome != "success" {
			handlerErr = errors.New("handler failure")
		}

		d.processScheduled(context.Background(), msg, handlerErr)

		assert.Len(t, sqsClient.deletes(), 1)

		if !enabled {
			assert.Zero(t, successCount)
			assert.Zero(t, retryCount)
			assert.Zero(t, deadCount)
			return
		}

		switch outcome {
		case "success":
			assert.Equal(t, 1, successCount)
			assert.Zero(t, retryCount)
			assert.Zero(t, deadCount)
			assert.Equal(t, "test", successName)
		case "retry":
			assert.Equal(t, 1, retryCount)
			assert.Zero(t, successCount)
			assert.Zero(t, deadCount)
			assert.Equal(t, "test", retryName)
		case "deadletter":
			assert.Equal(t, 1, deadCount)
			assert.Zero(t, successCount)
			assert.Zero(t, retryCount)
			assert.Equal(t, "test", deadName)
		}
	})
}

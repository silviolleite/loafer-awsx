package consumer

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"

	"github.com/silviolleite/loafer-awsx/router"
)

// Feature: fifo-scheduled-retry, Property 7: A message is deleted only after successful orchestration
func TestScheduledDeleteAfterSuccessfulOrchestration(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		outcome := rapid.SampledFrom([]string{"success", "error", "backoff"}).Draw(t, "outcome")
		scheduleSucceeds := rapid.Bool().Draw(t, "scheduleSucceeds")
		publishSucceeds := rapid.Bool().Draw(t, "publishSucceeds")
		max := rapid.IntRange(0, 500).Draw(t, "max")
		next := rapid.IntRange(1, 1000).Draw(t, "next")

		capture := &captureHandler{}
		log := slog.New(capture)

		sqsClient := &schedSQS{}
		sched := &fakeSchedulerClient{}
		if !scheduleSucceeds {
			sched.createErr = errors.New("create schedule failed")
		}
		if !publishSucceeds {
			sqsClient.sendErr = errors.New("send message failed")
		}

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

		var handlerErr error
		switch outcome {
		case "error":
			handlerErr = errors.New("handler failure")
		case "backoff":
			msg.Backoff(time.Second)
		}

		d.processScheduled(context.Background(), msg, handlerErr)

		schedulePath := next <= max
		orchestrationFailed := false
		expectedDelete := true
		if outcome != "success" {
			if schedulePath {
				expectedDelete = scheduleSucceeds
			} else {
				expectedDelete = publishSucceeds
			}
			orchestrationFailed = !expectedDelete
		}

		if expectedDelete {
			assert.Len(t, sqsClient.deletes(), 1)
		} else {
			assert.Empty(t, sqsClient.deletes())
		}

		if orchestrationFailed {
			assert.Empty(t, sqsClient.deletes())
			assert.GreaterOrEqual(t, capture.errorCount(), 1)
		}
	})
}

// Command localscheduler is a local development helper that emulates what AWS
// EventBridge Scheduler does when a one-time schedule fires. It exists solely to
// close the FIFO Scheduled Retry loop against LocalStack.
//
// Why it is needed: LocalStack's EventBridge Scheduler is mock-only. It accepts
// CreateSchedule but never actually fires the schedule to invoke its target, so
// the retry message is never re-published to the Entry_Queue and the retry/DLQ
// loop never completes locally (see
// https://docs.localstack.cloud/aws/services/scheduler/). This helper fills that
// gap for demos: it polls the schedules the consumer created, and once a
// schedule's fire time has elapsed it performs the SQS SendMessage the schedule
// describes and then deletes the schedule — mirroring EventBridge Scheduler's
// invoke-then-delete behavior (ActionAfterCompletion = DELETE).
//
// The consumer builds each retry schedule with the EventBridge Scheduler
// universal SQS SendMessage target, so the schedule's Target.Input is the full
// JSON of an SQS SendMessage request (queue URL, body, FIFO group id, a fresh
// deduplication id, and the message attributes including the incremented
// retry_count). This helper simply decodes that JSON and calls SendMessage,
// which LocalStack supports fully. As the message bounces back through the
// Entry_Queue its retry_count climbs on each failure until it exceeds the
// route's Max_Retry_Count, at which point the consumer dead-letters it.
//
// This is a demo-only tool. Against real AWS you do NOT run it: EventBridge
// Scheduler fires the schedules itself. Run it alongside the scheduled-retry
// consumer only when developing against LocalStack.
//
// Run it against LocalStack by adjusting the region, credentials, and endpoint
// below to match the consumer example.
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/silviolleite/loafer-awsx/conn"
)

// pollInterval is how often the helper scans for schedules that are due to fire.
const pollInterval = time.Second

// atExpressionLayout matches the timezone-less, whole-second timestamp the
// consumer writes into the one-time at(yyyy-mm-ddThh:mm:ss) schedule expression.
const atExpressionLayout = "2006-01-02T15:04:05"

// universalSQSSendMessageARN is the EventBridge Scheduler universal target ARN
// the consumer uses for retry schedules. The helper only fires schedules that
// target it, leaving any unrelated schedules untouched.
const universalSQSSendMessageARN = "arn:aws:scheduler:::aws-sdk:sqs:sendMessage"

// sqsMessageAttributeValue mirrors the value shape of an SQS message attribute
// inside the universal-target request JSON.
type sqsMessageAttributeValue struct {
	DataType    string `json:"DataType"`
	StringValue string `json:"StringValue"`
}

// sqsSendMessageRequest is the JSON payload the consumer stores as the schedule
// Target.Input. It matches the SQS SendMessage API request shape.
type sqsSendMessageRequest struct {
	MessageAttributes      map[string]sqsMessageAttributeValue `json:"MessageAttributes,omitempty"`
	QueueURL               string                              `json:"QueueUrl"`
	MessageBody            string                              `json:"MessageBody"`
	MessageGroupID         string                              `json:"MessageGroupId"`
	MessageDeduplicationID string                              `json:"MessageDeduplicationId"`
}

func main() {
	// Derive a context that is canceled on interrupt (Ctrl+C) or SIGTERM so the
	// polling loop shuts down cleanly.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Build the AWS connection pointed at LocalStack, matching the scheduled
	// retry consumer example.
	cfg, err := conn.New(ctx,
		conn.WithRegion("us-east-1"),
		conn.WithAccessKey("test", "test"),
		conn.WithEndpoint("http://localhost:4566"),
	)
	if err != nil {
		log.Fatalf("failed to create AWS config: %v", err)
	}

	// This helper deliberately keeps the direct AWS SDK clients rather than the
	// client.NewScheduler / client.NewSQS constructors. It calls Scheduler
	// operations (ListSchedules, GetSchedule, DeleteSchedule) and SQS operations
	// (SendMessage) that fall outside the minimal consumer.SchedulerClient and
	// consumer.SQSClient interfaces those constructors return, so the wrapped
	// clients could not satisfy this tool's needs.
	schedClient := scheduler.NewFromConfig(cfg)
	sqsClient := sqs.NewFromConfig(cfg)

	log.Printf("local EventBridge Scheduler simulator started, polling every %s (press Ctrl+C to stop)", pollInterval)

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("local scheduler simulator stopped")
			return
		case <-ticker.C:
			if err := fireDueSchedules(ctx, schedClient, sqsClient); err != nil {
				if ctx.Err() != nil {
					continue
				}
				log.Printf("error firing due schedules: %v", err)
			}
		}
	}
}

// fireDueSchedules lists every schedule, fires the ones whose at() time has
// elapsed, and deletes each one it fires.
func fireDueSchedules(ctx context.Context, schedClient *scheduler.Client, sqsClient *sqs.Client) error {
	var nextToken *string
	for {
		out, err := schedClient.ListSchedules(ctx, &scheduler.ListSchedulesInput{NextToken: nextToken})
		if err != nil {
			return err
		}

		for _, summary := range out.Schedules {
			fireIfDue(ctx, schedClient, sqsClient, summary)
		}

		if out.NextToken == nil {
			return nil
		}
		nextToken = out.NextToken
	}
}

// fireIfDue loads the full schedule, checks whether its fire time has elapsed,
// and if so performs the described SendMessage and deletes the schedule.
func fireIfDue(
	ctx context.Context,
	schedClient *scheduler.Client,
	sqsClient *sqs.Client,
	summary schedulertypes.ScheduleSummary,
) {
	name := aws.ToString(summary.Name)
	if name == "" {
		return
	}
	groupName := aws.ToString(summary.GroupName)

	sched, err := schedClient.GetSchedule(ctx, &scheduler.GetScheduleInput{
		Name:      aws.String(name),
		GroupName: groupNamePtr(groupName),
	})
	if err != nil {
		log.Printf("failed to get schedule %q: %v", name, err)
		return
	}

	if sched.Target == nil || aws.ToString(sched.Target.Arn) != universalSQSSendMessageARN {
		return
	}

	fireAt, ok := parseAtExpression(aws.ToString(sched.ScheduleExpression))
	if !ok {
		return
	}
	if time.Now().UTC().Before(fireAt) {
		return
	}

	var request sqsSendMessageRequest
	if err := json.Unmarshal([]byte(aws.ToString(sched.Target.Input)), &request); err != nil {
		log.Printf("schedule %q has an unparseable target input, skipping: %v", name, err)
		return
	}

	if _, err := sqsClient.SendMessage(ctx, buildSendMessageInput(request)); err != nil {
		log.Printf("failed to re-publish message for schedule %q: %v", name, err)
		return
	}

	log.Printf("fired schedule %q: re-published message to %s (group=%s retry_count=%s)",
		name, request.QueueURL, request.MessageGroupID, request.MessageAttributes["retry_count"].StringValue)

	if _, err := schedClient.DeleteSchedule(ctx, &scheduler.DeleteScheduleInput{
		Name:      aws.String(name),
		GroupName: groupNamePtr(groupName),
	}); err != nil {
		log.Printf("failed to delete fired schedule %q: %v", name, err)
	}
}

// buildSendMessageInput converts the decoded universal-target request into an
// SQS SendMessage input, preserving the FIFO group id, deduplication id, and
// message attributes.
func buildSendMessageInput(request sqsSendMessageRequest) *sqs.SendMessageInput {
	input := &sqs.SendMessageInput{
		QueueUrl:    aws.String(request.QueueURL),
		MessageBody: aws.String(request.MessageBody),
	}
	if request.MessageGroupID != "" {
		input.MessageGroupId = aws.String(request.MessageGroupID)
	}
	if request.MessageDeduplicationID != "" {
		input.MessageDeduplicationId = aws.String(request.MessageDeduplicationID)
	}
	if len(request.MessageAttributes) > 0 {
		attrs := make(map[string]sqstypes.MessageAttributeValue, len(request.MessageAttributes))
		for key, value := range request.MessageAttributes {
			attrs[key] = sqstypes.MessageAttributeValue{
				DataType:    aws.String(value.DataType),
				StringValue: aws.String(value.StringValue),
			}
		}
		input.MessageAttributes = attrs
	}
	return input
}

// parseAtExpression extracts the fire time from a one-time at(...) schedule
// expression, returning the parsed UTC time and whether parsing succeeded.
func parseAtExpression(expression string) (time.Time, bool) {
	trimmed := strings.TrimSpace(expression)
	if !strings.HasPrefix(trimmed, "at(") || !strings.HasSuffix(trimmed, ")") {
		return time.Time{}, false
	}
	timestamp := trimmed[len("at(") : len(trimmed)-1]

	fireAt, err := time.ParseInLocation(atExpressionLayout, timestamp, time.UTC)
	if err != nil {
		return time.Time{}, false
	}
	return fireAt, true
}

// groupNamePtr returns a pointer to the group name, or nil when it is empty so
// the default schedule group is used.
func groupNamePtr(groupName string) *string {
	if groupName == "" {
		return nil
	}
	return aws.String(groupName)
}

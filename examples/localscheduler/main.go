// Command localscheduler is a local development helper that emulates what AWS
// EventBridge Scheduler does when a one-time retry schedule fires. It exists
// solely to close the FIFO Scheduled Retry loop against MiniStack.
//
// Why it is needed: the consumer builds each retry schedule with the
// EventBridge Scheduler universal SQS SendMessage target
// (arn:aws:scheduler:::aws-sdk:sqs:sendMessage), so the schedule's Target.Input
// is the full JSON of an SQS SendMessage request (queue URL, body, FIFO group
// id, a fresh deduplication id, and the message attributes including the
// incremented retry_count). MiniStack's Scheduler fires schedules with
// templated targets, but it does not implement universal targets: it drops the
// invocation and, because retry schedules use ActionAfterCompletion = DELETE,
// deletes the schedule. The retry would be lost.
//
// What it does: every second it lists the schedules, and for each enabled
// schedule that targets the universal SQS SendMessage target it first claims it
// by setting its state to DISABLED, so MiniStack's own ten-second sweep skips
// it. The claim only protects a schedule that is not yet due when the helper
// sees it, so pair the helper with a base backoff of a few seconds (the
// fifo-scheduled-retry example uses 3s): a schedule that is already due can still
// be consumed by a sweep that lands before the claim, losing the retry. Once the
// schedule's fire time has elapsed, the helper performs the
// SendMessage the schedule describes and deletes the schedule, mirroring
// EventBridge Scheduler's invoke-then-delete behavior. Schedules with any other
// target are left untouched. As the message bounces back through the
// Entry_Queue its retry_count climbs on each failure until it exceeds the
// route's Max_Retry_Count, at which point the consumer dead-letters it.
//
// This is a demo-only tool. Against real AWS you do NOT run it: EventBridge
// Scheduler fires the schedules itself. Run it alongside the scheduled-retry
// consumer only when developing against MiniStack.
//
// Run it against another endpoint by adjusting the region, credentials, and
// endpoint below to match the consumer example.
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

	// Build the AWS connection pointed at MiniStack, matching the scheduled retry
	// consumer example.
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
	// operations (ListSchedules, GetSchedule, UpdateSchedule, DeleteSchedule) and SQS operations
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

// fireDueSchedules lists every schedule, claims each retry schedule it has not
// claimed yet, fires the ones whose at() time has elapsed, and deletes each one it
// fires.
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

// fireIfDue loads the full schedule, claims it if it still targets the universal
// SQS sendMessage target and is enabled, checks whether its fire time has
// elapsed, and if so performs the described SendMessage and deletes the schedule.
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

	if sched.State == schedulertypes.ScheduleStateEnabled {
		claim(ctx, schedClient, sched)
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

// claim disables a retry schedule so the emulator's own scheduler skips it.
// MiniStack sweeps schedules every ten seconds but cannot invoke the universal
// SQS sendMessage target: it drops the invocation and, because the schedule uses
// ActionAfterCompletion=DELETE, deletes the schedule, losing the retry. A
// disabled schedule keeps its target and input, so this helper still fires it
// when due. UpdateSchedule replaces the whole definition, so every field read
// from GetSchedule is sent back unchanged apart from the state. A failed claim
// is only logged: the helper still fires the schedule if it remains.
func claim(ctx context.Context, schedClient *scheduler.Client, sched *scheduler.GetScheduleOutput) {
	_, err := schedClient.UpdateSchedule(ctx, &scheduler.UpdateScheduleInput{
		Name:                       sched.Name,
		GroupName:                  sched.GroupName,
		ScheduleExpression:         sched.ScheduleExpression,
		ScheduleExpressionTimezone: sched.ScheduleExpressionTimezone,
		FlexibleTimeWindow:         sched.FlexibleTimeWindow,
		Target:                     sched.Target,
		ActionAfterCompletion:      sched.ActionAfterCompletion,
		Description:                sched.Description,
		StartDate:                  sched.StartDate,
		EndDate:                    sched.EndDate,
		KmsKeyArn:                  sched.KmsKeyArn,
		State:                      schedulertypes.ScheduleStateDisabled,
	})
	if err != nil {
		log.Printf("failed to claim schedule %q: %v", aws.ToString(sched.Name), err)
		return
	}
	log.Printf("claimed schedule %q (disabled so the emulator does not consume it)", aws.ToString(sched.Name))
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

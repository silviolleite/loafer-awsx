package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/scheduler/types"

	"github.com/silviolleite/loafer-awsx/errors"
	"github.com/silviolleite/loafer-awsx/idgen"
)

const (
	// universalSQSSendMessageARN is the EventBridge Scheduler universal target
	// ARN that invokes the SQS SendMessage API. The templated SQS target cannot
	// carry message attributes or an explicit MessageDeduplicationId, so the
	// universal target is used to re-publish retries with the full request shape.
	universalSQSSendMessageARN = "arn:aws:scheduler:::aws-sdk:sqs:sendMessage"

	// atExpressionLayout is the time layout for the one-time at() schedule
	// expression. EventBridge Scheduler expects a timezone-less, whole-second
	// timestamp of the form at(yyyy-mm-ddThh:mm:ss).
	atExpressionLayout = "2006-01-02T15:04:05"
)

// sqsMessageAttributeValue mirrors the value shape of an SQS message attribute
// in the universal-target request JSON: a data type plus a string value.
type sqsMessageAttributeValue struct {
	DataType    string `json:"DataType"`
	StringValue string `json:"StringValue"`
}

// sqsSendMessageRequest is the JSON payload passed as the universal target
// Input. It matches the SQS SendMessage API request shape so EventBridge
// Scheduler can invoke SendMessage against the Entry_Queue.
type sqsSendMessageRequest struct {
	QueueUrl               string                              `json:"QueueUrl"`
	MessageBody            string                              `json:"MessageBody"`
	MessageGroupId         string                              `json:"MessageGroupId"`
	MessageDeduplicationId string                              `json:"MessageDeduplicationId"`
	MessageAttributes      map[string]sqsMessageAttributeValue `json:"MessageAttributes,omitempty"`
}

// retryScheduler creates one-time EventBridge Scheduler schedules that
// re-publish a failed FIFO message to its Entry_Queue after a backoff delay. It
// owns the scheduler client, the resolved scheduler identity (target queue ARN,
// execution role ARN, and target queue URL), and a deduplication-id generator
// used to mint both the fresh MessageDeduplicationId and the unique schedule
// name.
type retryScheduler struct {
	client           SchedulerClient
	idgen            idgen.GroupIDGenerator
	targetQueueARN   string
	executionRoleARN string
	targetQueueURL   string
	now              func() time.Time
}

// newRetryScheduler builds a retryScheduler from the scheduler client, the
// resolved scheduler identity, and the deduplication-id generator. targetQueueARN
// is the Entry_Queue ARN configured as the schedule target, executionRoleARN is
// the role EventBridge Scheduler assumes to send the message, and targetQueueURL
// is the Entry_Queue URL the re-published SendMessage request is addressed to.
func newRetryScheduler(
	client SchedulerClient,
	targetQueueARN, executionRoleARN, targetQueueURL string,
	gen idgen.GroupIDGenerator,
) *retryScheduler {
	return &retryScheduler{
		client:           client,
		idgen:            gen,
		targetQueueARN:   targetQueueARN,
		executionRoleARN: executionRoleARN,
		targetQueueURL:   targetQueueURL,
		now:              time.Now,
	}
}

// schedule creates a one-time schedule that re-publishes msg to the Entry_Queue
// after backoff, carrying the original body and FIFO group id, the original user
// attributes with retry_count set to next (capped at ten attributes), and a
// fresh MessageDeduplicationId. The schedule fires at now+backoff, deletes itself
// after its single invocation, and uses a rigid (non-flexible) time window. It
// returns an error wrapping errors.ErrRetryScheduleCreate on any failure so the
// caller retains the original message.
func (rs *retryScheduler) schedule(ctx context.Context, msg *message, next int, backoff time.Duration) error {
	dedupID, err := rs.idgen.Generate(ctx, nil)
	if err != nil {
		return errors.Wrap(errors.ErrRetryScheduleCreate, err)
	}

	request := sqsSendMessageRequest{
		QueueUrl:               rs.targetQueueURL,
		MessageBody:            string(msg.Body()),
		MessageGroupId:         msg.SystemAttributeByKey(messageGroupIDKey),
		MessageDeduplicationId: dedupID,
		MessageAttributes:      buildRetryAttributes(msg.UserMessageAttributes(), next),
	}

	input, err := json.Marshal(request)
	if err != nil {
		return errors.Wrap(errors.ErrRetryScheduleCreate, err)
	}

	fireAt := scheduleAt(rs.now().UTC(), backoff)
	expression := fmt.Sprintf("at(%s)", fireAt.Format(atExpressionLayout))

	_, err = rs.client.CreateSchedule(ctx, &scheduler.CreateScheduleInput{
		Name:                  aws.String(dedupID),
		ScheduleExpression:    aws.String(expression),
		ActionAfterCompletion: types.ActionAfterCompletionDelete,
		FlexibleTimeWindow: &types.FlexibleTimeWindow{
			Mode: types.FlexibleTimeWindowModeOff,
		},
		Target: &types.Target{
			Arn:     aws.String(universalSQSSendMessageARN),
			RoleArn: aws.String(rs.executionRoleARN),
			Input:   aws.String(string(input)),
		},
	})
	if err != nil {
		return errors.Wrap(errors.ErrRetryScheduleCreate, err)
	}

	return nil
}

// buildRetryAttributes copies the original native user attributes onto the
// re-published message and sets retry_count to next. It guarantees retry_count is
// present and caps the total number of attributes at the SQS limit of ten by
// reserving one slot for retry_count: when the original attributes would exceed
// the remaining nine slots, the surplus is dropped while retry_count is always
// kept.
func buildRetryAttributes(userAttrs map[string]string, next int) map[string]sqsMessageAttributeValue {
	attrs := make(map[string]sqsMessageAttributeValue, maxMessageAttributes)

	remaining := maxMessageAttributes - 1
	for key, value := range userAttrs {
		if key == retryCountAttribute {
			continue
		}
		if remaining <= 0 {
			break
		}
		attrs[key] = sqsMessageAttributeValue{DataType: stringDataType, StringValue: value}
		remaining--
	}

	attrs[retryCountAttribute] = sqsMessageAttributeValue{
		DataType:    numberDataType,
		StringValue: strconv.Itoa(next),
	}

	return attrs
}

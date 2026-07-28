# Requirements Document

## Introduction

This feature adds a second, selectable retry model to the FIFO consumption path of
the loafer-awsx library. The standard (non-FIFO) SNS/SQS model is complete and out
of scope. The existing FIFO retry model — where a failing message stays in the FIFO
queue and its visibility timeout is extended — is preserved unchanged and remains the
default. The evolution introduces a new "Scheduled Retry" model (the "fat consumer")
in which the consumer owns the entire retry lifecycle so that a failing message never
blocks its MessageGroupId.

Under the Scheduled Retry model, when a handler fails the consumer increments a retry
counter carried on the message, schedules a delayed re-publish of the message through
AWS EventBridge Scheduler, and then deletes the original message from the FIFO queue so
the group is unblocked immediately. When the retry counter exceeds a configurable
threshold, the consumer publishes the message directly to a Dead Letter Queue instead
of scheduling another retry. Consumption metrics may optionally be emitted. Any
success-side publishing (to a topic, an API, or any other destination) is the handler's
responsibility; the library does not own or perform success publishing.

The retry model is selectable per route, and enabling the new model must not change the
behavior of routes that use the existing model.

The Scheduled Retry model deliberately trades two FIFO guarantees for group liveness.
First, because a failed message is deleted and re-published later while the next message
in the same group is processed immediately, strict ordering within a Message_Group_Id is
not preserved for messages that are retried. Second, because the model creates the retry
(schedule or DLQ publish) before deleting the original, a failure of the delete step can
leave both the original and the re-published copy in play, so delivery is at-least-once
and handlers are expected to be idempotent. These are accepted tradeoffs of the model and
are captured explicitly in the requirements below.

## Glossary

- **Consumer**: The loafer-awsx component that polls a queue, dispatches messages to a
  handler, and applies the outcome of each handler invocation.
- **Route**: The per-queue configuration unit that binds a queue, a handler, and the
  options controlling consumption, including the selected Retry_Model.
- **Retry_Model**: A per-route configuration value selecting how failed messages are
  retried. It is exactly one of Visibility_Retry_Model or Scheduled_Retry_Model.
- **Visibility_Retry_Model**: The existing FIFO retry behavior in which a failed message
  is left in the FIFO queue and its visibility timeout is extended until it succeeds or
  is redriven natively by AWS SQS. This is the default Retry_Model.
- **Scheduled_Retry_Model**: The new "fat consumer" retry behavior in which the Consumer
  increments the Retry_Count, schedules a delayed re-publish through EventBridge_Scheduler,
  and deletes the original message from the Entry_Queue so the Message_Group_Id is
  unblocked.
- **Retry_Count**: A non-negative integer message attribute recording how many delivery
  attempts have failed for a message. It starts at zero for a first delivery and is
  incremented by one on each handler failure under the Scheduled_Retry_Model.
- **Max_Retry_Count**: The configurable inclusive threshold of failed attempts after
  which the Consumer routes a message to the DLQ instead of scheduling another retry.
- **Backoff_Delay**: The computed time duration between a handler failure and the
  EventBridge_Scheduler re-publish of the message.
- **Retry_Scheduler**: The Consumer component that creates one-time EventBridge_Scheduler
  schedules to re-publish delayed messages.
- **EventBridge_Scheduler**: The AWS EventBridge Scheduler service used to re-publish a
  message to the Entry_Queue after the Backoff_Delay elapses.
- **Entry_Queue**: The source SQS FIFO queue that the Route consumes from and to which
  scheduled retries are re-published.
- **DLQ**: The Dead Letter Queue destination to which the Consumer publishes a message
  whose Retry_Count has exceeded Max_Retry_Count.
- **DLQ_Publisher**: The Consumer component that publishes a message to the DLQ under the
  Scheduled_Retry_Model.
- **Message_Group_Id**: The SQS FIFO MessageGroupId system attribute that serializes
  ordered processing within a group.
- **Message_Deduplication_Id**: The SQS FIFO MessageDeduplicationId assigned to a
  re-published retry message so it is not rejected as a duplicate.

## Requirements

### Requirement 1: Selectable retry model per route

**User Story:** As a library user, I want to select the FIFO retry model per route, so
that I can adopt the new scheduled retry behavior only where I need it while keeping
existing routes unchanged.

#### Acceptance Criteria

1. THE Route SHALL expose a configuration option that sets the Retry_Model to exactly one of Visibility_Retry_Model or Scheduled_Retry_Model.
2. WHERE no Retry_Model is configured, THE Route SHALL use Visibility_Retry_Model.
3. WHILE a Route uses Visibility_Retry_Model, THE Consumer SHALL apply the existing behavior of extending the message visibility timeout on backoff and leaving the message in the Entry_Queue.
4. IF a Retry_Model value that is not Visibility_Retry_Model or Scheduled_Retry_Model is supplied, THEN THE Route SHALL reject the configuration at Route configuration time and return a configuration error to the caller identifying the invalid value.
5. IF a Retry_Model configuration error is returned for a Route, THEN THE Consumer SHALL NOT begin consuming messages from that Route's Entry_Queue.
6. THE Route SHALL apply the configured Retry_Model independently of the configured run mode.

### Requirement 2: Retry attempt tracking

**User Story:** As a library user, I want the number of failed attempts tracked on the
message, so that the consumer can decide when to keep retrying and when to route to the
DLQ.

#### Acceptance Criteria

1. WHEN the Consumer reads a message under Scheduled_Retry_Model, THE Consumer SHALL determine the current Retry_Count by parsing the value of the message Retry_Count attribute as a non-negative integer.
2. IF a message has no Retry_Count attribute, THEN THE Consumer SHALL treat the current Retry_Count as zero.
3. IF a message Retry_Count attribute is present but its value does not parse as a non-negative integer, THEN THE Consumer SHALL treat the current Retry_Count as zero.
4. IF a message Retry_Count attribute is present but its value does not parse as a non-negative integer, THEN THE Consumer SHALL record the malformed attribute through the configured logger.
5. WHEN a handler returns an error under Scheduled_Retry_Model, THE Consumer SHALL compute the next Retry_Count as the current Retry_Count plus one.
6. WHEN the Consumer schedules a retry, THE Consumer SHALL set the Retry_Count attribute of the re-published message to the computed next Retry_Count.

### Requirement 3: Scheduled retry on handler failure

**User Story:** As a library user, I want a failed message to be re-published after a
delay through EventBridge Scheduler, so that transient failures are retried without
blocking the message group.

#### Acceptance Criteria

1. WHEN a handler returns an error under Scheduled_Retry_Model AND the next Retry_Count is at or below Max_Retry_Count, THE Consumer SHALL create a one-time EventBridge_Scheduler schedule that re-publishes the message to the Entry_Queue after the Backoff_Delay.
2. WHEN the Retry_Scheduler creates a retry schedule, THE Retry_Scheduler SHALL include the message body, the user-defined message attributes, and the computed next Retry_Count in the scheduled re-publish, without exceeding the SQS limit of ten message attributes.
3. WHEN the Retry_Scheduler creates a retry schedule for a FIFO message, THE Retry_Scheduler SHALL set the Message_Group_Id of the re-published message to the Message_Group_Id of the original message.
4. WHEN the Retry_Scheduler creates a retry schedule for a FIFO message, THE Retry_Scheduler SHALL assign a Message_Deduplication_Id that is distinct from the original message Message_Deduplication_Id.
5. WHEN the Retry_Scheduler successfully creates a retry schedule, THE Consumer SHALL delete the original message from the Entry_Queue.
6. WHEN the Consumer deletes the original message after scheduling a retry, THE Consumer SHALL leave the Message_Group_Id available for the next message in the group.
7. IF the Retry_Scheduler fails to create the retry schedule, THEN THE Consumer SHALL retain the original message in the Entry_Queue without deleting it and SHALL surface an error indicating that the retry schedule could not be created.
8. IF a handler returns an error under Scheduled_Retry_Model AND the next Retry_Count exceeds Max_Retry_Count, THEN THE Consumer SHALL NOT create a retry schedule and SHALL route the message to the DLQ as specified in Requirement 5.
9. WHEN a handler requests backoff under Scheduled_Retry_Model, THE Consumer SHALL treat the outcome as a handler failure and apply the scheduled-retry-or-DLQ decision rather than extending the message visibility timeout.
10. THE Route SHALL require the Entry_Queue to use explicit Message_Deduplication_Id deduplication, so that a re-published retry carrying an unchanged body is not discarded by content-based deduplication.

### Requirement 4: Backoff delay computation

**User Story:** As a library user, I want the retry delay to grow with the number of
attempts within configurable bounds, so that repeated failures are spaced out without
exceeding service limits.

#### Acceptance Criteria

1. THE Route SHALL expose configuration for a base Backoff_Delay and a maximum Backoff_Delay, expressed in milliseconds, for the Scheduled_Retry_Model, each within the inclusive range of 1 millisecond to the library-defined maximum backoff guard of 86,400,000 milliseconds (24 hours).
2. WHEN the Consumer computes the Backoff_Delay for a retry, THE Consumer SHALL derive a duration that is greater than or equal to the Backoff_Delay computed for the immediately preceding Retry_Count, and for the first retry SHALL use the base Backoff_Delay.
3. IF the computed Backoff_Delay exceeds the configured maximum Backoff_Delay, THEN THE Consumer SHALL use the configured maximum Backoff_Delay.
4. THE Consumer SHALL compute a Backoff_Delay of at least 1 millisecond for every retry.
5. WHERE no base Backoff_Delay is configured, THE Route SHALL apply a default base Backoff_Delay of 1,000 milliseconds.
6. IF a configured base or maximum Backoff_Delay is outside the inclusive range of 1 millisecond to 86,400,000 milliseconds, THEN THE Route SHALL return a configuration error identifying the invalid value AND SHALL NOT start the Scheduled_Retry_Model.
7. IF a configured maximum Backoff_Delay is less than the configured base Backoff_Delay, THEN THE Route SHALL return a configuration error identifying the conflict AND SHALL NOT start the Scheduled_Retry_Model.

### Requirement 5: Dead letter routing on threshold exceeded

**User Story:** As a library user, I want messages that exhaust their retries sent
directly to the DLQ by the consumer, so that poison messages are removed from the group
without relying on native SQS redrive.

#### Acceptance Criteria

1. WHEN a handler returns an error under Scheduled_Retry_Model AND the incremented Retry_Count is greater than Max_Retry_Count, THE DLQ_Publisher SHALL publish the message to the DLQ.
2. WHEN the DLQ_Publisher publishes a message to the DLQ, THE DLQ_Publisher SHALL include the message body, all user-defined message attributes, and the final Retry_Count that exceeded Max_Retry_Count.
3. WHEN the DLQ_Publisher confirms that a message publish to the DLQ is successful, THE Consumer SHALL delete the original message from the Entry_Queue.
4. IF the DLQ_Publisher fails to publish a message to the DLQ, THEN THE Consumer SHALL retain the original message in the Entry_Queue without deleting it AND SHALL surface an error indicating that the DLQ publish failed.
5. THE Route SHALL expose configuration for the Max_Retry_Count and the DLQ destination used by the Scheduled_Retry_Model.
6. IF the Scheduled_Retry_Model is selected without a DLQ destination configured, THEN THE Route SHALL return a configuration error identifying the missing DLQ destination.
7. IF a configured Max_Retry_Count is outside the inclusive integer range of 0 to 2,147,483,647, THEN THE Route SHALL return a configuration error identifying the invalid value.

### Requirement 6: Message preservation on retry-orchestration failure

**User Story:** As a library user, I want the original message retained when scheduling
or DLQ publishing fails, so that no message is lost when the retry orchestration cannot
complete.

#### Acceptance Criteria

1. IF the Retry_Scheduler fails to create a retry schedule, THEN THE Consumer SHALL NOT delete the original message from the Entry_Queue and SHALL leave it available for redelivery after its visibility timeout elapses.
2. IF the DLQ_Publisher fails to publish a message to the DLQ, THEN THE Consumer SHALL NOT delete the original message from the Entry_Queue and SHALL leave it available for redelivery after its visibility timeout elapses.
3. WHEN the Consumer retains a message because retry orchestration failed, THE Consumer SHALL record through the configured logger a log entry that indicates the orchestration failure reason and identifies the affected message.
4. THE Consumer SHALL delete the original message from the Entry_Queue only after either a retry schedule is created or a DLQ publish succeeds.
5. WHEN the Consumer retains a message because retry orchestration failed, THE Consumer SHALL keep the Message_Group_Id of the retained message blocked until the retained message is redelivered and its outcome is applied.

### Requirement 7: Successful processing outcome

**User Story:** As a library user, I want a successfully processed message removed from
the queue, so that the group advances, while any success-side publishing remains my
handler's responsibility.

#### Acceptance Criteria

1. WHEN a handler succeeds under Scheduled_Retry_Model, THE Consumer SHALL delete the message from the Entry_Queue so the Message_Group_Id is left available for the next message in the group.
2. WHEN a handler succeeds under Scheduled_Retry_Model, THE Consumer SHALL NOT publish any success event to any destination, leaving success-side publishing to the handler.
3. IF the Consumer fails to delete the message from the Entry_Queue after a handler succeeds under Scheduled_Retry_Model, THEN THE Consumer SHALL record the failure, including the failure cause, through the configured logger.

### Requirement 8: Consumption metrics

**User Story:** As a library user, I want business and technical metrics emitted on
consumption, so that I can observe retry, success, and dead-letter behavior of the new
model.

#### Acceptance Criteria

1. WHERE metrics are enabled for a Route, WHEN a handler succeeds under Scheduled_Retry_Model, THE Consumer SHALL emit exactly one success metric labeled with the route name.
2. WHERE metrics are enabled for a Route, WHEN the Consumer schedules a retry under Scheduled_Retry_Model, THE Consumer SHALL emit exactly one retry metric labeled with the route name.
3. WHERE metrics are enabled for a Route, WHEN the DLQ_Publisher publishes a message to the DLQ under Scheduled_Retry_Model, THE Consumer SHALL emit exactly one dead-letter metric labeled with the route name.
4. WHERE metrics are not enabled for a Route, THE Consumer SHALL process messages under Scheduled_Retry_Model without emitting any metric.
5. IF emitting a metric fails under Scheduled_Retry_Model, THEN THE Consumer SHALL record the failure through the configured logger and SHALL complete the message outcome without retaining or re-processing the message due to the metric failure.

### Requirement 9: EventBridge Scheduler resource lifecycle

**User Story:** As a library user, I want one-time retry schedules cleaned up after they
fire, so that scheduled retries do not accumulate as orphaned resources.

#### Acceptance Criteria

1. WHEN the Retry_Scheduler creates a retry schedule, THE Retry_Scheduler SHALL configure the schedule to be automatically deleted by the EventBridge_Scheduler after it completes its single invocation, such that no schedule resource remains after the invocation completes.
2. WHEN the Retry_Scheduler creates a retry schedule, THE Retry_Scheduler SHALL set the schedule invocation time to the current time plus the Backoff_Delay, within a tolerance of plus or minus 1 second.
3. THE Route SHALL expose configuration for the EventBridge_Scheduler identity used to create schedules, including the target Entry_Queue reference and the execution role reference.
4. IF the Scheduled_Retry_Model is selected without the EventBridge_Scheduler configuration required to create schedules, THEN THE Route SHALL return a configuration error that identifies each missing configuration item (target Entry_Queue reference or execution role reference) AND SHALL NOT begin message processing.
5. IF the EventBridge_Scheduler request to create a retry schedule fails, THEN THE Retry_Scheduler SHALL return an error indicating the schedule was not created AND SHALL leave the source message available for reprocessing without deletion or acknowledgement.

### Requirement 10: Backward compatibility

**User Story:** As an existing library user, I want the current FIFO and standard
behavior unchanged, so that upgrading to add the new model does not affect my existing
consumers.

#### Acceptance Criteria

1. WHILE a Route uses Visibility_Retry_Model, WHEN a handler requests backoff, THE Consumer SHALL extend the message visibility timeout by the configured backoff and leave the message in the Entry_Queue without deleting it.
2. THE Consumer SHALL construct without returning a configuration error for a Route using Visibility_Retry_Model that has no EventBridge_Scheduler or DLQ destination configuration.
3. WHILE a Route uses Visibility_Retry_Model, THE Consumer SHALL NOT create EventBridge_Scheduler schedules or publish to any DLQ destination.
4. WHILE a Route uses Visibility_Retry_Model, THE Consumer SHALL rely on native AWS SQS redrive for dead-letter handling and SHALL NOT itself move messages to a DLQ.

### Requirement 11: Delivery semantics and DLQ model exclusivity

**User Story:** As a library user, I want the delivery guarantees and dead-letter model
of the Scheduled_Retry_Model stated explicitly, so that I can design idempotent handlers
and avoid conflicting DLQ configuration.

#### Acceptance Criteria

1. WHILE a Route uses Scheduled_Retry_Model, THE Consumer SHALL provide at-least-once delivery and SHALL NOT guarantee ordered processing within a Message_Group_Id for messages that are retried.
2. IF the Consumer creates a retry schedule or publishes to the DLQ successfully but the subsequent deletion of the original message fails, THEN THE Consumer SHALL leave the original message for redelivery, accepting that the message may be delivered more than once.
3. IF a Route configures both the Scheduled_Retry_Model DLQ destination and the existing observe-only DLQ option, THEN THE Route SHALL return a configuration error identifying the conflicting DLQ configuration.
4. WHILE a Route uses Scheduled_Retry_Model, THE Consumer SHALL route exhausted messages to the DLQ by publishing to the configured DLQ destination and SHALL NOT rely on native AWS SQS redrive for dead-letter handling.

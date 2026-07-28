# Implementation Plan: FIFO Scheduled Retry

## Overview

This plan implements the per-route Scheduled Retry model inside the existing `consumer`
and `router` packages, following the design. It builds bottom-up so every step compiles
and is exercised before the next depends on it: sentinel errors, the new AWS dependency,
and fakes first; then the pure helpers (`computeBackoff`, `scheduleAt`, `parseRetryCount`)
with their property tests; then native message-attribute accessors; then router
configuration and validation; then the two orchestration collaborators
(`retryScheduler`, `dlqPublisher`); then metrics hooks and the
scheduled-aware visibility mode; then the `dispatcher.processScheduled` branch that wires
them together; then `Consumer.Run` wiring; and finally backward-compatibility and
end-to-end integration tests. No orphaned code is introduced: each collaborator is
consumed by the dispatcher, and the dispatcher is consumed by `Consumer.Run`.

The implementation language is Go, matching the existing codebase and the concrete Go
shown throughout the design. Tests follow the workspace Go steering: `_test` packages,
no comments except the required property tag, `pgregory.net/rapid` for data generation,
table-driven plus property-based, deterministic, high coverage.

## Tasks

- [x] 1. Foundation: sentinel errors, AWS dependency, client interfaces, and fakes
  - [x] 1.1 Add Scheduled-model sentinel errors
    - Add `ErrRetryScheduleCreate`, `ErrDLQPublish`, and `ErrScheduledRetryConfig` to `errors/errors.go` using the existing `New` constructor so they are matchable with `errors.Is` and wrappable via `errors.Wrap`
    - Add doc comments for each sentinel in English
    - Extend `errors/errors_test.go` to assert each new sentinel is distinct and matches through `errors.Wrap`
    - _Requirements: 1.4, 3.7, 4.6, 4.7, 5.4, 5.6, 5.7, 9.4, 9.5, 11.3_

  - [x] 1.2 Add the EventBridge Scheduler SDK dependency
    - Add `github.com/aws/aws-sdk-go-v2/service/scheduler` to `go.mod` and run `go mod tidy` to populate `go.sum`
    - Verify the resolved version is compatible with the pinned `github.com/aws/aws-sdk-go-v2 v1.43.0` core
    - _Requirements: 3.1, 9.1_

  - [x] 1.3 Extend and add consumer client interfaces
    - In `consumer/client.go` add `SendMessage(ctx, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)` to `SQSClient` (additive; `*sqs.Client` still satisfies it)
    - Add `SchedulerClient` interface mirroring `scheduler.Client.CreateSchedule`
    - Add compile-time assertions/doc comments noting the concrete AWS clients satisfy them
    - _Requirements: 3.1, 5.1_

  - [x] 1.4 Extend the SQS fake with SendMessage
    - In `fake/sqs_client.go` add `SendMessageFunc`, a recorded `sendMessageCalls` slice guarded by the existing mutex, the `SendMessage` method, and a `SendMessageCalls()` accessor following the existing pattern
    - Keep the `var _ consumer.SQSClient = (*SQSClient)(nil)` assertion valid
    - _Requirements: 5.1, 5.2_

  - [x] 1.5 Add the Scheduler fake
    - Create `fake/scheduler_client.go` with a `SchedulerClient` test double: `CreateScheduleFunc`, recorded `createScheduleCalls` guarded by a mutex, the `CreateSchedule` method, and a `CreateScheduleCalls()` accessor
    - Add `var _ consumer.SchedulerClient = (*SchedulerClient)(nil)`
    - _Requirements: 3.1, 9.1_

- [x] 2. Pure helpers: backoff, schedule time, and retry-count parsing
  - [x] 2.1 Implement `computeBackoff` and `scheduleAt`
    - Create `consumer/backoff.go` with `computeBackoff(attempt int, base, max time.Duration) time.Duration` implementing `base * 2^(attempt-1)`, clamped to `[1ms, max]`, monotonic non-decreasing, checking the shift against `max` before shifting to avoid overflow
    - Add `scheduleAt(now time.Time, backoff time.Duration) time.Time` returning `now.Add(backoff)` truncated to whole seconds
    - _Requirements: 4.2, 4.3, 4.4, 9.2_

  - [x] 2.2 Write property test for backoff computation
    - **Property 1: Backoff is bounded, monotonic, and seeded by base**
    - **Validates: Requirements 4.2, 4.3, 4.4, 4.5**
    - Create `consumer/backoff_test.go`; generate `base`/`max` in range and attempt pairs `i <= j`; assert monotonicity, `1ms <= computeBackoff(i) <= max`, and `computeBackoff(1)` equals base clamped; include boundary generators (1ms, 24h, base == max); tag: `// Feature: fifo-scheduled-retry, Property 1: ...`

  - [x] 2.3 Write property test for schedule invocation time
    - **Property 6: Schedule invocation time equals now plus backoff**
    - **Validates: Requirements 9.2**
    - In `consumer/backoff_test.go` generate `now` and backoff; assert `scheduleAt(now, backoff)` equals `now + backoff` within ±1s (truncation error); tag: `// Feature: fifo-scheduled-retry, Property 6: ...`

  - [x] 2.4 Implement `parseRetryCount`
    - Create `consumer/retrycount.go` with `parseRetryCount(msg *message, log *slog.Logger) int` that reads the native `retry_count` user attribute, parses it as a non-negative base-10 integer, defaults to `0` when absent or malformed, and logs the malformed value through the provided logger
    - _Requirements: 2.1, 2.2, 2.3, 2.4_

  - [x] 2.5 Write property and unit tests for retry-count parsing
    - **Property 2: Retry-count parsing defaults to zero**
    - **Validates: Requirements 2.1, 2.2, 2.3**
    - Create `consumer/retrycount_test.go`; generate arbitrary strings, valid non-negative integers, and the absent case; assert parsed value and zero-default; add a unit case asserting a malformed value records a log entry via the fake log handler; tag: `// Feature: fifo-scheduled-retry, Property 2: ...`
    - _Requirements: 2.4_

- [x] 3. Native SQS user message-attribute accessors
  - [x] 3.1 Add native user-attribute accessors to `message`
    - In `consumer/message.go` add `UserMessageAttribute(key string) string` and `UserMessageAttributes() map[string]string` that read the native SQS `types.Message.MessageAttributes` (distinct from the SNS-envelope `Attributes()` and `SystemAttributes()`), returning a fresh copy from `UserMessageAttributes`
    - _Requirements: 2.1, 3.2, 5.2_

  - [x] 3.2 Write unit tests for native user-attribute accessors
    - In `consumer/message_test.go` add table-driven cases: present attribute, absent attribute (empty string), copy independence for the map, and the empty-map case
    - _Requirements: 2.1, 3.2, 5.2_

- [x] 4. Router configuration and validation
  - [x] 4.1 Add retry-model type, config struct, and getters
    - Create `router/scheduled_retry.go` with `RetryModel` enum (`VisibilityRetryModel` = iota default, `ScheduledRetryModel`) and the `ScheduledRetryConfig` struct (TargetQueueARN, ExecutionRoleARN, DLQQueueURL, MaxRetryCount, BaseBackoff, MaxBackoff)
    - Add `retryModel RetryModel` and `scheduledRetry *ScheduledRetryConfig` fields to the `Route` struct in `router/router.go` and add `RetryModel()` and `ScheduledRetry()` getters
    - _Requirements: 1.1, 1.2, 1.6_

  - [x] 4.2 Add router options and fail-fast validation
    - In `router/options.go` add `WithRetryModel(m RetryModel)` (rejects unknown values) and `WithScheduledRetry(opts ...ScheduledRetryOption)` (sets model to Scheduled and attaches a validated config), plus sub-options `WithSchedulerIdentity(targetQueueARN, executionRoleARN)`, `WithScheduledDLQ(dlqQueueURL)`, `WithMaxRetryCount(n)`, `WithBackoff(base, max)`
    - Validate at construction, returning errors wrapping `errors.ErrScheduledRetryConfig` that identify the offending value: required scheduler identity items named individually; required DLQ destination; `MaxRetryCount` in `[0, 2147483647]`; base/max backoff in `[1ms, 86_400_000ms]` with `max >= base`; base defaults to `1000ms` when unset; and mutual exclusivity with the observe-only `WithDLQ`
    - _Requirements: 1.1, 1.4, 3.10, 4.1, 4.5, 4.6, 4.7, 5.5, 5.6, 5.7, 9.3, 9.4, 11.3_

  - [x] 4.3 Write example tests for router configuration and defaults
    - Create `router/scheduled_retry_test.go` with table-driven cases asserting: default `RetryModel()` is Visibility (Req 1.2); an invalid `RetryModel` value is rejected (Req 1.4); each invalid/missing/conflicting Scheduled config returns the expected wrapped error and does not build the route (Req 4.6, 4.7, 5.6, 5.7, 9.4, 11.3); base backoff defaults to `1000ms` (Req 4.5); a valid Scheduled config builds and getters return it
    - _Requirements: 1.2, 1.4, 4.5, 4.6, 4.7, 5.6, 5.7, 9.4, 11.3_

- [x] 5. Retry orchestration collaborators
  - [x] 5.1 Implement `retryScheduler`
    - Create `consumer/retry_scheduler.go` owning a `SchedulerClient`, resolved scheduler identity (target queue ARN, execution role ARN, target queue URL), and a dedup-id generator (`idgen`)
    - Build the universal-target `Input` JSON of an SQS `SendMessage` request: `MessageBody` = original raw body; `MessageAttributes` = original native user attributes with `retry_count` set to the next count, capped at ten attributes; `MessageGroupId` = original group id; fresh distinct `MessageDeduplicationId`
    - Call `CreateSchedule` with `ScheduleExpression = at(scheduleAt(now, backoff))`, `FlexibleTimeWindow.Mode = OFF`, `ActionAfterCompletion = DELETE`, universal SQS SendMessage target ARN, execution role ARN, and the fresh dedup id as the schedule name; return an error wrapping `errors.ErrRetryScheduleCreate` on failure
    - _Requirements: 2.6, 3.1, 3.2, 3.3, 3.4, 3.7, 9.1, 9.2, 9.5_

  - [x] 5.2 Write property and example tests for the scheduler
    - **Property 5: Re-published retry preserves body and FIFO identity**
    - **Validates: Requirements 2.6, 3.2, 3.3, 3.4**
    - Create `consumer/retry_scheduler_test.go`; generate FIFO messages (body, group id, user attributes up to and beyond ten) and next counts; decode the universal-target `Input` JSON and assert body, group id, `retry_count`, the ten-attribute cap, and dedup-id distinctness; tag: `// Feature: fifo-scheduled-retry, Property 5: ...`
    - Add a deterministic example test asserting `CreateScheduleInput` has `ActionAfterCompletion = DELETE`, `FlexibleTimeWindow.Mode = OFF`, the universal-target ARN, and the execution role (Req 9.1), and a case asserting a failing `CreateSchedule` returns an error wrapping `errors.ErrRetryScheduleCreate` (Req 3.7, 9.5)
    - _Requirements: 9.1, 3.7, 9.5_

  - [x] 5.3 Implement `dlqPublisher`
    - Create `consumer/dlq_publisher.go` owning an `SQSClient` and the resolved DLQ destination (queue URL; group-id reuse + fresh dedup-id generator for a FIFO DLQ)
    - Send the original body, all user attributes, and the final `retry_count` via `SendMessage`; return an error wrapping `errors.ErrDLQPublish` on failure
    - _Requirements: 5.1, 5.2, 5.4_

  - [x] 5.4 Write unit tests for the DLQ publisher
    - Create `consumer/dlq_publisher_test.go` asserting the `SendMessageInput` carries body, all user attributes, and the final `retry_count`, includes a FIFO group id and a fresh dedup id, and that a failing `SendMessage` returns an error wrapping `errors.ErrDLQPublish`
    - _Requirements: 5.1, 5.2, 5.4_

- [x] 6. Metrics hooks
  - [x] 6.1 Add metric types and consumer options
    - In `consumer/options.go` add `SuccessMetric`, `RetryMetric`, and `DeadLetterMetric` func types and `WithSuccessMetric`, `WithRetryMetric`, `WithDeadLetterMetric` options (nil-ignored, following the existing `DLQMetric`/`WithDLQMetric` pattern)
    - Add a `WithSchedulerClient(SchedulerClient)` option and the corresponding `Consumer` field for wiring the Scheduled model collaborators
    - _Requirements: 8.1, 8.2, 8.3, 8.4_

  - [x] 6.2 Add the recover-guarded metric emitter
    - Create `consumer/metrics.go` with a helper that invokes a metric hook (when non-nil) inside a `recover`, logging and swallowing any panic so the message outcome still completes without retention or reprocessing
    - _Requirements: 8.4, 8.5_

  - [x] 6.3 Write unit tests for the metric emitter
    - Create `consumer/metrics_test.go` asserting the emitter is a no-op when the hook is nil, invokes the hook with the route name when set, and recovers and logs a panicking hook without propagating
    - _Requirements: 8.4, 8.5_

- [x] 7. Scheduled-aware visibility management
  - [x] 7.1 Add scheduled-aware mode to `visibilityManager`
    - In `consumer/visibility.go` add a scheduled-aware flag/constructor path so the `run` loop selects only on the ticker, `dispatchSignal`, and `ctx.Done()` — never on `backoffSignal` — under the Scheduled model, leaving the buffered backoff channel unconsumed so `msg.Backoff` never blocks and no backoff-driven `ChangeMessageVisibility` is issued
    - Keep the goroutine tracked by the dispatcher wait group and guaranteed to return
    - _Requirements: 3.9_

  - [x] 7.2 Write unit test for scheduled-aware visibility
    - In `consumer/visibility_test.go` add a deterministic case (using `sleepInterval`) asserting that under scheduled-aware mode a backoff signal never triggers a `ChangeMessageVisibility` call and the goroutine still returns on dispatch/cancel
    - _Requirements: 3.9_

- [x] 8. Dispatcher scheduled-retry processing
  - [x] 8.1 Implement `processScheduled` and wire the branch into `process`
    - In `consumer/dispatch.go` add a `retryModel`-aware split: the Visibility path stays unchanged; the Scheduled path delegates to `processScheduled(ctx, msg, err)`
    - `processScheduled` calls `msg.Dispatch()` immediately, treats handler error OR `msg.BackedOff()` as failure, computes `current = parseRetryCount(...)` and `next = current + 1`, takes the schedule path when `next <= MaxRetryCount` (schedule then delete then retry metric) and the DLQ path when `next > MaxRetryCount` (publish then delete then dead-letter metric), retaining-and-logging on any orchestration failure without deleting; on success deletes the message and emits the success metric, performing no success-side publishing (that is the handler's responsibility)
    - Add dispatcher fields for `retryScheduler`, `dlqPublisher`, the three metric hooks, and the `ScheduledRetryConfig`, and construct them in `newDispatcher` only for the Scheduled model
    - _Requirements: 2.5, 3.1, 3.5, 3.6, 3.8, 3.9, 5.1, 5.3, 6.1, 6.2, 6.3, 6.4, 6.5, 7.1, 7.2, 7.3, 8.1, 8.2, 8.3, 11.1, 11.2, 11.4_

  - [x] 8.2 Write property test for retry-count increment
    - **Property 3: Retry count increments by one on failure**
    - **Validates: Requirements 2.5**
    - Create `consumer/nextcount_test.go`; generate `n >= 0`; assert the next count computed on failure is `n + 1`; tag: `// Feature: fifo-scheduled-retry, Property 3: ...`

  - [x] 8.3 Write property test for the retry-versus-DLQ decision
    - **Property 4: Retry-versus-DLQ decision follows the threshold**
    - **Validates: Requirements 3.1, 3.8, 5.1**
    - Create `consumer/decision_test.go`; generate next count and `Max_Retry_Count` with a fake scheduler and fake DLQ; assert the schedule path is taken when `next <= max`, the DLQ path when `next > max`, and never both; tag: `// Feature: fifo-scheduled-retry, Property 4: ...`

  - [x] 8.4 Write property test for the delete-after-orchestration invariant
    - **Property 7: A message is deleted only after successful orchestration**
    - **Validates: Requirements 3.5, 3.7, 5.3, 5.4, 6.1, 6.2, 6.4, 9.5, 11.2**
    - Create `consumer/preservation_test.go` as a model-based test; generate handler outcome (success/error/backoff) and fake scheduler/DLQ success/failure; assert `DeleteMessage` is called iff the handler succeeded or a schedule/DLQ publish succeeded, and otherwise no delete occurs and a wrapped error is logged; tag: `// Feature: fifo-scheduled-retry, Property 7: ...`

  - [x] 8.5 Write property test for backoff-as-failure
    - **Property 8: Backoff requests are treated as failures**
    - **Validates: Requirements 3.9**
    - Create `consumer/backoff_failure_test.go`; generate handlers that call `Backoff` under the Scheduled model; assert no `ChangeMessageVisibility` call is issued and the schedule/DLQ decision is applied; tag: `// Feature: fifo-scheduled-retry, Property 8: ...`

  - [x] 8.6 Write property test for per-outcome metrics
    - **Property 10: Exactly one metric per outcome when enabled**
    - **Validates: Requirements 8.1, 8.2, 8.3, 8.4**
    - Create `consumer/metrics_outcome_test.go`; generate each outcome with metrics enabled and disabled using fake counters; assert exactly one matching metric labeled with the route name when enabled and zero when disabled; tag: `// Feature: fifo-scheduled-retry, Property 10: ...`

  - [x] 8.7 Write property test for success-without-publishing
    - **Property 9: Success deletes without publishing**
    - **Validates: Requirements 7.1, 7.2**
    - Create `consumer/success_test.go`; generate successful handler outcomes under the Scheduled model; assert `DeleteMessage` is called and that the consumer issues no success-side publish (it holds no publisher client); tag: `// Feature: fifo-scheduled-retry, Property 9: ...`

- [x] 9. Consumer wiring
  - [x] 9.1 Wire collaborators into `Consumer.Run` for the Scheduled model
    - In `consumer/consumer.go` inspect `route.RetryModel()`: for the Visibility model keep the existing path untouched and construct no scheduler/DLQ client; for the Scheduled model build the visibility manager in scheduled-aware mode and construct the dispatcher with the scheduler client, resolved `ScheduledRetryConfig`, and metric hooks from the consumer options
    - _Requirements: 1.3, 1.5, 1.6, 7.1, 10.1, 10.2, 10.3, 10.4_

  - [x] 9.2 Write backward-compatibility and wiring example tests
    - In `consumer/consumer_test.go` add cases asserting: a Visibility-model route constructs with no scheduler/DLQ configuration and never constructs a scheduler/DLQ client (Req 10.2, 10.3); a Visibility-model backoff extends visibility and leaves the message (Req 10.1, 10.4); a Scheduled-model route with a missing client surfaces the appropriate error and does not begin consuming (Req 1.5); confirm existing consumer/dispatch/visibility tests still pass unchanged
    - _Requirements: 1.5, 10.1, 10.2, 10.3, 10.4_

- [x] 10. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [x] 11. End-to-end integration
  - [x] 11.1 Write LocalStack integration tests for the Scheduled model
    - In `consumer/consumer_integration_test.go` (mirroring the existing integration tests and `docker-compose.integration.yml`) add 1–3 representative cases: a Scheduled-model route whose handler fails creates a schedule and deletes the original message, and an exhausted message lands in the configured DLQ
    - _Requirements: 3.1, 3.5, 5.1, 5.3_

- [x] 12. Final checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [x] 13. Documentation and runnable examples
  - [x] 13.1 Document the Scheduled Retry model in the root README
    - In `README.md` add a section for the per-route Scheduled Retry model covering: the `RetryModel` selection (Visibility default vs Scheduled); the router options `WithRetryModel` and `WithScheduledRetry` with its sub-options `WithSchedulerIdentity`, `WithScheduledDLQ`, `WithMaxRetryCount`, and `WithBackoff`; the consumer `WithSchedulerClient` option and the metric hooks `WithSuccessMetric` / `WithRetryMetric` / `WithDeadLetterMetric`
    - Document the required AWS resources and IAM permissions: `scheduler:CreateSchedule`, `iam:PassRole` for the execution role, and `sqs:SendMessage` to the DLQ; document the explicit-deduplication requirement on the FIFO Entry_Queue (the re-published retry carries an explicit `MessageDeduplicationId`, so the queue must not rely on content-based deduplication)
    - Document the accepted tradeoffs: at-least-once delivery, in-group ordering not preserved for retried messages, and handler-owned success publishing (the library only deletes on success and emits a success metric)
    - _Requirements: 1.1, 1.2, 3.1, 3.10, 4.1, 5.5, 8.1, 8.2, 8.3, 9.1, 9.3, 7.2, 11.1, 11.2_

  - [x] 13.2 Update the examples README to list the Scheduled Retry example
    - In `examples/README.md` add the new example to the examples table and add a short "FIFO Scheduled Retry" subsection describing what it shows (a FIFO route using `WithScheduledRetry` with scheduler identity, DLQ, max retry count, backoff, the wired `WithSchedulerClient`, and a failing handler that exercises the scheduled-retry path), matching the existing per-example write-ups
    - Note the additional local resources the example expects (Entry_Queue with explicit dedup and a DLQ) consistently with the existing "not provisioned by Terraform" notes
    - _Requirements: 1.1, 3.1, 5.5, 9.3_

  - [x] 13.3 Update package doc comments for the Scheduled Retry model
    - In `router/doc.go` describe the Scheduled Retry model and the new exported symbols (`RetryModel`, `VisibilityRetryModel`, `ScheduledRetryModel`, `WithRetryModel`, `WithScheduledRetry`, `WithSchedulerIdentity`, `WithScheduledDLQ`, `WithMaxRetryCount`, `WithBackoff`, `ScheduledRetryConfig`), in English, per the Go steering
    - In `consumer/doc.go` describe the Scheduled Retry consumption path and the new exported symbols (`SchedulerClient`, `WithSchedulerClient`, `SuccessMetric` / `RetryMetric` / `DeadLetterMetric`, `WithSuccessMetric` / `WithRetryMetric` / `WithDeadLetterMetric`), noting success publishing is the handler's responsibility
    - _Requirements: 1.1, 3.1, 5.5, 8.1, 9.3_

  - [x] 13.4 Add the FIFO Scheduled Retry example program
    - Create `examples/fifo-scheduled-retry/main.go` as a standalone `package main` mirroring the existing `examples/fifo/main.go` and `examples/basic/main.go` layout and style: build the AWS connection with `conn`, create the SQS and EventBridge Scheduler clients, declare a FIFO route with `router.WithScheduledRetry(...)` setting scheduler identity (`WithSchedulerIdentity`), DLQ destination (`WithScheduledDLQ`), max retry count (`WithMaxRetryCount`), and backoff (`WithBackoff`), wire the scheduler client through the consumer `WithSchedulerClient` option, and register a handler that returns an error to exercise the scheduled-retry path
    - Optionally wire the `WithSuccessMetric` / `WithRetryMetric` / `WithDeadLetterMetric` hooks to log each outcome; keep the program minimal and idiomatic and ensure it builds with `go build ./...`
    - _Requirements: 1.1, 3.1, 4.1, 5.5, 8.2, 8.3, 9.1, 9.3_

  - [x] 13.5 Add the example Makefile target
    - In `examples/Makefile` add a `run-fifo-scheduled-retry` target (with a `##` help comment and included in the `.PHONY` list) that runs `$(GO) run ./fifo-scheduled-retry`, matching the existing `run-fifo` target style so the new example can be built and run
    - _Requirements: 3.1, 9.3_

  - [x] 13.6 Provision the FIFO Scheduled Retry example resources with Terraform and close the producer loop
    - In `examples/terraform/main.tf` add the resources the `examples/fifo-scheduled-retry` example expects, mirroring the existing resource/style conventions in that file (LocalStack-targeted provider, matching naming/variables patterns):
      - A FIFO Entry_Queue (e.g. `example-scheduled.fifo`) created with EXPLICIT deduplication (`content_based_deduplication = false`) so a re-published retry carrying an explicit `MessageDeduplicationId` is not discarded by content-based deduplication
      - A FIFO DLQ (e.g. `example-scheduled-dlq.fifo`) for messages whose retry count exceeds the maximum
      - An IAM execution role EventBridge Scheduler assumes, with a trust policy allowing `scheduler.amazonaws.com` and permissions for `sqs:SendMessage` to the Entry_Queue (and, per the design, the consumer identity needs `scheduler:CreateSchedule` and `iam:PassRole`); document the LocalStack Community caveat that EventBridge Scheduler may be unavailable
    - Add corresponding variables to `examples/terraform/variables.tf` (queue names, role name) following the existing variable conventions, and expose the Entry_Queue ARN, DLQ URL, and execution role ARN via `examples/terraform/outputs.tf` so the example's placeholder ARNs/URLs can be filled in
    - In `examples/terraform/main.tf` add a FIFO SNS topic (e.g. `example-scheduled-topic.fifo`, `fifo_topic = true`, `content_based_deduplication` set as appropriate) and subscribe the Scheduled Retry Entry_Queue to it with `raw_message_delivery = true` (so the body is delivered verbatim and native message attributes such as a future `retry_count` are preserved), plus the SQS queue policy allowing that topic to `sqs:SendMessage` to the Entry_Queue — mirroring the existing standard/`fifo` topic→queue subscription and queue-policy wiring conventions; add the topic name variable to `examples/terraform/variables.tf` and expose the topic ARN via `examples/terraform/outputs.tf`
    - In `examples/producer/main.go` add a publish path that produces one or more FIFO messages to the new Scheduled Retry topic (reusing the existing FIFO ID-generator wiring so `MessageGroupId`/`MessageDeduplicationId` are auto-generated via `idgen.NewRandom`), following the style of the existing `publishFIFOSingle`/`publishFIFOBatch` functions, so running the producer seeds the Entry_Queue (`example-scheduled.fifo`) and the scheduled-retry consumer has input to process; add the topic name const alongside the existing topic consts
    - Update `examples/README.md` (and the `examples/Makefile` run notes if relevant) so the FIFO Scheduled Retry example documents the full cycle: provision the resources, start the scheduled-retry consumer, then run the producer to seed messages into the Entry_Queue and observe the scheduled-retry/DLQ path
    - Update the example resource notes in `examples/README.md` and the example program comments if needed so they reference the now-provisioned resources consistently
    - _Requirements: 1.1, 3.1, 3.10, 5.5, 9.1, 9.3_

## Notes

- Tasks marked with `*` are optional test sub-tasks and can be skipped for a faster MVP; core implementation sub-tasks are never optional.
- Each task references specific requirement sub-clauses for traceability, and each property-based test task references its numbered design property.
- Property tests use `pgregory.net/rapid`, run 100+ iterations, live in `_test` packages, and carry the `// Feature: fifo-scheduled-retry, Property {n}: {property text}` tag.
- Checkpoints (tasks 10 and 12) provide incremental validation points and are not part of the dependency graph.
- The Visibility model path is preserved unchanged; no scheduler, DLQ, or SNS client is constructed unless a route selects the Scheduled model.
- Task group 13 adds documentation and a runnable example (root README, examples README, `router`/`consumer` package docs, the `examples/fifo-scheduled-retry` program, and the example Makefile target). These are core (non-optional) repo-file tasks that depend on the public API and wiring being complete, so they run in the late waves.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2", "2.1", "3.1"] },
    { "id": 1, "tasks": ["1.3", "2.2", "2.4", "3.2", "4.1", "7.1"] },
    { "id": 2, "tasks": ["1.4", "1.5", "2.3", "2.5", "4.2", "7.2"] },
    { "id": 3, "tasks": ["4.3", "5.1", "5.3", "6.1", "6.2"] },
    { "id": 4, "tasks": ["5.2", "5.4", "6.3", "8.1"] },
    { "id": 5, "tasks": ["8.2", "8.3", "8.4", "8.5", "8.6", "8.7", "9.1"] },
    { "id": 6, "tasks": ["9.2", "13.1", "13.3", "13.4"] },
    { "id": 7, "tasks": ["11.1", "13.2", "13.5", "13.6"] }
  ]
}
```

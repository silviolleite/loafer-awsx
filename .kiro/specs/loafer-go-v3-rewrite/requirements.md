# Requirements Document

## Introduction

This document specifies the requirements for loafer-awsx (formerly the loafer-go v3 rewrite), a major rewrite of the AWS SQS/SNS message processing library in Go delivered as its own module `github.com/silviolleite/loafer-awsx`. The release modernizes the architecture following patterns established in loafer-natsx, introducing a clean package structure (conn, producer, router, consumer, broker, logger, typed), generic type-safe handlers, pluggable middleware pipelines, built-in Prometheus metrics, and comprehensive observability via OpenTelemetry. All existing loafer-go v2 features are preserved but redesigned for improved ergonomics, testability, and performance. Backward compatibility with loafer-go v2 is NOT required.

## Glossary

- **Broker**: The top-level orchestrator that manages the lifecycle of multiple Consumer instances, providing fail-fast behavior and coordinated shutdown with no goroutine leaks.
- **Consumer**: A component that polls messages from a single SQS queue, manages visibility timeouts, and dispatches messages to a worker pool.
- **Router**: A configuration unit that binds a queue name to a handler function, middleware chain, and route-specific options (worker pool size, visibility timeout, run mode).
- **Producer**: A component that publishes messages to AWS SNS topics, supporting both standard and FIFO topics with single and batch operations.
- **Handler**: A function with signature `func(ctx context.Context, msg Message) error` that processes a single message.
- **Typed_Handler**: A generic wrapper `Handler[T]` with signature `func(ctx context.Context, msg T) error` that abstracts message decoding using a Codec interface.
- **Middleware**: A function that wraps a Handler, providing cross-cutting concerns such as logging, metrics, and tracing. Signature: `func(Handler) Handler`.
- **Codec**: A generic interface `Codec[T]` with methods `Encode(T) ([]byte, error)` and `Decode([]byte) (T, error)` used by Typed_Handler for type-safe message deserialization.
- **Message**: An interface representing a received SQS message with methods for decoding, attribute access, metadata retrieval, backoff, and dispatch control.
- **Visibility_Timeout_Manager**: The mechanism that automatically extends the SQS message visibility timeout while processing is ongoing, preventing duplicate delivery.
- **Backoff**: A mechanism to delay message redelivery by changing the visibility timeout of a message instead of deleting it from the queue.
- **Run_Mode**: The message dispatch strategy: Parallel (random worker assignment) or PerGroupID (hash-based affinity using MessageGroupId and custom fields).
- **Worker_Pool**: A fixed-size pool of goroutines per route that concurrently process messages from the assigned queue.
- **Conn**: The package responsible for establishing and configuring AWS SDK v2 connections with credentials, region, endpoint, retry policies, and profile support.
- **Logger**: The standard library `*slog.Logger` type (from `log/slog`) used for structured, leveled logging throughout the library. The library does NOT define a custom logger interface; it accepts `*slog.Logger` directly.
- **Metrics**: Built-in Prometheus instrumentation exposing messages_received_total, messages_processed_total, messages_errors_total, message_processing_duration_seconds, and messages_inflight gauge per route.
- **DLQ**: Dead Letter Queue observability. The actual redrive of messages to the DLQ is performed by AWS SQS's native redrive policy (RedrivePolicy / maxReceiveCount configured on the source queue). The library does NOT move, publish, or delete messages for DLQ purposes; it only observes when a message is exhausted (about to be redriven by SQS) and emits observability signals (an Error-level log, the `loafer_messages_dlq_total` metric, and an optional `OnDLQ` callback).
- **Functional_Options**: The configuration pattern where all components accept variadic option functions for dynamic, composable, and backward-compatible configuration.
- **IDGen**: The package responsible for generating MessageGroupId and MessageDeduplicationId values, supporting key-based deterministic generation and random (UUID-based) generation.

## Requirements

### Requirement 1: Package Architecture

**User Story:** As a developer, I want the library organized into cohesive, single-responsibility packages, so that I can import only what I need and navigate the codebase easily.

#### Acceptance Criteria

1. THE Library SHALL organize code into the following top-level packages: conn, producer, router, consumer, broker, logger, typed, middleware, and idgen.
2. THE Library SHALL use the module path `github.com/silviolleite/loafer-awsx`.
3. THE Library SHALL require Go 1.24 or later as the minimum supported version.
4. THE Library SHALL use aws-sdk-go-v2 as the sole AWS SDK dependency.
5. THE conn Package SHALL expose a function to create an AWS SDK v2 configuration from functional options (region, credentials, endpoint, profile, retry count).

### Requirement 2: Connection Management

**User Story:** As a developer, I want a clean connection package to configure AWS clients, so that I can reuse a single configuration across producers and consumers.

#### Acceptance Criteria

1. THE conn Package SHALL accept configuration through Functional_Options including region, access key, secret key, session token, profile, endpoint override, and retry count.
2. THE conn Package SHALL return a configured `aws.Config` object usable by both SQS and SNS clients.
3. WHEN no retry count is provided, THE conn Package SHALL default to 10 retry attempts.
4. WHEN no region is provided, THE conn Package SHALL return an error indicating the region is required.
5. WHEN an endpoint override is provided, THE conn Package SHALL configure the AWS SDK to use that endpoint for all requests.

### Requirement 3: Router and Route Configuration

**User Story:** As a developer, I want to define routes declaratively with functional options, so that I can configure queue consumption without coupling to the consumption engine.

#### Acceptance Criteria

1. THE router Package SHALL provide a Route struct configured via Functional_Options for queue name, handler, worker pool size, max messages, wait time seconds, visibility timeout, extension limit, run mode, custom group fields, and middleware chain.
2. WHEN no worker pool size is specified, THE Route SHALL default to 5 workers.
3. WHEN no visibility timeout is specified, THE Route SHALL default to 30 seconds.
4. WHEN no max messages is specified, THE Route SHALL default to 10 messages per receive call.
5. WHEN no wait time seconds is specified, THE Route SHALL default to 10 seconds for long polling.
6. WHEN a visibility timeout of 11 seconds or less is specified, THE Route SHALL set the visibility timeout to 11 seconds as the minimum allowed value.
7. THE Route SHALL accept a middleware chain applied in order to the handler before message processing.

### Requirement 4: Consumer and Message Polling

**User Story:** As a developer, I want a consumer that efficiently polls SQS and dispatches messages to workers, so that I can process messages with high throughput.

#### Acceptance Criteria

1. THE Consumer SHALL receive messages from the configured SQS queue using batch receive with the route-configured max messages and wait time seconds.
2. THE Consumer SHALL request all message attributes and all system attributes in every receive call.
3. THE Consumer SHALL dispatch each received message to a Worker_Pool goroutine for processing.
4. WHEN the handler returns nil, THE Consumer SHALL delete the message from the queue.
5. WHEN the handler returns an error, THE Consumer SHALL log the error with message body, group ID, and receipt handle, and leave the message in the queue for redelivery.
6. WHEN a message is backed off, THE Consumer SHALL change the message visibility timeout to the backoff duration instead of deleting it.
7. THE Consumer SHALL support Parallel Run_Mode where messages are assigned to workers randomly.
8. THE Consumer SHALL support PerGroupID Run_Mode where messages are assigned to workers based on a hash of the MessageGroupId and optional custom group fields.

### Requirement 5: Visibility Timeout Management

**User Story:** As a developer, I want automatic visibility timeout extension, so that long-running message processing does not cause duplicate delivery.

#### Acceptance Criteria

1. WHILE a message is being processed, THE Visibility_Timeout_Manager SHALL periodically extend the message visibility timeout before it expires.
2. THE Visibility_Timeout_Manager SHALL calculate the sleep interval as (visibility timeout minus 10 seconds).
3. WHEN the first extension tick fires, THE Visibility_Timeout_Manager SHALL set the visibility timeout to the configured route visibility timeout value.
4. WHEN subsequent extension ticks fire, THE Visibility_Timeout_Manager SHALL increment the visibility timeout by the configured route visibility timeout value.
5. WHEN the extension limit is reached, THE Visibility_Timeout_Manager SHALL stop extending and allow the message to become visible again.
6. WHEN the handler completes (dispatch signal), THE Visibility_Timeout_Manager SHALL stop the extension loop immediately.
7. WHEN a backoff signal is received, THE Visibility_Timeout_Manager SHALL set the visibility timeout to the backoff duration and stop the extension loop.
8. IF a ChangeMessageVisibility API call fails, THEN THE Visibility_Timeout_Manager SHALL log the error and continue operation.
9. THE Visibility_Timeout_Manager SHALL enforce a maximum visibility timeout of 43200 seconds (12 hours) as per AWS SQS limits.
10. THE Visibility_Timeout_Manager SHALL enforce a minimum visibility timeout of 0 seconds.

### Requirement 6: Message Interface

**User Story:** As a developer, I want a rich message interface, so that I can access all relevant message data and control message lifecycle.

#### Acceptance Criteria

1. THE Message interface SHALL provide a Decode method that unmarshals the raw message body into a supplied output using JSON.
2. THE Message interface SHALL provide a DecodeMessage method that unmarshals the inner SNS Message field into a supplied output using JSON.
3. THE Message interface SHALL provide an Attribute method that returns a single custom attribute by key.
4. THE Message interface SHALL provide an Attributes method that returns all custom attributes as a string map.
5. THE Message interface SHALL provide a SystemAttributeByKey method that returns a single system attribute by key.
6. THE Message interface SHALL provide a SystemAttributes method that returns all system attributes as a string map.
7. THE Message interface SHALL provide a Metadata method that returns message metadata as a string map.
8. THE Message interface SHALL provide an Identifier method that returns the message receipt handle.
9. THE Message interface SHALL provide a Body method that returns the raw message body as a byte slice.
10. THE Message interface SHALL provide a Message method that returns the inner message content as a string.
11. THE Message interface SHALL provide a TimeStamp method that returns the message timestamp.
12. THE Message interface SHALL provide a Dispatch method that signals processing completion.
13. THE Message interface SHALL provide a Backoff method that accepts a duration and delays message redelivery by that amount.
14. THE Message interface SHALL provide a BackedOff method that returns whether the message was backed off.

### Requirement 7: Broker Orchestration

**User Story:** As a developer, I want a broker that manages all consumers with coordinated lifecycle, so that my application starts and stops cleanly.

#### Acceptance Criteria

1. THE Broker SHALL accept one or more Route configurations and start a Consumer for each route.
2. WHEN no routes are registered, THE Broker SHALL return an error immediately.
3. THE Broker SHALL start all consumers concurrently and wait for all to complete before returning.
4. WHEN the context is canceled, THE Broker SHALL signal all consumers to stop polling and drain in-flight messages.
5. WHEN a consumer fails during configuration (queue URL resolution), THE Broker SHALL log the error and return it immediately (fail-fast behavior).
6. THE Broker SHALL accept configuration through Functional_Options including a `*slog.Logger`, retry timeout for transient receive errors, and global middleware.
7. WHEN a receive error occurs, THE Broker SHALL wait the configured retry timeout before retrying (default 5 seconds).
8. THE Broker SHALL guarantee no goroutine leaks after shutdown completes.

### Requirement 8: SNS Producer

**User Story:** As a developer, I want to publish messages to SNS topics (standard and FIFO), so that I can emit events from my application.

#### Acceptance Criteria

1. THE Producer SHALL publish a single message to a specified SNS topic ARN.
2. THE Producer SHALL publish a batch of up to 10 messages to a specified SNS topic ARN.
3. WHEN publishing to a FIFO topic, THE Producer SHALL include the MessageGroupId in the publish request.
4. WHEN a deduplication ID is provided, THE Producer SHALL include the MessageDeduplicationId in the publish request.
5. WHEN message attributes are provided, THE Producer SHALL include them as SNS MessageAttributes with String data type.
6. WHEN a nil or empty publish input is provided, THE Producer SHALL return an error indicating the input is empty.
7. WHEN the batch size exceeds 10 messages, THE Producer SHALL return an error indicating the maximum batch size is exceeded.
8. THE Producer SHALL return the message ID on successful single publish.
9. THE Producer SHALL return both successful and failed entries on batch publish.
10. THE Producer SHALL provide a helper function to build a topic ARN from region, account ID, and topic name.
11. THE Producer SHALL accept configuration through Functional_Options.

### Requirement 9: Typed Handlers (Generic Type-Safe Processing)

**User Story:** As a developer, I want generic type-safe handlers, so that I can work with strongly-typed messages without manual decoding boilerplate.

#### Acceptance Criteria

1. THE typed Package SHALL define a `Codec[T]` interface with `Encode(T) ([]byte, error)` and `Decode([]byte) (T, error)` methods.
2. THE typed Package SHALL provide a `JSONCodec[T]` implementation that serializes and deserializes using JSON.
3. THE typed Package SHALL provide a `WrapHandler[T]` adapter function that converts a `func(ctx context.Context, msg T) error` into a standard Handler using a supplied Codec.
4. WHEN the codec Decode fails, THE WrapHandler adapter SHALL return the decode error to the consumer for standard error handling.
5. FOR ALL valid typed messages, encoding then decoding SHALL produce an equivalent object (round-trip property).
6. THE typed Package SHALL provide a typed Producer[T] wrapper that encodes a value of type T before publishing via the underlying Producer.

### Requirement 10: Middleware Pipeline

**User Story:** As a developer, I want pluggable middleware, so that I can add cross-cutting concerns like tracing, metrics, and logging without modifying handler code.

#### Acceptance Criteria

1. THE middleware Package SHALL define a Middleware type as `func(Handler) Handler`.
2. THE middleware Package SHALL provide a Chain function that composes multiple middlewares into a single Middleware, applied in order (first middleware is outermost).
3. THE middleware Package SHALL provide a Recovery middleware that catches panics in handlers, logs the panic, and returns an error instead of crashing.
4. THE middleware Package SHALL provide a Logging middleware that logs message receipt, processing duration, and outcome (success or error) for each message.
5. THE middleware Package SHALL provide an OpenTelemetry middleware that creates a span for each message processing, recording message attributes as span attributes and errors as span events.
6. THE middleware Package SHALL provide a Metrics middleware that records Prometheus counters (messages_received_total, messages_processed_total, messages_errors_total), a histogram (message_processing_duration_seconds), and a gauge (messages_inflight) labeled by route name.
7. WHEN middleware is configured at both broker level and route level, THE Broker SHALL apply broker-level middleware first (outermost) and route-level middleware second (innermost, closer to handler).

### Requirement 11: Logging with the Standard Library slog

**User Story:** As a developer, I want the library to use the standard library `*slog.Logger` as its logging type, so that I can plug in my existing slog-based logging (including zap, zerolog, or logrus via their slog bridges) without writing a custom adapter.

#### Acceptance Criteria

1. THE Library SHALL use `*slog.Logger` (from the standard library `log/slog` package) as its logging type throughout all packages, and SHALL NOT define a custom logger interface.
2. THE logger Package SHALL provide a constructor that returns a default `*slog.Logger` writing structured, leveled output to stdout using a `slog.TextHandler` on `os.Stdout`.
3. THE logger Package SHALL provide a constructor that returns a no-op `*slog.Logger` backed by a discard handler for silent operation.
4. THE `*slog.Logger` SHALL be accepted as a Functional_Option on the Broker via a `WithLogger` option.
5. WHEN no logger is provided, THE Broker SHALL default to the stdout `*slog.Logger`.
6. WHERE a `*slog.Logger` is produced by a third-party bridge (for example, zap via `zapslog` or zerolog via a slog handler), THE Library SHALL accept and use that logger directly without requiring an additional adapter.

### Requirement 12: Prometheus Metrics

**User Story:** As a developer, I want built-in Prometheus metrics, so that I can monitor message processing performance and error rates without custom instrumentation.

#### Acceptance Criteria

1. WHEN the Metrics middleware is enabled, THE Middleware SHALL register a counter `loafer_messages_received_total` labeled by route.
2. WHEN the Metrics middleware is enabled, THE Middleware SHALL register a counter `loafer_messages_processed_total` labeled by route and status (success, error).
3. WHEN the Metrics middleware is enabled, THE Middleware SHALL register a counter `loafer_messages_errors_total` labeled by route.
4. WHEN the Metrics middleware is enabled, THE Middleware SHALL register a histogram `loafer_message_processing_duration_seconds` labeled by route.
5. WHEN the Metrics middleware is enabled, THE Middleware SHALL register a gauge `loafer_messages_inflight` labeled by route.
6. THE Metrics middleware SHALL increment the inflight gauge when processing begins and decrement it when processing completes.

### Requirement 13: Graceful Shutdown

**User Story:** As a developer, I want coordinated graceful shutdown, so that in-flight messages complete processing before the application terminates.

#### Acceptance Criteria

1. WHEN the context passed to the Broker is canceled, THE Broker SHALL stop all consumers from polling new messages.
2. WHEN shutdown is initiated, THE Broker SHALL wait for all in-flight messages to complete processing, bounding that wait only when a shutdown timeout is explicitly configured; when no shutdown timeout is configured, THE Broker SHALL wait indefinitely for in-flight messages to complete.
3. WHEN all in-flight messages complete, THE Broker SHALL return nil from the Run method.
4. THE Broker SHALL close all internal channels and stop all goroutines upon shutdown completion.
5. THE Broker SHALL guarantee that no goroutine leaks exist after Run returns (verifiable via goleak in tests).

### Requirement 14: Dead Letter Queue Observability

**User Story:** As a developer, I want DLQ observability, so that when AWS SQS redrives an exhausted message to the dead letter queue via its native redrive policy, I am notified through logs, metrics, and an optional callback without the library duplicating the redrive behavior SQS already provides.

#### Acceptance Criteria

1. THE Consumer SHALL NOT move, publish, or delete messages for DLQ purposes; redrive to the dead letter queue SHALL be delegated to the AWS SQS native redrive policy.
2. WHERE a DLQ route option is configured, THE Consumer SHALL track the ApproximateReceiveCount system attribute of each message.
3. WHERE a DLQ route option is configured, WHEN the handler returns an error AND the ApproximateReceiveCount is greater than or equal to the configured maxReceiveCount, THE Consumer SHALL treat the message as exhausted and emit DLQ observability signals.
4. WHEN a message is treated as exhausted, THE Consumer SHALL emit a log at Error level including the message identifier, queue name, and receive count.
5. WHERE the Metrics middleware is enabled, WHEN a message is treated as exhausted, THE Middleware SHALL increment the counter `loafer_messages_dlq_total` labeled by route.
6. WHERE an `OnDLQ` callback is configured, WHEN a message is treated as exhausted, THE Consumer SHALL invoke the `OnDLQ` callback with the message context.
7. WHERE a DLQ route option is configured, THE Consumer SHALL leave the message in the source queue so that AWS SQS performs the redrive per its own policy.
8. THE DLQ route option SHALL NOT require a target ARN, since the redrive destination is owned by the AWS SQS queue redrive configuration.
9. THE DLQ route option SHALL accept the maxReceiveCount and an optional `OnDLQ` callback via Functional_Options, and the maxReceiveCount SHALL mirror the source queue's redrive policy.

### Requirement 15: Functional Options Pattern

**User Story:** As a developer, I want all components configured via functional options, so that configuration is composable, type-safe, and maintainable.

#### Acceptance Criteria

1. THE Broker SHALL accept configuration exclusively through Functional_Options (no exported config struct with public fields).
2. THE Route SHALL accept configuration exclusively through Functional_Options.
3. THE Producer SHALL accept configuration exclusively through Functional_Options.
4. THE conn Package SHALL accept configuration exclusively through Functional_Options.
5. WHEN an invalid option value is provided, THE component SHALL return a descriptive error at creation time rather than silently accepting invalid configuration.

### Requirement 16: Concurrency Safety and Performance

**User Story:** As a developer, I want the library to be concurrent-safe and leak-free, so that I can use it in production with confidence.

#### Acceptance Criteria

1. THE Library SHALL have zero data races as verified by the Go race detector in all tests.
2. THE Library SHALL have zero goroutine leaks as verified by go.uber.org/goleak in test teardown.
3. THE Broker SHALL start and stop consumers without goroutine leaks under repeated start/stop cycles.
4. THE Consumer Worker_Pool SHALL process messages concurrently without blocking the polling loop.
5. THE Producer SHALL be safe for concurrent use from multiple goroutines.

### Requirement 17: Testing Standards

**User Story:** As a developer, I want comprehensive test coverage with property-based tests, so that I can trust the library's correctness under all inputs.

#### Acceptance Criteria

1. THE Library SHALL maintain a minimum of 95% test coverage across all packages.
2. THE Library SHALL use pgregory.net/rapid for property-based tests on all codec, serialization, and routing logic.
3. THE Library SHALL use go.uber.org/goleak in TestMain or test teardown to detect goroutine leaks.
4. THE Library SHALL run all tests with the `-race` flag enabled.
5. THE typed Package SHALL include a round-trip property test verifying `Decode(Encode(x)) == x` for JSONCodec.
6. THE Consumer SHALL include property tests verifying that PerGroupID routing consistently assigns messages with the same group key to the same worker index.

### Requirement 18: Build Tooling and Developer Experience

**User Story:** As a developer, I want a single `make configure` command to set up my development environment, so that I can contribute quickly.

#### Acceptance Criteria

1. THE Makefile SHALL provide a `configure` target that installs all required development tools (goimports, golangci-lint, fieldalignment, lefthook, mockery, rapid).
2. THE Makefile SHALL provide a `test` target that runs all tests with race detection and coverage reporting.
3. THE Makefile SHALL provide a `lint` target that runs golangci-lint with the project configuration.
4. THE Makefile SHALL provide a `test-integration` target that starts LocalStack via Docker Compose and runs integration tests.
5. THE Makefile SHALL provide a `cover` target that generates a filtered coverage report excluding example and fake packages.
6. THE Makefile SHALL provide a `test-bench` target that runs benchmark tests.

### Requirement 19: Examples and Infrastructure

**User Story:** As a developer, I want diverse examples with Terraform-provisioned infrastructure, so that I can quickly understand how to use the library and spin up required AWS resources locally.

#### Acceptance Criteria

1. THE examples Directory SHALL contain a basic example demonstrating standard queue consumption with a simple handler.
2. THE examples Directory SHALL contain a FIFO example demonstrating PerGroupID routing with custom group fields.
3. THE examples Directory SHALL contain a typed handler example demonstrating generic type-safe message processing.
4. THE examples Directory SHALL contain a middleware example demonstrating OpenTelemetry tracing and Prometheus metrics.
5. THE examples Directory SHALL contain a producer example demonstrating single and batch message publishing to standard and FIFO topics.
6. THE examples Directory SHALL contain Terraform configuration files that provision all required AWS resources (SQS queues, SNS topics, subscriptions) compatible with LocalStack.
7. THE examples Directory SHALL contain a Docker Compose file that starts LocalStack for local development.

### Requirement 20: ID Generation (GroupID and DeduplicationID)

**User Story:** As a developer, I want a flexible ID generation package, so that I can generate MessageGroupId and MessageDeduplicationId values from custom keys or randomly without writing boilerplate logic.

#### Acceptance Criteria

1. THE idgen Package SHALL provide a `GroupIDGenerator` interface with a `Generate(ctx context.Context, fields map[string]string) (string, error)` method.
2. THE idgen Package SHALL provide a `DeduplicationIDGenerator` interface with a `Generate(ctx context.Context, fields map[string]string) (string, error)` method.
3. THE idgen Package SHALL provide a `KeyBased` generator that produces a deterministic ID by concatenating and hashing the provided field values in a stable sorted order.
4. THE idgen Package SHALL provide a `Random` generator that produces a UUID v4 string, ignoring the provided fields.
5. THE idgen Package SHALL provide a `Composite` generator that combines multiple field values with a configurable separator to produce a deterministic ID without hashing.
6. THE idgen Package SHALL provide a `CompositeWithSuffix` generator that combines field values with a configurable separator and appends a random numeric suffix within a configurable range (e.g., 1 to 20) to distribute load across group partitions.
7. WHEN a KeyBased generator receives an empty fields map, THE generator SHALL return an error indicating at least one field is required.
8. WHEN a KeyBased generator receives fields, THE generator SHALL produce the same output for the same input fields regardless of map iteration order.
9. WHEN a CompositeWithSuffix generator is used, THE generator SHALL produce an ID in the format `field1<sep>field2<sep>...<sep>N` where N is a random integer within the configured range (inclusive).
10. WHEN a CompositeWithSuffix generator range is configured as [1, 20], THE generator SHALL produce suffix values uniformly distributed between 1 and 20 inclusive.
11. THE idgen Package SHALL accept generator configuration through Functional_Options (hash algorithm selection, separator character, field whitelist, suffix range min/max).
12. FOR ALL inputs to KeyBased generation, generating the ID twice with the same fields SHALL produce identical output (idempotence property).
13. THE Producer SHALL accept optional GroupIDGenerator and DeduplicationIDGenerator instances via Functional_Options to auto-generate IDs when not explicitly provided in the publish input.

### Requirement 21: Documentation

**User Story:** As a developer, I want a professional README with architecture diagrams, so that I can understand the library's design and usage at a glance.

#### Acceptance Criteria

1. THE README SHALL include a C4 context diagram showing the library's relationship to applications, AWS SQS, and AWS SNS.
2. THE README SHALL include a C4 container diagram showing the internal package architecture (conn, producer, router, consumer, broker, logger, typed, middleware, idgen).
3. THE README SHALL include installation instructions for the loafer-awsx module path.
4. THE README SHALL include a quickstart example demonstrating basic consumer and producer setup.
5. THE README SHALL include a configuration reference documenting all functional options for each component.
6. THE README SHALL include a migration guide from v2 to v3 highlighting breaking changes and new patterns.

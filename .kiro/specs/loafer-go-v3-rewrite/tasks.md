# Implementation Plan — loafer-awsx

## Overview

This plan implements loafer-awsx (formerly the loafer-go v3 rewrite, extracted into its own module `github.com/silviolleite/loafer-awsx`) as a set of small, dependency-ordered packages: connection management, producer, router, consumer, broker, logger, typed handlers, middleware, ID generation, errors, fakes, and examples. Each task is scoped to a single package concern with explicit acceptance criteria and requirement traceability.

## Tasks

- [x] 1. Initialize Module and Project Skeleton
  - Initialize the Go module with the loafer-awsx module path, set up go.mod with Go 1.24 minimum, add aws-sdk-go-v2 and test dependencies (testify, rapid, goleak).
  - Create the top-level directory structure for all packages (conn, producer, router, consumer, broker, logger, typed, middleware, idgen, errors, fake, examples).
  - Files to create/modify: `go.mod`, `go.sum`, directory stubs with `package` declarations for each package.
  - Acceptance criteria: `go build ./...` passes with Go 1.24; module path is `github.com/silviolleite/loafer-awsx` at the repository root; all required packages exist with valid Go files.
  - _Requirements: 1_

- [x] 2. Sentinel Errors Package
  - Create the `errors` package with all sentinel errors (ErrNoRoute, ErrNoSQSClient, ErrNoHandler, ErrGetMessage, ErrQueueResolve, ErrNoSNSClient, ErrEmptyInput, ErrMaxBatchSize, ErrEmptyRegion, ErrEmptyQueueName, ErrInvalidOption, ErrEmptyFields) and the `Wrap` helper function.
  - Files to create/modify: `errors/errors.go`, `errors/errors_test.go`.
  - Acceptance criteria: All sentinel errors are defined and exported; `Wrap(sentinel, err)` produces an error matchable with `errors.Is()`; 100% test coverage.
  - _Requirements: 15, 4, 7, 8_

- [x] 3. Logger Package
  - Implement the `logger` package providing `New() *slog.Logger` (a `*slog.Logger` backed by a `slog.TextHandler` on `os.Stdout`) and `NewNoOp() *slog.Logger` (a `*slog.Logger` backed by a discard handler, e.g. `slog.DiscardHandler` on Go 1.24). Do NOT define a custom `Logger` interface; the library uses the standard library `*slog.Logger` type everywhere.
  - Files to create/modify: `logger/logger.go`, `logger/noop.go`, `logger/logger_test.go`.
  - Acceptance criteria: `New()` returns a `*slog.Logger` writing structured, leveled output to stdout via a `slog.TextHandler` on `os.Stdout`; `NewNoOp()` returns a silent `*slog.Logger` backed by a discard handler; tests verify the stdout logger emits records and the NoOp logger discards all output; no custom logger interface is defined.
  - _Requirements: 11_

- [x] 4. Middleware Core Types and Chain
  - Implement the `middleware` package core: define `Handler` type (`func(ctx context.Context, msg Message) error`), `Message` interface (minimal subset to avoid import cycles), `Middleware` type (`func(Handler) Handler`), and `Chain` function that composes middlewares with first-is-outermost semantics. Include property-based tests verifying composition order.
  - Files to create/modify: `middleware/middleware.go`, `middleware/middleware_test.go`.
  - Acceptance criteria: `Chain(A, B, C)(h)` executes in order A→B→C→h; empty Chain returns handler unchanged; single middleware Chain wraps correctly; property test verifies order for arbitrary middleware count.
  - _Requirements: 10_

- [x] 5. Connection Package (conn)
  - Implement the `conn` package with `New(ctx, ...Option) (aws.Config, error)` factory and all functional options: WithRegion, WithAccessKey, WithSessionToken, WithProfile, WithEndpoint, WithRetryCount. Validate region is non-empty, default retry to 10.
  - Files to create/modify: `conn/conn.go`, `conn/options.go`, `conn/conn_test.go`.
  - Acceptance criteria: Missing region returns `ErrEmptyRegion`; default retry count is 10; endpoint override configures custom resolver; static credentials take precedence over profile; all options compose correctly.
  - _Requirements: 2_

- [x] 6. Router Package
  - Implement the `router` package: `Route` struct (immutable, unexported fields with getters), `Mode` type (Parallel, PerGroupID), `DLQConfig` struct, `New()` constructor with validation, and all functional options (WithWorkerPoolSize, WithMaxMessages, WithWaitTimeSeconds, WithVisibilityTimeout, WithExtensionLimit, WithRunMode, WithCustomGroupFields, WithMiddleware, WithDLQ). Enforce defaults and validation rules.
  - DLQ config is observe-only (no redrive by the library): `DLQConfig` has NO target ARN. It carries `MaxReceiveCount int` (mirrors the SQS redrive policy's maxReceiveCount) and an optional `OnDLQ` callback. The `WithDLQ` option signature is `WithDLQ(maxReceiveCount int, opts ...DLQOption)` — there is NO target ARN parameter.
  - Note: Task 6 was already implemented against the old DLQ shape (target ARN + MaxRetries). Its DLQ types/options in `router/dlq.go` must be revised to the observe-only model described above (rename/replace `MaxRetries`/`TargetARN` with `MaxReceiveCount`, drop `TargetARN`, and update `WithDLQ`).
  - Files to create/modify: `router/router.go`, `router/mode.go`, `router/dlq.go`, `router/options.go`, `router/router_test.go`.
  - Acceptance criteria: Empty queue name returns error; nil handler returns error; visibility < 11 clamped to 11; defaults: 5 workers, 30s visibility, 10 max messages, 10s wait time; all getters return correct values; DLQ config optional, exposing `MaxReceiveCount` and an optional `OnDLQ` callback with no target ARN.
  - _Requirements: 3_

- [x] 7. Consumer — Message Implementation
  - Implement the internal `message` struct in the consumer package satisfying the full `Message` interface: Decode, DecodeMessage, Attribute, Attributes, SystemAttributeByKey, SystemAttributes, Metadata, Identifier, Body, Message, TimeStamp, Dispatch, Backoff, BackedOff. Include dispatch/backoff signaling channels for visibility management.
  - Files to create/modify: `consumer/message.go`, `consumer/message_test.go`, `consumer/client.go`.
  - Acceptance criteria: Decode correctly unmarshals JSON body; DecodeMessage unmarshals inner SNS Message field; Dispatch closes the dispatch channel; Backoff sends duration to backoff channel and marks BackedOff true; all attribute accessors return correct values; Identifier returns receipt handle.
  - _Requirements: 6_

- [x] 8. Consumer — Visibility Timeout Manager
  - Implement the visibility timeout manager goroutine in the consumer package. It periodically extends message visibility (sleep = visibilityTimeout - 10s), handles dispatch signal, backoff signal, extension limit, and context cancellation. Enforce AWS max (43200s) and min (0s) visibility limits.
  - Files to create/modify: `consumer/visibility.go`, `consumer/visibility_test.go`.
  - Acceptance criteria: Extension fires at correct interval; first extension sets visibility to route value; subsequent extensions increment by route value; stops at extension limit; dispatch signal stops immediately; backoff sets custom visibility and stops; ChangeMessageVisibility failure is logged but doesn't crash; max 43200s enforced; goleak passes.
  - _Requirements: 5_

- [x] 9. Consumer — Worker Pool and Dispatch
  - Implement the worker pool dispatch logic: Parallel mode (random worker assignment) and PerGroupID mode (hash of MessageGroupId + custom group fields → worker index). Workers read from buffered channels, execute middleware-wrapped handler, handle success (delete), error (log + leave), and backoff (change visibility).
  - Files to create/modify: `consumer/dispatch.go`, `consumer/dispatch_test.go`.
  - Acceptance criteria: Parallel mode distributes messages across workers; PerGroupID mode consistently assigns same group key to same worker index (property test); worker correctly deletes on nil return; worker leaves message on error return; worker changes visibility on backoff; no goroutine leaks on shutdown.
  - _Requirements: 4, 16_

- [x] 10. Consumer — Polling Loop and Run
  - Implement `Consumer.New()` and `Consumer.Run(ctx)`: `New` validates inputs and returns `(*Consumer, error)` — a nil SQS client returns `ErrNoSQSClient` and a nil route returns `ErrNoRoute` (fail-fast at construction). `Run` resolves the queue URL (fail-fast), enters the polling loop with batch receive (requesting all attributes and setting `VisibilityTimeout` to the route value to avoid an extra initial extension call), dispatches to the worker pool, handles receive errors with a retry timeout, and performs graceful shutdown on context cancellation (stop polling, close channels, wait for in-flight).
  - Files to create/modify: `consumer/consumer.go`, `consumer/options.go`, `consumer/consumer_test.go`.
  - Acceptance criteria: `New` returns `ErrNoSQSClient` on nil client and `ErrNoRoute` on nil route; Run resolves queue URL first and returns error on failure; polling uses configured maxMessages, waitTimeSeconds, and sets VisibilityTimeout to the route value on receive; receive error triggers retry timeout wait; context cancellation stops polling; all in-flight messages complete before Run returns; no goroutine leaks (goleak); race detector clean.
  - _Requirements: 4, 13, 15_

- [x] 11. Broker Package
  - Implement the `broker` package: `New(sqsClient, routes, ...Option) (*Broker, error)` with validation (no routes → ErrNoRoute), `Run(ctx) error` that starts all consumers concurrently, applies global middleware, handles fail-fast on config errors, coordinated shutdown, and retry timeout configuration. The `WithLogger` option accepts a `*slog.Logger`.
  - Files to create/modify: `broker/broker.go`, `broker/options.go`, `broker/broker_test.go`.
  - Acceptance criteria: No routes returns ErrNoRoute; Run starts one consumer per route concurrently; context cancellation triggers shutdown of all consumers; consumer config error (queue resolve) causes fail-fast; global middleware applied outermost; no goroutine leaks after Run returns; default retry timeout is 5s.
  - _Requirements: 7, 13_

- [x] 12. Producer — Core Publish
  - Implement the `producer` package: `SNSClient` interface, `Producer` struct with `New(client, ...Option)`, `Publish(ctx, *PublishInput) (string, error)` for single message publish, and `BuildTopicARN(region, accountID, topicName) string` helper. Validate nil/empty input, handle FIFO fields (GroupID, DeduplicationID), and message attributes.
  - Files to create/modify: `producer/producer.go`, `producer/client.go`, `producer/arn.go`, `producer/options.go`, `producer/producer_test.go`.
  - Acceptance criteria: Nil input returns ErrEmptyInput; empty message returns ErrEmptyInput; GroupID included for FIFO topics; DeduplicationID included when provided; message attributes sent as String type; returns message ID on success; BuildTopicARN produces correct format; concurrent-safe usage.
  - _Requirements: 8_

- [x] 13. Producer — Batch Publish
  - Implement `PublishBatch(ctx, *PublishBatchInput) (*PublishBatchOutput, error)` supporting up to 10 messages per batch. Validate batch size, map SNS SDK response to successful/failed entries.
  - Files to create/modify: `producer/producer.go`, `producer/producer_test.go`.
  - Acceptance criteria: Batch > 10 returns ErrMaxBatchSize; nil input returns ErrEmptyInput; successful entries include EntryID and MessageID; failed entries include EntryID and error; all FIFO fields and attributes passed through correctly.
  - _Requirements: 8_

- [x] 14. IDGen — KeyBased and Random Generators
  - Implement the `idgen` package core: `GroupIDGenerator` and `DeduplicationIDGenerator` interfaces, `NewKeyBased(opts ...Option)` generator (sorts fields, concatenates, hashes with configurable algorithm), `NewRandom()` generator (UUID v4 via crypto/rand), and options (WithHashAlgorithm, WithSeparator, WithFields).
  - Files to create/modify: `idgen/idgen.go`, `idgen/keybased.go`, `idgen/random.go`, `idgen/options.go`, `idgen/keybased_test.go`, `idgen/random_test.go`.
  - Acceptance criteria: Empty fields map returns ErrEmptyFields; same fields produce same hash regardless of map iteration order (property test); different fields produce different hashes; Random produces valid UUID v4 format; SHA256 and FNV64 algorithms supported; property test verifies idempotence.
  - _Requirements: 20_

- [x] 15. IDGen — Composite and CompositeWithSuffix Generators
  - Implement `NewComposite(opts ...Option)` (joins field values with separator in whitelist order) and `NewCompositeWithSuffix(opts ...Option)` (composite + random numeric suffix in [min, max] range). Include WithSuffixRange option.
  - Files to create/modify: `idgen/composite.go`, `idgen/composite_suffix.go`, `idgen/composite_test.go`, `idgen/composite_suffix_test.go`.
  - Acceptance criteria: Composite joins fields in whitelist order with separator; CompositeWithSuffix appends suffix in format `<sep>N`; suffix N always within [min, max] inclusive (property test); default range [1, 20]; invalid range (min > max) returns ErrInvalidOption.
  - _Requirements: 20_

- [x] 16. Producer — ID Generation Integration
  - Wire `GroupIDGenerator` and `DeduplicationIDGenerator` into the Producer via functional options (WithGroupIDGenerator, WithDeduplicationIDGenerator). When input.GroupID is empty and a generator is configured, auto-generate from message attributes. Same for DeduplicationID.
  - Files to create/modify: `producer/options.go`, `producer/producer.go`, `producer/producer_test.go`.
  - Acceptance criteria: When generator is set and GroupID is empty, auto-generates from attributes; when GroupID is explicitly provided, generator is not invoked; same behavior for DeduplicationID; generator errors propagated correctly.
  - _Requirements: 8, 20_

- [x] 17. Typed Package — Codec and WrapHandler
  - Implement the `typed` package: `Codec[T]` interface, `JSONCodec[T]` implementation (Encode/Decode via json.Marshal/Unmarshal), and `WrapHandler[T](codec, fn) middleware.Handler` adapter that decodes message body and calls the typed handler. Include round-trip property test.
  - Files to create/modify: `typed/typed.go`, `typed/codec.go`, `typed/codec_test.go`, `typed/typed_test.go`.
  - Acceptance criteria: `Decode(Encode(x)) == x` for all serializable types (property test); WrapHandler decodes body and calls typed fn; decode failure returns error to consumer; JSONCodec handles nested structs, slices, and maps correctly.
  - _Requirements: 9_

- [x] 18. Typed Package — Typed Producer
  - Implement `typed.Producer[T]` wrapper that encodes a value of type T via Codec before delegating to the underlying `producer.Producer`. Include `NewProducer[T]`, `Publish`, and publish options (WithGroupID, WithDeduplicationID, WithAttributes).
  - Files to create/modify: `typed/producer.go`, `typed/producer_test.go`.
  - Acceptance criteria: Typed Producer encodes value before publish; encode failure returns error without calling SNS; all publish options forwarded correctly; concurrent-safe usage.
  - _Requirements: 9_

- [x] 19. Recovery Middleware
  - Implement the `Recovery` middleware that catches panics in handlers, logs the panic value and stack trace via a `*slog.Logger`, and returns a wrapped error instead of crashing the process.
  - Files to create/modify: `middleware/recovery.go`, `middleware/recovery_test.go`.
  - Acceptance criteria: Panic in handler is caught; error returned contains panic value; stack trace logged at Error level; non-panicking handlers pass through unchanged; nil error on success.
  - _Requirements: 10_

- [x] 20. Logging Middleware
  - Implement the `Logging` middleware that logs message receipt (message ID), processing duration, and outcome (success/error) via a `*slog.Logger` at appropriate levels (Info for receipt and success, Error for failures).
  - Files to create/modify: `middleware/logging.go`, `middleware/logging_test.go`.
  - Acceptance criteria: Receipt logged with message ID at Info level; success logged with duration at Info level; error logged with duration and error detail at Error level; log output includes all required key-value pairs.
  - _Requirements: 10_

- [x] 21. Metrics Middleware (Prometheus)
  - Implement the `Metrics` middleware that registers and updates Prometheus collectors: `loafer_messages_received_total`, `loafer_messages_processed_total` (with status label), `loafer_messages_errors_total`, `loafer_message_processing_duration_seconds`, `loafer_messages_inflight`, and `loafer_messages_dlq_total`. Support custom registerer via WithMetricsRegisterer option.
  - Files to create/modify: `middleware/metrics.go`, `middleware/metrics_test.go`.
  - Acceptance criteria: All 6 metrics registered; received counter increments per message; processed counter increments with correct status label; errors counter increments on handler error; inflight gauge increments on start, decrements on completion; duration histogram observes elapsed time; custom registerer option works; no panics on duplicate registration.
  - _Requirements: 10, 12_

- [x] 22. OpenTelemetry Middleware
  - Implement the `OTel` middleware that creates a span per message processing, sets messaging.system, messaging.destination.name, messaging.message.id as span attributes, records errors as span events, and sets span status. Support custom TracerProvider via WithTracerProvider option.
  - Files to create/modify: `middleware/otel.go`, `middleware/otel_test.go`.
  - Acceptance criteria: Span created with correct name pattern `loafer.process/<route>`; SpanKind is Consumer; message attributes recorded; errors recorded as span events with Error status; success sets Ok status; custom TracerProvider option works.
  - _Requirements: 10_

- [x] 23. DLQ Observability in Consumer
  - Implement observe-only DLQ logic in the consumer. AWS SQS's native redrive policy performs the actual move to the DLQ; the library does NOT move, publish, or delete messages for DLQ purposes. When a DLQ route option is configured, WHEN the handler returns an error AND the message's `ApproximateReceiveCount` system attribute is greater than or equal to the route's `MaxReceiveCount`, treat the message as exhausted and emit observability signals: emit an Error-level log (message identifier, queue name, receive count), increment `loafer_messages_dlq_total` via the Metrics middleware when enabled, and invoke the optional `OnDLQ` callback. Leave the message in the source queue so AWS SQS performs the redrive per its own policy — do NOT publish to any destination and do NOT delete the message. No SNS client is required.
  - Files to create/modify: `consumer/consumer.go`, `consumer/consumer_test.go`.
  - Acceptance criteria: An exhausted message (handler error AND `ApproximateReceiveCount` >= route `MaxReceiveCount`) triggers an Error-level log, a `loafer_messages_dlq_total` increment when the Metrics middleware is enabled, and the `OnDLQ` callback when configured; the message is left in the source queue (not deleted); no publish to any DLQ destination occurs; the `ApproximateReceiveCount` threshold is respected (below threshold does not trigger DLQ signals); no DLQ processing when DLQ is not configured; Error log includes message identifier, queue name, and receive count.
  - _Requirements: 14_

- [x] 24. Fakes Package
  - Create the `fake` package with test doubles for all key interfaces: `Message` (configurable struct implementing full interface), `SQSClient` (configurable responses per call), `SNSClient` (configurable responses), and a capturing `slog.Handler` that records log records so tests can build a `*slog.Logger` and assert on emitted logs. These fakes are used across all package tests.
  - Files to create/modify: `fake/message.go`, `fake/sqs_client.go`, `fake/sns_client.go`, `fake/loghandler.go`.
  - Acceptance criteria: `fake.Message` implements full `middleware.Message` interface; `fake.SQSClient` implements `consumer.SQSClient` with configurable return values; `fake.SNSClient` implements `producer.SNSClient` with configurable return values; the capturing `slog.Handler` records all log records (level, message, attributes) and can be wrapped in a `*slog.Logger` via `slog.New` for assertions; no custom logger interface is faked.
  - _Requirements: 17_

- [x] 25. Examples — Basic Consumer
  - Create the basic example demonstrating standard queue consumption: configure conn, create a simple handler, configure a route with defaults, create and run a broker with graceful shutdown via OS signal handling.
  - Files to create/modify: `examples/basic/main.go`.
  - Acceptance criteria: Example compiles successfully; demonstrates conn setup, route creation, broker creation, and graceful shutdown; code is well-commented explaining each step.
  - _Requirements: 19_

- [x] 26. Examples — FIFO, Typed, Middleware, and Producer
  - Create remaining examples: FIFO example with PerGroupID routing and custom group fields; typed handler example with JSONCodec; middleware example with OTel tracing and Prometheus metrics; producer example with single and batch publish to standard and FIFO topics.
  - Files to create/modify: `examples/fifo/main.go`, `examples/typed/main.go`, `examples/middleware/main.go`, `examples/producer/main.go`.
  - Acceptance criteria: All examples compile successfully; FIFO example uses PerGroupID mode with custom fields; typed example uses WrapHandler with JSONCodec; middleware example configures OTel and Metrics; producer example shows single and batch publish with FIFO support.
  - _Requirements: 19_

- [x] 27. Terraform and Docker Compose for LocalStack
  - Create Terraform configuration files provisioning all required AWS resources (SQS queues standard and FIFO, SNS topics standard and FIFO, SNS-to-SQS subscriptions) compatible with LocalStack. Create a Docker Compose file that starts LocalStack with SQS and SNS services.
  - Files to create/modify: `examples/terraform/main.tf`, `examples/terraform/variables.tf`, `examples/terraform/outputs.tf`, `examples/terraform/docker-compose.yml`.
  - Acceptance criteria: Terraform files are valid HCL; resources include standard queue, FIFO queue, standard topic, FIFO topic, and subscriptions; Docker Compose starts LocalStack with SQS+SNS on port 4566; variables are configurable for region and account ID.
  - _Requirements: 19_

- [x] 28. Makefile and CI Configuration
  - Create the Makefile at the loafer-awsx module root with all required targets (configure, test, lint, test-integration, cover, test-bench) and the GitHub Actions CI workflow (lint, test with race detection, coverage reporting). Configure `.golangci.yml` with the `github.com/silviolleite/loafer-awsx` local/gci prefix.
  - Files to create/modify: `Makefile`, `.github/workflows/ci.yml`, `.golangci.yml`, `docker-compose.integration.yml`.
  - Acceptance criteria: `make configure` installs all dev tools; `make test` runs tests with -race and coverage; `make lint` runs golangci-lint with the `github.com/silviolleite/loafer-awsx` prefix; `make test-integration` starts LocalStack and runs integration tests; `make cover` generates filtered coverage report; `make test-bench` runs benchmarks; CI workflow triggers on push/PR.
  - _Requirements: 18_

- [x] 29. Integration Tests
  - Write integration tests using LocalStack that exercise the full message lifecycle: receive → process → delete, receive → error → redelivery, receive → backoff → visibility change, visibility timeout extension, graceful shutdown with in-flight messages, and PerGroupID routing consistency.
  - Files to create/modify: `consumer/consumer_integration_test.go`, `broker/broker_integration_test.go`.
  - Acceptance criteria: Tests use `//go:build integration` tag; full lifecycle verified end-to-end; graceful shutdown verified with in-flight messages; all tests pass with -race flag; goleak verification in teardown.
  - _Requirements: 16, 17_

- [x] 30. Release Automation and Contribution Workflow
  - Configure automated releases with release-please for the Go module at the repository root: add `.release-please-config.json` (`release-type: go`, `include-v-in-tag: true`, root package `.` with `changelog-path: CHANGELOG.md`) and `.release-please-manifest.json` seeded with the current/next version (e.g. `{ ".": "0.1.0" }`), plus an initial `CHANGELOG.md`.
  - Add the release workflow `.github/workflows/release-please.yml` that triggers on push to `main`, first reuses the existing CI workflow via `uses: ./.github/workflows/ci.yml`, then runs `googleapis/release-please-action@v4` with `config-file` and `manifest-file` inputs; grant `contents: write` and `pull-requests: write` permissions.
  - Enforce Conventional Commits: add `commitlint.config.js` (`module.exports = { extends: ['@commitlint/config-conventional'] };`), a private dev-only `package.json` with `@commitlint/cli` and `@commitlint/config-conventional` dev dependencies and a hook-preparation script, and `.github/workflows/commitlint.yml` that runs on `pull_request`, checks out with `fetch-depth: 0`, and runs `wagoid/commitlint-github-action@v6`.
  - Wire the local commit-msg commitlint hook through the EXISTING lefthook setup (not husky): add a `commit-msg` hook to `lefthook.yml` running `npx --no -- commitlint --edit {1}` to stay consistent with the awsx tooling.
  - Extend the `Makefile` to align with the natsx developer experience: add `setup-dev` (installs npm/commitlint deps and registers the commit-msg hook via lefthook), `install-govulncheck` and `check-vuln` (`govulncheck ./...`) with govulncheck wired into the `test`/`check` flow, and `test-chaos` (`GOMAXPROCS=1 go test ./... -race -count=30 -shuffle=on -timeout 15m`); extend the `configure` target to include the new install targets and `setup-dev`.
  - Add `CONTRIBUTING.md` documenting the Conventional Commits requirement, the commitlint commit-msg hook, how release-please cuts releases from commit history, and the local dev setup (`make configure` / `make setup-dev`).
  - Files to create/modify: `.release-please-config.json`, `.release-please-manifest.json`, `.github/workflows/release-please.yml`, `.github/workflows/commitlint.yml`, `commitlint.config.js`, `package.json`, `CHANGELOG.md`, `CONTRIBUTING.md`, `lefthook.yml`, `Makefile`.
  - Acceptance criteria: release-please is configured for a Go module at the repository root with v-prefixed tags and CHANGELOG generation; the release-please workflow runs CI then the release action on push to `main` with `contents: write` and `pull-requests: write` permissions; commitlint validates PR commits via the GitHub Action and locally via a commit-msg git hook wired through lefthook; Conventional Commits are enforced; `make setup-dev` installs the commit-hook tooling and registers the hook; `make check-vuln` runs `govulncheck ./...`; `make test-chaos` runs the stress suite (`GOMAXPROCS=1 go test ./... -race -count=30 -shuffle=on -timeout 15m`); `CONTRIBUTING.md` documents the commit convention and release flow; all config files are valid (valid JSON/YAML/JS).
  - _Requirements: 18_

- [x] 31. Examples README and Makefile
  - Create a README and a Makefile under `examples/` to make running the examples easier. The README documents the prerequisites (Docker, Terraform, Go 1.24), how to bring up LocalStack and provision resources with the existing Terraform config, the required environment variables (region, endpoint, account ID), and how to run each example (basic, fifo, typed, middleware, producer). The Makefile provides convenience targets (e.g. `up`/`down` for LocalStack, `provision`/`destroy` for Terraform, and per-example `run-*` targets plus a `run-all` helper).
  - Files to create/modify: `examples/README.md`, `examples/Makefile`.
  - Acceptance criteria: `examples/README.md` documents prerequisites, LocalStack + Terraform setup, environment variables, and step-by-step run instructions for every example; `examples/Makefile` exposes targets to start/stop LocalStack, provision/destroy Terraform resources, and run each example; targets are self-documented and reuse the existing `examples/terraform/docker-compose.yml`.
  - _Requirements: 19_

- [x] 32. README and Documentation
  - Write the comprehensive README.md with C4 context diagram (Mermaid), C4 container diagram, installation instructions for the loafer-awsx module path, quickstart example (consumer + producer setup), configuration reference for all functional options per component, and migration guide from loafer-go v2 to loafer-awsx highlighting breaking changes and new patterns.
  - Files to create/modify: `README.md`.
  - Acceptance criteria: C4 context diagram present (Mermaid); C4 container diagram present; installation instructions reference `github.com/silviolleite/loafer-awsx`; quickstart shows working consumer and producer; configuration reference covers all packages; a link to the `examples/` directory (and its README) points readers to runnable examples; add a note stating that the project was inspired by https://github.com/JustCodes/loafer-go.
  - _Requirements: 21_

- [x] 33. Broker — Unbounded Shutdown Wait by Default
  - Change the broker so graceful shutdown waits indefinitely for in-flight consumers to drain by default, applying a bound only when the caller explicitly opts in via `WithShutdownTimeout`. Today `defaultShutdownTimeout` is 30s, so a slow-but-healthy consumer is force-abandoned on shutdown; in production this cuts off in-flight work that would have completed. The default must be "wait until consumers finish", and a timeout must be a deliberate choice.
  - In `broker/options.go`: remove the `defaultShutdownTimeout` default (or set it to 0/"unset" meaning no bound). Keep `WithShutdownTimeout(d)` as the only way to set a positive bound; a non-positive duration continues to mean "no timeout" (unbounded wait). Update the `WithShutdownTimeout` doc comment to state that when unset the broker waits indefinitely.
  - In `broker/broker.go`: update `New` so `shutdownTimeout` is left as the zero value (unbounded) rather than seeded with a 30s default; update the `New` and `Run` doc comments accordingly. Change `waitForConsumers` so that when `shutdownTimeout <= 0` it blocks on the consumers-done channel with no timer (no `time.NewTimer`, no timeout log path), and only arms the timer / logs "shutdown timeout exceeded" when a positive timeout was configured. Ensure no timer goroutine or channel leaks in the unbounded path.
  - Update the spec to match: revise Requirement 13 acceptance criterion 2 in `requirements.md` to state the broker waits for all in-flight messages to complete, and only bounds the wait when a shutdown timeout is explicitly configured; update the shutdown-timeout default in `design.md` (the `WithShutdownTimeout` note and the defaults table, changing `30 seconds` to `unbounded (wait until consumers finish)`).
  - Update user-facing docs: change the `WithShutdownTimeout` default in the `README.md` broker options table from `30s` to `unbounded` (waits until consumers finish; set a duration to bound it).
  - Files to create/modify: `broker/options.go`, `broker/broker.go`, `broker/broker_test.go`, `.kiro/specs/loafer-go-v3-rewrite/requirements.md`, `.kiro/specs/loafer-go-v3-rewrite/design.md`, `README.md`.
  - Acceptance criteria: with no `WithShutdownTimeout` option, `Run` waits for all in-flight consumers to finish on context cancellation and never logs "shutdown timeout exceeded"; a positive `WithShutdownTimeout` still bounds the wait and logs on expiry as before; a non-positive `WithShutdownTimeout` is treated as unbounded; `waitForConsumers` starts no timer in the unbounded path; existing tests are updated (the current 30s-default assumption in `TestRunReturnsWhenShutdownTimeoutExceeded` and any default-timeout test) and a new test verifies the unbounded default drains a slow consumer without abandoning it; no goroutine leaks (goleak); race detector clean.
  - _Requirements: 13_

## Task Dependency Graph

- Task 1: (no dependencies)
- Task 2: depends on 1
- Task 3: depends on 1
- Task 4: depends on 1
- Task 5: depends on 1, 2
- Task 6: depends on 4
- Task 7: depends on 4
- Task 8: depends on 7, 3
- Task 9: depends on 8, 6
- Task 10: depends on 9
- Task 11: depends on 10, 3
- Task 12: depends on 2, 5
- Task 13: depends on 12
- Task 14: depends on 2
- Task 15: depends on 14
- Task 16: depends on 13, 15
- Task 17: depends on 4
- Task 18: depends on 17, 12
- Task 19: depends on 4, 3
- Task 20: depends on 4, 3
- Task 21: depends on 4
- Task 22: depends on 4
- Task 23: depends on 10, 21
- Task 24: depends on 7, 12
- Task 25: depends on 11
- Task 26: depends on 25, 17, 22, 21, 16
- Task 27: depends on 1
- Task 28: depends on 1
- Task 29: depends on 11, 27
- Task 30: depends on 28
- Task 31: depends on 26, 27, 28
- Task 32: depends on 26, 28, 31
- Task 33: depends on 11

```json
{
  "waves": [
    { "wave": 1, "tasks": [1] },
    { "wave": 2, "tasks": [2, 3, 4, 27, 28] },
    { "wave": 3, "tasks": [5, 6, 7, 14, 17, 19, 20, 21, 22, 30] },
    { "wave": 4, "tasks": [8, 12, 15] },
    { "wave": 5, "tasks": [9, 13, 18, 24] },
    { "wave": 6, "tasks": [10, 16] },
    { "wave": 7, "tasks": [11, 23] },
    { "wave": 8, "tasks": [25, 29] },
    { "wave": 9, "tasks": [26] },
    { "wave": 10, "tasks": [31] },
    { "wave": 11, "tasks": [32] },
    { "wave": 12, "tasks": [33] }
  ]
}
```

## Notes

- Each task targets a single package concern and must satisfy its acceptance criteria before completion.
- Tests follow the project steering: `<package>_test` external test packages, table-driven and property-based tests (pgregory.net/rapid), goleak for goroutine-leak detection, and a 95% minimum coverage target.
- Requirement numbers reference the requirements in `requirements.md`.

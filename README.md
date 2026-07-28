# loafer-awsx

[![Go Reference](https://pkg.go.dev/badge/github.com/silviolleite/loafer-awsx.svg)](https://pkg.go.dev/github.com/silviolleite/loafer-awsx)
[![CI](https://github.com/silviolleite/loafer-awsx/actions/workflows/ci.yml/badge.svg)](https://github.com/silviolleite/loafer-awsx/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/github/go-mod/go-version/silviolleite/loafer-awsx)](go.mod)
[![License](https://img.shields.io/github/license/silviolleite/loafer-awsx)](LICENSE)

> A modern, idiomatic Go library for AWS SQS/SNS message processing, built on
> `aws-sdk-go-v2` with generic type-safe handlers, a composable middleware
> pipeline, first-class `log/slog` logging, and built-in Prometheus and
> OpenTelemetry observability.

`loafer-awsx` organizes message processing into small, single-responsibility
packages you can compose: build an AWS connection, declare routes, wrap them in
a broker, and publish events with a producer. Everything is configured through
functional options, and every component accepts the standard library
`*slog.Logger` directly, no custom logger interface.

- **Module:** `github.com/silviolleite/loafer-awsx`
- **Minimum Go version:** Go 1.26 or later
- **AWS SDK:** `aws-sdk-go-v2` (SQS + SNS)

---

## Table of Contents

1. [Architecture](#architecture)
   - [Context Diagram](#context-diagram)
   - [Container Diagram](#container-diagram)
2. [Installation](#installation)
3. [Quickstart](#quickstart)
4. [Examples](#examples)
5. [Configuration Reference](#configuration-reference)
6. [Scheduled Retry (FIFO)](#scheduled-retry-fifo)
7. [Benchmarks](#benchmarks)
8. [Acknowledgements](#acknowledgements)
9. [License](#license)

---

## Architecture

`loafer-awsx` is a library, not a service. Your application imports it, wires
routes and a broker, and processes messages from AWS SQS while optionally
publishing to AWS SNS. The library also exposes Prometheus metrics and
OpenTelemetry spans for observability.

### Context Diagram

```mermaid
flowchart LR
    dev([Developer])
    loafer[loafer-awsx]
    sqs[(AWS SQS)]
    sns[(AWS SNS)]
    prom[Prometheus]
    otel[OpenTelemetry]

    dev --> loafer
    loafer --> sqs
    loafer --> sns
    loafer --> prom
    loafer --> otel
```

### Container Diagram

The broker orchestrates one consumer per route: each `Route` binds a queue to a
handler, and its `Consumer` runs a worker pool that polls the matching SQS queue.
Publishing runs alongside through the `producer`.

```mermaid
flowchart TB
    app([Application])
    broker[Broker]

    subgraph routeA[Route A]
        consumerA[Consumer / Workers]
    end
    subgraph routeB[Route B]
        consumerB[Consumer / Workers]
    end
    subgraph routeN[Route N]
        consumerN[Consumer / Workers]
    end

    sqsA[(SQS Queue A)]
    sqsB[(SQS Queue B)]
    sqsN[(SQS Queue N)]

    producer[Producer]
    sns[(AWS SNS)]

    app --> broker
    app --> producer

    broker --> routeA
    broker --> routeB
    broker --> routeN

    consumerA --> sqsA
    consumerB --> sqsB
    consumerN --> sqsN

    producer --> sns
```

Cross-cutting packages support this pipeline: `conn` builds the shared
`aws.Config`, `middleware` wraps each route handler (global middleware outermost,
route middleware innermost), `typed` adds generic type-safe handlers and
producers, `idgen` generates FIFO IDs, and `logger` supplies the `*slog.Logger`
used throughout.

**Package responsibilities at a glance:**

| Package | Responsibility |
| --- | --- |
| `conn` | Factory for an `aws.Config` (region, credentials, endpoint, profile, retry). |
| `logger` | Constructors for the standard library `*slog.Logger` (stdout + no-op). |
| `middleware` | `Handler`, `Middleware`, `Chain`, and built-in Recovery, Logging, Metrics, OTel. |
| `router` | Immutable `Route` value object binding a queue to a handler and options. |
| `consumer` | SQS polling loop, worker-pool dispatch, visibility management, DLQ observability. |
| `broker` | Lifecycle orchestrator that runs one consumer per route with graceful shutdown. |
| `producer` | SNS single and batch publish for standard and FIFO topics. |
| `typed` | Generic, type-safe handlers and producers via `Codec[T]`. |
| `idgen` | `MessageGroupId` / `MessageDeduplicationId` generation strategies. |
| `errors` | Sentinel errors matchable with `errors.Is`. |

---

## Installation

Requires Go 1.26 or later.

```bash
go get github.com/silviolleite/loafer-awsx
```

Then import the packages you need, for example:

```go
import (
    "github.com/silviolleite/loafer-awsx/broker"
    "github.com/silviolleite/loafer-awsx/conn"
    "github.com/silviolleite/loafer-awsx/logger"
    "github.com/silviolleite/loafer-awsx/router"
    "github.com/silviolleite/loafer-awsx/producer"
)
```

---

## Quickstart

A typical setup builds a shared AWS config with `conn.New`, binds queue names to
handlers with `router.New`, hands the routes to a `broker`, and calls
`broker.Run` (which blocks until the context is canceled, then drains in-flight
messages). Publishing works the same way: create a `producer` and call `Publish`
or `PublishBatch`.

For complete, runnable programs covering the consumer, producer, FIFO, typed,
and middleware setups, see the [`examples/`](./examples) directory and its
[README](./examples/README.md).

---

## Examples

Runnable, self-contained programs live in the [`examples/`](./examples)
directory, wired to run locally against [LocalStack](https://www.localstack.cloud/)
with infrastructure provisioned by Terraform. See
[`examples/README.md`](./examples/README.md) for setup and run instructions
(`make up`, `make provision`, `make run-basic`, and friends).

| Example | Directory | What it shows |
| --- | --- | --- |
| Basic | [`examples/basic/`](./examples/basic) | Standard SQS queue consumption with a simple handler. |
| FIFO | [`examples/fifo/`](./examples/fifo) | Ordered consumption in `PerGroupID` mode with custom group fields. |
| Typed | [`examples/typed/`](./examples/typed) | Generic type-safe handling via `typed.WrapHandler` + `typed.JSONCodec`. |
| Middleware | [`examples/middleware/`](./examples/middleware) | Recovery, logging, Prometheus metrics, and OpenTelemetry tracing. |
| Producer | [`examples/producer/`](./examples/producer) | Single and batch publishing to standard and FIFO SNS topics. |

---

## Configuration Reference

Every component is configured through functional options. Invalid values are
rejected at construction time (returning a descriptive error) rather than being
silently accepted.

### `conn` — AWS configuration

`conn.New(ctx context.Context, opts ...conn.Option) (aws.Config, error)`

| Option | Signature | Default | Description |
| --- | --- | --- | --- |
| `WithRegion` | `WithRegion(region string)` | — (required) | AWS region. `New` returns `ErrEmptyRegion` when empty. |
| `WithAccessKey` | `WithAccessKey(key, secret string)` | — | Static credentials; take precedence over a profile. |
| `WithSessionToken` | `WithSessionToken(token string)` | — | Session token; applied only with static credentials. |
| `WithProfile` | `WithProfile(profile string)` | — | Shared config profile name. |
| `WithEndpoint` | `WithEndpoint(url string)` | — | Custom endpoint URL (LocalStack, etc.). |
| `WithRetryCount` | `WithRetryCount(n uint)` | `10` | Maximum retry attempts. |

### `router` — route configuration

`router.New(queueName string, handler middleware.Handler, opts ...router.Option) (*router.Route, error)`

Returns `ErrEmptyQueueName` for an empty name and `ErrNoHandler` for a nil
handler. Option failures are wrapped with `ErrInvalidOption`.

| Option | Signature | Default | Description |
| --- | --- | --- | --- |
| `WithWorkerPoolSize` | `WithWorkerPoolSize(n int)` | `5` | Worker goroutines per route (must be > 0). |
| `WithMaxMessages` | `WithMaxMessages(n int32)` | `10` | Messages per SQS receive call (range `[1, 10]`). |
| `WithWaitTimeSeconds` | `WithWaitTimeSeconds(n int32)` | `10` | Long-poll wait in seconds (range `[0, 20]`). |
| `WithVisibilityTimeout` | `WithVisibilityTimeout(seconds int32)` | `30` | Visibility timeout; values `<= 11` are clamped up to `11`. |
| `WithExtensionLimit` | `WithExtensionLimit(n int)` | `2` | Max visibility extensions (must not be negative). |
| `WithRunMode` | `WithRunMode(mode router.Mode)` | `Parallel` | Dispatch strategy: `Parallel` or `PerGroupID`. |
| `WithCustomGroupFields` | `WithCustomGroupFields(fields ...string)` | — | Fields forming the group key for `PerGroupID`. |
| `WithMiddleware` | `WithMiddleware(mws ...middleware.Middleware)` | — | Route-level middleware appended in order. |
| `WithDLQ` | `WithDLQ(maxReceiveCount int, opts ...router.DLQOption)` | — | Enables DLQ observability (must be > 0). See below. |

**Run modes** (`router.Mode`): `Parallel` (random worker assignment) and
`PerGroupID` (hash of `MessageGroupId` + custom group fields, preserving
per-group ordering).

**DLQ observability** (`router.DLQOption`):

| Option | Signature | Description |
| --- | --- | --- |
| `WithOnDLQ` | `WithOnDLQ(fn func(ctx context.Context, msg middleware.Message))` | Callback invoked when a message is treated as exhausted. |

> DLQ is **observe-only**. `WithDLQ` does **not** take a target ARN and the
> library never moves, publishes, or deletes messages for DLQ purposes. AWS SQS
> performs the actual redrive natively via the source queue's redrive policy.
> `maxReceiveCount` must mirror that policy; it is used only to detect when a
> message is exhausted so the consumer can emit an Error log, the
> `loafer_messages_dlq_total` metric, and the optional `OnDLQ` callback while
> leaving the message in the queue.

### `consumer` — SQS polling

`consumer.New(client consumer.SQSClient, route *router.Route, opts ...consumer.Option) (*consumer.Consumer, error)`

Returns `ErrNoSQSClient` for a nil client and `ErrNoRoute` for a nil route. In
most applications the broker creates consumers for you; use these options
directly only when driving a consumer yourself.

| Option | Signature | Default | Description |
| --- | --- | --- | --- |
| `WithLogger` | `WithLogger(log *slog.Logger)` | no-op | Structured logger (nil is ignored). |
| `WithRetryTimeout` | `WithRetryTimeout(d time.Duration)` | `5s` | Wait after a failed `ReceiveMessage` (non-positive ignored). |
| `WithGlobalMiddleware` | `WithGlobalMiddleware(mws ...middleware.Middleware)` | — | Outermost middleware, ahead of route middleware. |
| `WithDLQMetric` | `WithDLQMetric(inc consumer.DLQMetric)` | — | Increments `loafer_messages_dlq_total`; wire only with Metrics enabled. |

### `broker` — lifecycle orchestration

`broker.New(sqsClient consumer.SQSClient, routes []*router.Route, opts ...broker.Option) (*broker.Broker, error)`

Returns `ErrNoRoute` when no routes are provided. `broker.Run(ctx)` starts one
consumer per route, blocks until the context is canceled, and drains in-flight
messages within the shutdown timeout with no goroutine leaks.

| Option | Signature | Default | Description |
| --- | --- | --- | --- |
| `WithLogger` | `WithLogger(log *slog.Logger)` | `logger.New()` | Structured logger, forwarded to every consumer. |
| `WithRetryTimeout` | `WithRetryTimeout(d time.Duration)` | `5s` | Per-consumer wait after a failed receive. |
| `WithShutdownTimeout` | `WithShutdownTimeout(d time.Duration)` | unbounded | Max wait for in-flight messages on shutdown. Unset waits until consumers finish; set a duration to bound it. |
| `WithMiddleware` | `WithMiddleware(mws ...middleware.Middleware)` | — | Global middleware applied outermost to all routes. |

> Middleware ordering: broker-level (global) middleware is applied outermost and
> route-level middleware innermost (closest to the handler).

### `producer` — SNS publishing

`producer.New(client producer.SNSClient, opts ...producer.Option) (*producer.Producer, error)`

Returns `ErrNoSNSClient` for a nil client. `Publish` returns `ErrEmptyInput` for
a nil/empty input; `PublishBatch` returns `ErrEmptyInput` for an empty batch and
`ErrMaxBatchSize` for more than 10 entries.

| Option | Signature | Description |
| --- | --- | --- |
| `WithGroupIDGenerator` | `WithGroupIDGenerator(gen idgen.GroupIDGenerator)` | Auto-generate `MessageGroupId` for FIFO topics when not set. |
| `WithDeduplicationIDGenerator` | `WithDeduplicationIDGenerator(gen idgen.DeduplicationIDGenerator)` | Auto-generate `MessageDeduplicationId` for FIFO topics when not set. |

Helpers: `producer.BuildTopicARN(region, accountID, topicName string) string`.

> Auto-generation only applies to FIFO topics (ARNs ending in `.fifo`) and only
> when the corresponding ID is empty. Standard topics never receive
> auto-generated IDs, because SNS rejects them on non-FIFO topics.

### `typed` — generic type-safe handlers

- `typed.Codec[T]` — interface with `Encode(T) ([]byte, error)` and `Decode([]byte) (T, error)`.
- `typed.JSONCodec[T]` — JSON implementation of `Codec[T]`.
- `typed.WrapHandler[T](codec Codec[T], fn func(ctx, msg T) error) middleware.Handler` — adapts a typed handler into a standard `Handler`; a decode error is returned to the consumer.
- `typed.NewProducer[T](p *producer.Producer, codec Codec[T]) *typed.Producer[T]` — a typed producer that encodes before publishing.
- `typed.Producer[T].Publish(ctx, topicARN, value T, opts ...typed.PublishOption) (string, error)`.

| Publish option | Signature | Description |
| --- | --- | --- |
| `WithGroupID` | `WithGroupID(id string)` | Sets `MessageGroupId`. |
| `WithDeduplicationID` | `WithDeduplicationID(id string)` | Sets `MessageDeduplicationId`. |
| `WithAttributes` | `WithAttributes(attrs map[string]string)` | Sets message attributes. |

### `middleware` package

- `middleware.Handler` — `func(ctx context.Context, msg middleware.Message) error`.
- `middleware.Middleware` — `func(Handler) Handler`.
- `middleware.Chain(mws ...Middleware) Middleware` — composes middleware; the first is outermost.
- `middleware.Recovery(log *slog.Logger) Middleware` — recovers panics, logs the stack, returns `ErrPanic`.
- `middleware.Logging(log *slog.Logger) Middleware` — logs receipt, duration, and outcome.
- `middleware.Metrics(routeName string, opts ...MetricsOption) Middleware` — Prometheus counters, histogram, and inflight gauge.
- `middleware.OTel(routeName string, opts ...OTelOption) Middleware` — an OpenTelemetry span per message.

| Option | Signature | Default | Description |
| --- | --- | --- | --- |
| `WithMetricsRegisterer` | `WithMetricsRegisterer(r prometheus.Registerer)` | `prometheus.DefaultRegisterer` | Custom Prometheus registerer. |
| `WithTracerProvider` | `WithTracerProvider(tp trace.TracerProvider)` | global provider | Custom OpenTelemetry tracer provider. |

**Metrics emitted:** `loafer_messages_received_total`,
`loafer_messages_processed_total` (labeled by status), `loafer_messages_errors_total`,
`loafer_message_processing_duration_seconds` (histogram),
`loafer_messages_inflight` (gauge), and `loafer_messages_dlq_total` — all
labeled by route.

### `logger` — standard library slog constructors

- `logger.New() *slog.Logger` — structured, leveled output to stdout via `slog.TextHandler`.
- `logger.NewNoOp() *slog.Logger` — a discard-backed logger (silent).

> The library uses `*slog.Logger` everywhere and defines **no** custom logger
> interface. A `*slog.Logger` produced by a third-party bridge (for example zap
> via `zapslog`, or zerolog via a slog handler) is accepted directly, no adapter
> required.

### `idgen` — ID generation

Interfaces `idgen.GroupIDGenerator` and `idgen.DeduplicationIDGenerator` both
expose `Generate(ctx context.Context, fields map[string]string) (string, error)`.
A single concrete generator satisfies both.

| Constructor | Description |
| --- | --- |
| `NewKeyBased(opts ...Option)` | Deterministic ID from sorted field values, hashed with the configured algorithm. Returns `ErrEmptyFields` when no field is selected. |
| `NewRandom()` | Random UUID v4 on every call; ignores fields. |
| `NewComposite(opts ...Option)` | Joins selected field values with a separator (no hashing). |
| `NewCompositeWithSuffix(opts ...Option)` | Like `NewComposite`, plus a random numeric suffix from the configured range to spread load across partitions. |

| Option | Signature | Default | Description |
| --- | --- | --- | --- |
| `WithHashAlgorithm` | `WithHashAlgorithm(algorithm idgen.HashAlgorithm)` | `SHA256` | Digest for key-based hashing: `SHA256` or `FNV64`. |
| `WithSeparator` | `WithSeparator(separator string)` | `":"` | Separator joining key/value pairs. |
| `WithFields` | `WithFields(fields ...string)` | all fields | Whitelist of fields to include. |
| `WithSuffixRange` | `WithSuffixRange(min, max int)` | `[1, 20]` | Inclusive suffix range for `NewCompositeWithSuffix`. |

### `errors` — sentinel errors

The `errors` package exports sentinels matchable with `errors.Is`, including
`ErrNoRoute`, `ErrNoSQSClient`, `ErrNoHandler`, `ErrGetMessage`,
`ErrQueueResolve`, `ErrNoSNSClient`, `ErrEmptyInput`, `ErrMaxBatchSize`,
`ErrEmptyRegion`, `ErrEmptyQueueName`, `ErrInvalidOption`, `ErrEmptyFields`, and
`ErrPanic`. `errors.New(text string) error` and `errors.Wrap(sentinel, err error) error`
help build and combine errors while preserving `errors.Is` matching against both
causes.

---

## Scheduled Retry (FIFO)

The FIFO consumption path supports two per-route retry models, selected with
`router.WithRetryModel` (or the `router.WithScheduledRetry` shortcut):

| Model | Constant | Behavior |
| --- | --- | --- |
| Visibility (default) | `router.VisibilityRetryModel` | A failed message stays in the queue and its visibility timeout is extended until it succeeds or AWS SQS redrives it natively. This blocks the `MessageGroupId` until the message resolves. |
| Scheduled | `router.ScheduledRetryModel` | The consumer owns the whole retry lifecycle: on failure it schedules a delayed re-publish through AWS EventBridge Scheduler and deletes the original message so the `MessageGroupId` is unblocked immediately. |

When no retry model is configured a route uses `VisibilityRetryModel`, so
existing routes are unchanged. Selecting the Scheduled model on one route never
affects routes that use the Visibility model, and no scheduler client is
constructed or required unless a route opts in.

Under the Scheduled model, when a handler fails (returns an error **or** requests
backoff) the consumer reads a `retry_count` message attribute (default `0`),
computes `next = current + 1`, and either:

- **Schedules a retry** when `next <= MaxRetryCount`: it creates a one-time
  EventBridge Scheduler schedule that re-publishes the message to the queue after
  the computed backoff, then deletes the original.
- **Publishes to the DLQ** when `next > MaxRetryCount`: it sends the message to
  the configured DLQ, then deletes the original.

On success the message is simply deleted. The library performs **no** success-side
publishing; whether success means publishing to a topic, calling an API, or doing
nothing is the handler's responsibility.

### Architecture

```mermaid
graph TD
    classDef aws fill:#FF9900,stroke:#232F3E,stroke-width:2px,color:#232F3E;
    classDef compute fill:#232F3E,stroke:#FF9900,stroke-width:2px,color:#FFFFFF;
    classDef queue fill:#E2E3E5,stroke:#6C757D,stroke-width:2px,color:#232F3E;
    classDef dlq fill:#F8D7DA,stroke:#DC3545,stroke-width:2px,color:#721C24;
    classDef action fill:#D1E7DD,stroke:#0F5132,stroke-width:2px,color:#0F5132;

    PROD[Producer service<br/>e.g. Checkout]:::compute
    SNS{{SNS FIFO topic<br/>order_created.fifo}}:::aws
    SQS[(Entry_Queue &mdash; SQS FIFO<br/>inventory_order_created.fifo)]:::queue
    DLQ[(DLQ &mdash; SQS FIFO<br/>inventory_order_created_dlq.fifo)]:::dlq
    WORKER[Consumer service<br/>loafer-awsx worker]:::compute
    EBS((Amazon EventBridge<br/>Scheduler)):::aws
    DEL{{Delete from Entry_Queue<br/>frees the MessageGroupId}}:::action

    PROD -->|1. Publish event| SNS
    SNS -->|2. Route, raw delivery| SQS
    SQS -->|3. Poll / read batch| WORKER

    WORKER -->|4a. Success| DEL

    WORKER -->|4b. Transient error:<br/>compute backoff, create schedule,<br/>retry_count + 1| EBS
    EBS -.->|5. Fire time reached:<br/>re-publish to the queue| SQS
    WORKER -.->|Delete original now<br/>to free the MessageGroupId| DEL

    WORKER -->|4c. retry_count &gt; MaxRetryCount:<br/>publish directly to the DLQ| DLQ
```

**Why this architecture.** A FIFO queue guarantees ordering within a
`MessageGroupId` by delivering the group's messages one at a time. That guarantee
turns a single poison or transiently failing message into a *head-of-line block*:
under the default Visibility model the failed message stays in the queue and its
visibility timeout is extended, so every later message sharing its group waits
behind it until it finally succeeds or SQS redrives it. For a busy group, one bad
message can stall a whole stream of otherwise healthy work.

The Scheduled Retry model breaks that coupling by moving the wait *out of the
queue*. On failure the consumer hands the retry to EventBridge Scheduler (step
4b) and immediately deletes the original message (step 5, the dashed
delete-to-free edge). The `MessageGroupId` is unblocked right away, so the next
message in the group is processed while the failed one waits — off-queue — for its
backoff to elapse. When the schedule fires, EventBridge Scheduler re-publishes the
message to the same Entry_Queue with an incremented `retry_count`, and the cycle
repeats until the message either succeeds or exceeds `MaxRetryCount` and is routed
straight to the DLQ (step 4c).

**Why it is efficient.**

- **Group liveness:** a failing message no longer blocks its group. Throughput of
  a group is bounded by its healthy messages, not by its slowest failure.
- **No worker is held during backoff:** the delay lives in EventBridge Scheduler,
  not in a sleeping goroutine or an extended visibility timeout, so worker slots
  and in-flight-message limits are not consumed while waiting to retry.
- **Backoff without polling churn:** exponential backoff is expressed as a
  one-time schedule fire time, so the queue is not repeatedly re-reading and
  re-hiding the same message across attempts.
- **Deterministic, consumer-owned dead-lettering:** the DLQ decision is driven by
  the `retry_count` carried on the message and the configured `MaxRetryCount`,
  rather than SQS `maxReceiveCount` redrive, giving you explicit control over when
  a message is dead-lettered and what metadata it carries.
- **Self-cleaning schedules:** each retry schedule is created with
  `ActionAfterCompletion = DELETE`, so it removes itself after its single
  invocation and no schedule resources accumulate.

**Accepted tradeoffs.** Because the original is deleted before the retry is
delivered, the model provides **at-least-once** delivery (a delete failure after a
successful schedule/DLQ publish leaves the original for redelivery), and **strict
ordering within a `MessageGroupId` is not preserved for messages that are
retried** — the retried message rejoins the queue later, after messages that were
behind it. Design handlers to be idempotent. These tradeoffs are the deliberate
price paid for group liveness.

### Router configuration

`router.WithRetryModel(m router.RetryModel)` sets the model explicitly and
rejects any value other than `VisibilityRetryModel` or `ScheduledRetryModel`.
`router.WithScheduledRetry(opts ...router.ScheduledRetryOption)` is the usual
entry point: it sets the model to Scheduled **and** attaches a validated
configuration assembled from its sub-options.

| Sub-option | Signature | Description |
| --- | --- | --- |
| `WithSchedulerIdentity` | `WithSchedulerIdentity(targetQueueARN, executionRoleARN string)` | Required. The EventBridge Scheduler target (Entry_Queue) ARN and the execution role ARN the scheduler assumes. A missing item is named individually in the error. |
| `WithScheduledDLQ` | `WithScheduledDLQ(dlqQueueURL string)` | Required. The DLQ destination queue URL for exhausted messages. |
| `WithMaxRetryCount` | `WithMaxRetryCount(n int)` | Inclusive threshold before DLQ routing. Must be within `[0, 2147483647]`. |
| `WithBackoff` | `WithBackoff(base, max time.Duration)` | Base and maximum backoff delay. Each must be within `[1ms, 24h]` and `max >= base`. Base defaults to `1000ms` when unset. |

All Scheduled-model configuration is validated at `router.New` time. An invalid
or incomplete configuration returns an error wrapping
`errors.ErrScheduledRetryConfig` that identifies the offending value, so a
misconfigured route is never built and consumption never starts for it.
Configuring both `WithScheduledRetry` and the observe-only `WithDLQ` on the same
route is a configuration error, regardless of option order.

### Consumer wiring

> The broker does **not** forward the scheduler client or the metric hooks to
> the consumers it creates. Wire a Scheduled-model route through `consumer.New`
> directly and run it yourself.

`consumer.WithSchedulerClient(consumer.SchedulerClient)` supplies the EventBridge
Scheduler client. A concrete `*scheduler.Client` from
`github.com/aws/aws-sdk-go-v2/service/scheduler` satisfies the interface
directly. A Scheduled-model route given to a consumer without a scheduler client
fails fast at `Run` with `errors.ErrNoSchedulerClient` and never begins
consuming.

Three optional metric hooks report each outcome, each labeled by route name and
no-op when nil:

| Option | Signature | Emitted when |
| --- | --- | --- |
| `WithSuccessMetric` | `WithSuccessMetric(func(routeName string))` | A handler succeeds and the original message is deleted. |
| `WithRetryMetric` | `WithRetryMetric(func(routeName string))` | A retry schedule is created successfully. |
| `WithDeadLetterMetric` | `WithDeadLetterMetric(func(routeName string))` | An exhausted message is published to the DLQ successfully. |

### Example

```go
package main

import (
    "context"
    "errors"
    "log/slog"
    "time"

    "github.com/aws/aws-sdk-go-v2/service/scheduler"
    "github.com/aws/aws-sdk-go-v2/service/sqs"

    "github.com/silviolleite/loafer-awsx/conn"
    "github.com/silviolleite/loafer-awsx/consumer"
    "github.com/silviolleite/loafer-awsx/logger"
    "github.com/silviolleite/loafer-awsx/middleware"
    "github.com/silviolleite/loafer-awsx/router"
)

func main() {
    ctx := context.Background()
    log := logger.New()

    cfg, err := conn.New(ctx, conn.WithRegion("us-east-1"))
    if err != nil {
        log.Error("failed to build AWS config", slog.Any("error", err))
        return
    }

    sqsClient := sqs.NewFromConfig(cfg)
    schedulerClient := scheduler.NewFromConfig(cfg)

    handler := func(ctx context.Context, msg middleware.Message) error {
        // Return an error (or call msg.Backoff) to exercise the scheduled-retry path.
        return errors.New("transient failure")
    }

    route, err := router.New("orders.fifo", handler,
        router.WithRunMode(router.PerGroupID),
        router.WithScheduledRetry(
            router.WithSchedulerIdentity(
                "arn:aws:sqs:us-east-1:000000000000:orders.fifo",       // target Entry_Queue ARN
                "arn:aws:iam::000000000000:role/loafer-scheduler-role", // execution role ARN
            ),
            router.WithScheduledDLQ("https://sqs.us-east-1.amazonaws.com/000000000000/orders-dlq.fifo"),
            router.WithMaxRetryCount(5),
            router.WithBackoff(1*time.Second, 15*time.Minute),
        ),
    )
    if err != nil {
        log.Error("failed to build route", slog.Any("error", err))
        return
    }

    // The Scheduled model is wired through consumer.New directly, not broker.New:
    // the scheduler client and metric hooks are consumer options.
    c, err := consumer.New(sqsClient, route,
        consumer.WithLogger(log),
        consumer.WithSchedulerClient(schedulerClient),
        consumer.WithSuccessMetric(func(routeName string) { log.Info("success", slog.String("route", routeName)) }),
        consumer.WithRetryMetric(func(routeName string) { log.Info("retry", slog.String("route", routeName)) }),
        consumer.WithDeadLetterMetric(func(routeName string) { log.Info("dead-letter", slog.String("route", routeName)) }),
    )
    if err != nil {
        log.Error("failed to build consumer", slog.Any("error", err))
        return
    }

    if err := c.Run(ctx); err != nil {
        log.Error("consumer stopped", slog.Any("error", err))
    }
}
```

### Required AWS resources and IAM permissions

The Scheduled model creates one-time schedules and publishes to a DLQ, so the
identities involved need these permissions:

- **The consumer's credentials** need `scheduler:CreateSchedule` to create retry
  schedules and `iam:PassRole` on the execution role passed via
  `WithSchedulerIdentity` (EventBridge Scheduler requires the caller to be
  allowed to pass the role it will assume). They also need `sqs:SendMessage` to
  the DLQ so exhausted messages can be published.
- **The execution role** (the second argument to `WithSchedulerIdentity`) is the
  role EventBridge Scheduler assumes when a schedule fires. It needs
  `sqs:SendMessage` to the Entry_Queue so the re-published retry can be
  delivered, and its trust policy must allow `scheduler.amazonaws.com` to assume
  it.

Each one-time schedule is created with `ActionAfterCompletion = DELETE` and a
disabled flexible time window, so EventBridge Scheduler **self-cleans** the
schedule after its single invocation. The library never tracks or reaps schedule
resources.

### Entry_Queue must use explicit deduplication

A scheduled retry re-publishes the message with an unchanged body but an explicit
`MessageDeduplicationId` distinct from the original. The FIFO Entry_Queue **must
not** rely on content-based deduplication: it must be configured for explicit
deduplication (`MessageDeduplicationId` provided per message). If the queue used
content-based deduplication, the re-published retry would be discarded as a
duplicate of the original because the body is identical.

### Accepted tradeoffs

The Scheduled model deliberately trades two FIFO guarantees for group liveness:

- **At-least-once delivery.** The retry (schedule or DLQ publish) is created
  before the original is deleted. If the delete step fails after a successful
  schedule or publish, both the original and the re-published copy can be in
  play. Handlers must be **idempotent**.
- **In-group ordering is not preserved for retried messages.** Because a failed
  message is deleted and re-published later while the next message in the same
  `MessageGroupId` is processed immediately, strict ordering within a group does
  not hold for messages that are retried.
- **Handler-owned success publishing.** On success the library only deletes the
  message and emits the success metric. Any success-side publishing (to a topic,
  an API, or elsewhere) is the handler's responsibility.

---

## Benchmarks

The numbers below compare the per-message processing overhead of `loafer-awsx`
with [JustCodes/loafer-go](https://github.com/JustCodes/loafer-go) for both
standard and FIFO (PerGroupID) routing.

Both libraries are driven by the same in-memory SQS client, a no-op handler, and
an identical 8-worker pool, so the results isolate library overhead (dispatch,
worker routing, visibility bookkeeping) and deliberately exclude AWS and network
latency. In production, end-to-end throughput is dominated by SQS round-trips, so
treat these figures as a measure of framework cost, not real-world throughput.

| Mode | Library | Time/op | Throughput | Allocs/op | Bytes/op |
| --- | --- | ---: | ---: | ---: | ---: |
| Standard | `loafer-awsx` | ~5.4 µs | ~184k msg/s | 19 | 1,175 B |
| Standard | `loafer-go` | ~9.3 µs | ~105k msg/s | 19 | 1,245 B |
| FIFO | `loafer-awsx` | ~6.1 µs | ~165k msg/s | 22 | 1,518 B |
| FIFO | `loafer-go` | ~9.9 µs | ~100k msg/s | 22 | 1,589 B |

Medians of `-benchtime=2s -count=6` on an Intel Core i5-8265U (Go 1.26,
`linux/amd64`). Absolute numbers are machine-specific; the relative gap is what
matters, and both the code and methodology are reproducible.

Relative to `loafer-go`, on this run:

- **Standard queue:** ~41% lower latency, ~70% higher throughput, ~6% less
  memory per message, and the same number of allocations.
- **FIFO queue:** ~38% lower latency, ~62% higher throughput, ~5% less memory
  per message, and the same number of allocations.

The benchmarks live in their own module under [`benchmarks/`](benchmarks) (kept
separate so the competitor dependency never touches the library's `go.mod`). To
reproduce:

```bash
cd benchmarks
go test -run '^$' -bench . -benchtime=2s -count=6
```

---

## Acknowledgements

This project was inspired by [JustCodes/loafer-go](https://github.com/JustCodes/loafer-go).

---

## License

See [LICENSE](./LICENSE).

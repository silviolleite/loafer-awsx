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
- **AWS SDK:** `aws-sdk-go-v2` (SQS + SNS + EventBridge Scheduler)

---

## Table of Contents

1. [Architecture](#architecture)
   - [Context Diagram](#context-diagram)
   - [Container Diagram](#container-diagram)
2. [Installation](#installation)
3. [Quickstart](#quickstart)
4. [Examples](#examples)
5. [Configuration Reference](#configuration-reference)
6. [Client constructors](#client-constructors)
7. [IAM permissions](#iam-permissions)
8. [Scheduled Retry (FIFO)](#scheduled-retry-fifo)
9. [Batched deletes](#batched-deletes)
10. [Benchmarks](#benchmarks)
11. [Acknowledgements](#acknowledgements)
12. [License](#license)

---

## Architecture

`loafer-awsx` is a library, not a service. Your application imports it, wires
routes and a broker, and processes messages from AWS SQS while optionally
publishing to AWS SNS. The library also exposes Prometheus metrics and
OpenTelemetry spans for observability.

Diagram conventions, shared by every diagram in this README: rounded boxes are
your application code, rectangles are `loafer-awsx` components, cylinders are SQS
queues, hexagons are other AWS services, and parallelograms are observability
backends. Solid arrows are calls made by your code or the library; dashed arrows
are calls AWS makes on its own.

### Context Diagram

```mermaid
%%{init: {"theme": "neutral"}}%%
flowchart LR
    app([Your application])
    loafer[loafer-awsx]
    sqs[(SQS queues)]
    sns{{SNS topics}}
    ebs{{EventBridge Scheduler}}
    prom[/Prometheus/]
    otel[/OpenTelemetry/]

    app -->|routes, handlers, publish| loafer
    loafer -->|poll / delete / change visibility| sqs
    loafer -->|publish| sns
    loafer -->|Scheduled retry: create schedule| ebs
    ebs -.->|fire: re-publish| sqs
    loafer -->|metrics| prom
    loafer -->|spans| otel
```

### Container Diagram

The broker orchestrates one consumer per route: each `Route` binds a queue to a
handler, and its `Consumer` runs a worker pool that polls the matching SQS queue.
Publishing runs alongside through the `producer`.

```mermaid
%%{init: {"theme": "neutral"}}%%
flowchart TB
    app([Your application])
    broker[Broker]
    producer[Producer]

    subgraph routeVis[Route: Visibility retry model]
        consumerVis[Consumer + workers]
    end
    subgraph routeSched[Route: Scheduled retry model]
        consumerSched[Consumer + workers]
    end

    sqsVis[(SQS queue<br/>standard or FIFO)]
    sqsEntry[(SQS FIFO Entry_Queue)]
    dlq[(SQS DLQ)]
    sns{{SNS topic}}
    ebs{{EventBridge Scheduler}}

    app -->|broker.New| broker
    app -->|consumer.New| routeSched
    app -->|producer.New| producer
    broker -->|one consumer per route| routeVis

    consumerVis -->|poll / delete / change visibility| sqsVis
    consumerSched -->|poll / delete| sqsEntry
    consumerSched -->|retry: create one-time schedule| ebs
    ebs -.->|fire: re-publish with retry_count + 1| sqsEntry
    consumerSched -->|exhausted: publish| dlq

    producer -->|publish / publish batch| sns
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
| `client` | Constructors that turn an `aws.Config` into SQS/SNS/Scheduler clients with construction-time connectivity validation. |
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

Every component is configured through functional options following the same
pattern: pass `With*` options to each package's `New` constructor, which
validates them and rejects invalid values at construction time with a
descriptive error rather than accepting them silently. Option failures wrap
`errors.ErrInvalidOption`, and each package exposes its own sentinel errors
(matchable with `errors.Is`) for missing required inputs.

For the full, always-current list of options, signatures, defaults, and
sentinel errors for every package, see the Go Reference linked by the badge at
the top of this README. The notes below cover the conceptual behaviors that are
easy to miss from signatures alone.

- **`conn`** builds the shared `aws.Config` (region, credentials, endpoint,
  profile, retry). Region is required.
- **`router`** declares an immutable `Route` binding a queue to a handler, with
  options for worker-pool size, receive batching, long-poll wait, visibility
  timeout and extension limit, run mode, route middleware, DLQ
  observability, and batched deletes (`WithDeleteBatch()`, see
  [Batched deletes](#batched-deletes)).
- **`consumer`** and **`broker`** run the polling loop and orchestrate one
  consumer per route; both accept a `*slog.Logger`, a retry timeout, and
  middleware, and the broker adds a shutdown timeout.
- **`producer`** publishes to SNS (single and batch), with optional
  auto-generation of FIFO IDs.
- **`typed`**, **`idgen`**, **`middleware`**, **`logger`**, and **`errors`**
  provide generic type-safe handlers/producers, FIFO ID generation strategies,
  the middleware primitives and built-ins, `*slog.Logger` constructors, and the
  sentinel error set respectively.

**Run modes** (`router.Mode`): `Parallel` assigns messages to workers randomly;
`PerGroupID` hashes the `MessageGroupId` plus any custom group fields so a
group's messages are handled in order.

**Middleware ordering:** broker-level (global) middleware is applied outermost
and route-level middleware innermost (closest to the handler).

**DLQ observability** (`router.WithDLQ`) is **observe-only**. It does **not**
take a target ARN, and the library never moves, publishes, or deletes messages
for DLQ purposes. AWS SQS performs the actual redrive natively via the source
queue's redrive policy. The `maxReceiveCount` you pass must mirror that policy;
it is used only to detect when a message is exhausted so the consumer can emit
an Error log, the `loafer_messages_dlq_total` metric, and the optional `OnDLQ`
callback, while leaving the message in the queue.

**Logging:** the library uses the concrete `*slog.Logger` type everywhere and
defines **no** custom logger interface. This works without any adapter because
the extension point in `slog` is not the `*slog.Logger` type but the
`slog.Handler` interface it wraps. A `*slog.Logger` is just a thin struct that
delegates every record to its `slog.Handler`, and you build one with
`slog.New(handler)`. Any handler that implements `slog.Handler` therefore plugs
in directly:

```go
import (
    "log/slog"

    "go.uber.org/zap"
    "go.uber.org/zap/exp/zapslog" // zap's official slog bridge
)

zapLogger, _ := zap.NewProduction()
handler := zapslog.NewHandler(zapLogger.Core()) // implements slog.Handler
log := slog.New(handler)                        // -> *slog.Logger

broker.WithLogger(log) // accepted directly, no adapter
```

For backends without an official `slog` bridge (for example zerolog), any
third-party or hand-written `slog.Handler` works the same way. The translation
to the underlying backend happens inside the handler, so the library never needs
a logger adapter of its own.

---

## Client constructors

The `client` package turns an `aws.Config` (produced by `conn.New`) into the
service clients the rest of the library consumes, so your application never has
to import the AWS SDK for Go v2 service packages (`sqs`, `sns`, `scheduler`)
directly:

- **`client.NewSQS(ctx, cfg, opts...)`** returns a client for the broker and
  consumer.
- **`client.NewSNS(ctx, cfg, opts...)`** returns a client for the producer.
- **`client.NewScheduler(ctx, cfg, opts...)`** returns a client for the
  Scheduled Retry path (wired through `consumer.WithSchedulerClient`).

Each constructor validates connectivity **during construction**: before
returning, it issues a lightweight, read-only request (the "Ping") to confirm
the client can reach its AWS service with valid credentials, failing fast if it
cannot. The validation uses a dedicated timeout and retry budget that are
independent of the request retry policy carried by the `aws.Config` (defaults:
`3s` timeout, `2` retries).

Three functional options tune this behavior:

- **`WithPingTimeout(d)`** overrides the total time budget for connectivity
  validation (including retries). The duration must be positive.
- **`WithPingRetryLimit(n)`** overrides the number of retries performed beyond
  the initial attempt.
- **`WithoutConnectivityCheck()`** disables the connectivity validation
  entirely. Use it when the credentials lack the read-only permission the Ping
  requires, or to construct a client offline.

The existing Go Reference badge at the top of this README covers the full
signatures, defaults, and sentinel errors.

---

## IAM permissions

Each constructor's connectivity validation (Ping) issues an additional
read-only request beyond the operations the client uses at runtime, so the
caller's credentials need the Ping permission too — unless the check is disabled
with `WithoutConnectivityCheck()`. The tables below list the complete set of
permissions each client requires.

### SQS client (`client.NewSQS`, used by broker and consumer)

| Action | Required by | Notes |
| --- | --- | --- |
| `sqs:ReceiveMessage` | Runtime (consumer poll) | On the Entry_Queue. |
| `sqs:DeleteMessage` | Runtime | On the Entry_Queue. Also authorizes `DeleteMessageBatch` (used by `router.WithDeleteBatch()`); no extra permission is needed. |
| `sqs:ChangeMessageVisibility` | Runtime | Visibility extension during processing. |
| `sqs:GetQueueUrl` | Runtime | Resolve the queue URL from its name. |
| `sqs:SendMessage` | Runtime (Scheduled Retry only) | On the DLQ, to publish exhausted messages. |
| `sqs:ListQueues` | Construction (Ping) | Account-level; omit only if the check is disabled. |

### SNS client (`client.NewSNS`, used by producer)

| Action | Required by | Notes |
| --- | --- | --- |
| `sns:Publish` | Runtime | Covers both `Publish` and `PublishBatch`, on the target topic(s). |
| `sns:ListTopics` | Construction (Ping) | Account-level; omit only if the check is disabled. |

### EventBridge Scheduler client (`client.NewScheduler`, used by consumer Scheduled Retry)

| Action | Required by | Notes |
| --- | --- | --- |
| `scheduler:CreateSchedule` | Runtime | Create the one-time retry schedule. |
| `iam:PassRole` | Runtime | On the execution role passed via `WithSchedulerIdentity`. |
| `scheduler:ListSchedules` | Construction (Ping) | Omit only if the check is disabled. |

The **execution role** assumed by EventBridge Scheduler (the second argument to
`WithSchedulerIdentity`) is separate from the caller's credentials and needs
`sqs:SendMessage` on the Entry_Queue plus a trust policy allowing
`scheduler.amazonaws.com` to assume it.

> Because the Ping uses account-level `List*` permissions that scoped
> credentials may not grant, callers with least-privilege policies can either
> add the `List*` action or construct with `WithoutConnectivityCheck()`.

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
%%{init: {"theme": "neutral"}}%%
flowchart TB
    prod([Producer service<br/>e.g. Checkout])
    sns{{SNS FIFO topic<br/>order_created.fifo}}
    entry[(Entry_Queue: SQS FIFO<br/>inventory_order_created.fifo)]
    worker[loafer-awsx consumer]
    ebs{{EventBridge Scheduler}}
    dlq[(DLQ: SQS FIFO<br/>inventory_order_created_dlq.fifo)]

    prod -->|1. publish event| sns
    sns -->|2. deliver, raw delivery| entry
    entry -->|3. poll batch| worker

    worker -->|4a. error or backoff,<br/>retry_count + 1 &le; MaxRetryCount:<br/>create one-time schedule| ebs
    worker -->|4b. error or backoff,<br/>retry_count + 1 &gt; MaxRetryCount:<br/>publish to the DLQ| dlq
    worker -->|5. delete original on success,<br/>or after 4a / 4b succeeds:<br/>frees the MessageGroupId| entry
    ebs -.->|6. fire time reached:<br/>re-publish with retry_count + 1| entry
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
4a) and, once the schedule exists, deletes the original message (step 5). The
`MessageGroupId` is unblocked right away, so the next message in the group is
processed while the failed one waits — off-queue — for its backoff to elapse.
When the schedule fires, EventBridge Scheduler re-publishes the message to the
same Entry_Queue with an incremented `retry_count` (step 6), and the cycle
repeats until the message either succeeds or exceeds `MaxRetryCount` and is
routed straight to the DLQ (step 4b). If creating the schedule or publishing to
the DLQ fails, the original is not deleted and is redelivered after its
visibility timeout.

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

> The broker does **not** forward the scheduler client or the metrics recorder
> to the consumers it creates. Wire a Scheduled-model route through
> `consumer.New` directly and run it yourself.

`consumer.WithSchedulerClient(consumer.SchedulerClient)` supplies the EventBridge
Scheduler client. A concrete `*scheduler.Client` from
`github.com/aws/aws-sdk-go-v2/service/scheduler` satisfies the interface
directly. A Scheduled-model route given to a consumer without a scheduler client
fails fast at `Run` with `errors.ErrNoSchedulerClient` and never begins
consuming.

`consumer.WithMetrics(consumer.MetricsRecorder)` wires a single recorder whose
methods report each outcome, labeled by route name. The whole recorder is no-op
when nil, and a panicking method is recovered so the message outcome always
completes:

| Method | Emitted when |
| --- | --- |
| `IncSuccess(routeName string)` | A handler succeeds and the original message is deleted. |
| `IncRetry(routeName string)` | A retry schedule is created successfully. |
| `IncDeadLetter(routeName string)` | An exhausted message is published to the DLQ successfully. |
| `IncDLQ(routeName string)` | Under the Visibility model, a message is observed as exhausted (observe-only DLQ). |

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
    // the scheduler client and metrics recorder are consumer options.
    c, err := consumer.New(sqsClient, route,
        consumer.WithLogger(log),
        consumer.WithSchedulerClient(schedulerClient),
        consumer.WithMetrics(logMetrics{log: log}),
    )
    if err != nil {
        log.Error("failed to build consumer", slog.Any("error", err))
        return
    }

    if err := c.Run(ctx); err != nil {
        log.Error("consumer stopped", slog.Any("error", err))
    }
}

// logMetrics is a consumer.MetricsRecorder that logs each outcome. A production
// implementation would back these methods with the counters registered by the
// Metrics middleware.
type logMetrics struct{ log *slog.Logger }

func (m logMetrics) IncDLQ(route string)        { m.log.Info("dlq", slog.String("route", route)) }
func (m logMetrics) IncSuccess(route string)    { m.log.Info("success", slog.String("route", route)) }
func (m logMetrics) IncRetry(route string)      { m.log.Info("retry", slog.String("route", route)) }
func (m logMetrics) IncDeadLetter(route string) { m.log.Info("dead-letter", slog.String("route", route)) }
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

## Batched deletes

By default the consumer removes each committed message with its own
`DeleteMessage` call, issued by the worker that processed it. A route can opt in
to removing committed messages through the SQS `DeleteMessageBatch` API instead:

```go
route, err := router.New("orders", handler,
    router.WithDeleteBatch(),
)
```

The option takes no arguments and combines with every run mode and both retry
models. Routes without it keep the synchronous `DeleteMessage` path unchanged.

### How batches form

Batching is **opportunistic** and adds no delay. On a batched-delete route,
every Commit (handler success, or under the Scheduled model a successful retry
schedule or DLQ publish) hands the message's receipt handle to a per-route
queue and the worker moves on to its next message. A set of Sender goroutines
reads that queue. Each Sender blocks until a handle arrives, then takes
whatever other handles are **already** queued, without waiting, up to the AWS
limit of 10, and sends one `DeleteMessageBatch` request immediately.

There is no linger timer and no receive-cycle flush. A batch grows only while
earlier requests are in flight, so the only extra wait a delete can see is the
duration of a request already in progress. Under light load most requests carry
a single entry; as the delete rate rises, batches fill up.

The set of messages that get deleted is identical with and without the option:
handler errors, backoffs, held FIFO messages, and failed schedules or DLQ
publishes are never committed. Success, retry, dead-letter, and observe-only DLQ
signals are emitted under the same conditions on both paths. A failed delete
(request error or a failed entry in the response) is logged once per message
with `"failed to delete message"`, and the message reappears after its
visibility timeout, as with `DeleteMessage`.

Because the delete is asynchronous, the worker does not wait for it and cannot
react to its failure. On a FIFO `PerGroupID` route, if the delete of message A
fails, the next message B of the same group may already be processed, and A is
delivered again after B. The synchronous path only logs a failed delete too, so
the observable behavior is the same, but on a batched-delete route the library
can never hold or fail the group in response to a failed delete.

### When it pays off

A batch request is billed as one request regardless of how many entries it
carries, so the option cuts the number of delete requests by up to 10x. The
savings depend on volume:

- **High-volume queues.** At roughly $0.40 per million standard requests, the
  best case saves about $36 per 100M messages per month. At low volume, batches
  stay small and the savings are negligible. A route runs one Sender per worker,
  and each Sender sends about one request per round trip, so batches start to
  fill only when the delete rate exceeds `workers / RTT`. For example, 8 workers
  with a 5 ms round trip reach about 1,600 deletes per second before batches
  carry more than one entry. Below that rate, the option mainly takes the delete
  round trip off the worker and does not reduce the number of requests.
- **FIFO queues outside high-throughput mode.** These allow 300 API calls per
  second per action. With synchronous deletes, `DeleteMessage` caps you near 300
  messages per second; with batches of 10 the same quota covers up to 3,000.
  Enable the option when a FIFO route's delete rate approaches that quota.

Standard queues have nearly unlimited per-action throughput, so on them the
option is a request-cost optimization only. See the
[batched-delete benchmarks](#batched-delete-benchmarks) for measured request
counts and throughput.

### FIFO latency

On FIFO queues SQS does not deliver the next message of a `MessageGroupId`
until the in-flight one is deleted, so any delay before a delete adds directly
to per-group latency. Opportunistic batching never waits for a fuller batch,
and one group's delete never waits on another group's handler. The delete of a
committed message is sent as soon as a Sender is free; the most it can wait is
one delete request already in flight.

### Client requirement

`consumer.SQSClient` is unchanged. Batched deletes need a client that also
implements `consumer.BatchDeleteClient` (the `DeleteMessageBatch` method).
`*sqs.Client`, as returned by `client.NewSQS`, and `fake.SQSClient` already do.
If a batched-delete route runs with a client that does not, `Consumer.Run` (and
therefore the broker) fails fast with an error matching
`errors.ErrDeleteBatchUnsupported`, before resolving the queue URL or starting
any goroutine. It never falls back silently to single deletes.

### Senders and backpressure

Each batched-delete route runs one Sender per worker (`WithWorkerPoolSize`), so
delete parallelism never drops below the synchronous path, where each worker
issues its own delete. The pending-delete queue holds `WorkerPoolSize ×
MaxMessages` receipt handles (50 with the defaults of 5 workers and 10
messages). When it is full, a committing worker blocks until a Sender takes an
entry, which keeps memory bounded and mirrors a worker blocking on its own
`DeleteMessage`.

### Shutdown and delivery guarantees

On graceful shutdown the consumer keeps accepting deletes until every worker has
exited, then sends every remaining pending delete before `Consumer.Run`
returns. Delete requests use a context that is not canceled by the run context,
and each request is bounded by a 5s timeout, so a stalled request cannot hang
shutdown. No Sender goroutine survives `Run`.

Because the delete is asynchronous to the worker, a message counts as handled
before it is removed from the queue. If the process stops **without** a
graceful shutdown (crash, `SIGKILL`), queued and in-flight deletes are lost and
those messages are redelivered after their visibility timeout. This stays
within the library's at-least-once contract: handlers must be **idempotent**.

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
| Standard | `loafer-awsx` | ~5.6 µs | ~178k msg/s | 19 | 1,207 B |
| Standard | `loafer-go` | ~8.5 µs | ~117k msg/s | 19 | 1,245 B |
| FIFO | `loafer-awsx` | ~6.5 µs | ~154k msg/s | 22 | 1,557 B |
| FIFO | `loafer-go` | ~8.7 µs | ~114k msg/s | 22 | 1,589 B |

Medians of 24 samples (four runs of `-benchtime=2s -count=6`) on an Intel Core
i5-8265U (Go 1.26, `linux/amd64`). Absolute numbers are machine-specific; the
relative gap is what matters, and both the code and methodology are
reproducible.

Relative to `loafer-go`:

- **Standard queue:** ~34% lower latency, ~52% higher throughput, ~3% less
  memory per message, and the same number of allocations.
- **FIFO queue:** ~26% lower latency, ~35% higher throughput, ~2% less memory
  per message, and the same number of allocations.

The gap moved between runs (32–48% lower latency for standard, 25–36% for
FIFO), but `loafer-awsx` was faster in every run.

The benchmarks live in their own module under [`benchmarks/`](benchmarks) (kept
separate so the competitor dependency never touches the library's `go.mod`). To
reproduce:

```bash
cd benchmarks
go test -run '^$' -bench . -benchtime=2s -count=6
```

### Batched-delete benchmarks

These benchmarks compare the synchronous `DeleteMessage` path with
[`WithDeleteBatch()`](#batched-deletes) for standard and FIFO (`PerGroupID`, 64
message groups) routes. They use the same in-memory SQS client, no-op handler,
and 8-worker pool as above. With `latency=2ms`, every delete request
(`DeleteMessage` or `DeleteMessageBatch`) is delayed by 2 ms to stand in for a
network round trip. `delete-calls/msg` is the number of delete requests per
message; `entries/batch` is the average number of entries per
`DeleteMessageBatch` request.

| Benchmark | Delete latency | Time/op | Throughput | delete-calls/msg | entries/batch |
| --- | --- | ---: | ---: | ---: | ---: |
| StandardSync | 0 | 5.36 µs | 186,576 msg/s | 1.000 | – |
| StandardBatch | 0 | 3.86 µs | 259,312 msg/s | 0.747 | 1.34 |
| FIFOSync | 0 | 6.65 µs | 150,328 msg/s | 1.000 | – |
| FIFOBatch | 0 | 5.33 µs | 187,490 msg/s | 0.861 | 1.16 |
| StandardSync | 2 ms | 344.8 µs | 2,900 msg/s | 1.000 | – |
| StandardBatch | 2 ms | 34.6 µs | 28,944 msg/s | 0.100 | 9.99 |
| FIFOSync | 2 ms | 342.7 µs | 2,918 msg/s | 1.000 | – |
| FIFOBatch | 2 ms | 34.2 µs | 29,209 msg/s | 0.100 | 9.99 |

Medians of 24 samples (four runs of `-benchtime=2s -count=6`) on an Intel Core
i5-8265U (4 cores / 8 threads, laptop), Go 1.26.6, `linux/amd64`.

- **With 2 ms delete latency:** batches were almost always full (9.99 entries),
  so delete requests dropped by 90% and throughput rose about 10x on both
  standard and FIFO routes. The synchronous path is bound by 8 workers each
  blocking on one delete. Batching also used about 170 B and 4 allocations less
  per message. Run-to-run spread was 11% or less.
- **With zero delete latency:** the result is not stable, so the medians in
  the table hide a split. In three of four runs, batches held 1.16–1.37
  entries and batching took 20–47% less time per message than the
  synchronous path. In the fourth run, batches barely formed (1.06–1.13
  entries) and batching took 13–25% more time. Either way it costs about
  295–380 B and 5–7 allocations more per message. Do not expect a speedup
  from batching when deletes are this cheap.

In short, `WithDeleteBatch()` pays off most when delete round trips are real,
as with any networked SQS endpoint, and volume is high. With in-memory,
zero-latency deletes it can be faster or slower per message, and it always
uses slightly more memory and allocations. To reproduce:

```bash
cd benchmarks
go test -run '^$' -bench 'Delete' -benchtime=2s -count=6
```

---

## Acknowledgements

This project was inspired by [JustCodes/loafer-go](https://github.com/JustCodes/loafer-go).

---

## License

See [LICENSE](./LICENSE).

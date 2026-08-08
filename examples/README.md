# loafer-awsx Examples

Runnable examples for the loafer-awsx library, wired to run locally against
[LocalStack](https://www.localstack.cloud/) with infrastructure provisioned by
the Terraform config in [`terraform/`](./terraform).

Each example is a standalone `package main` under this directory:

| Example | Directory | What it shows |
| --- | --- | --- |
| Basic | [`basic/`](./basic) | Standard SQS queue consumption with a simple handler. |
| FIFO | [`fifo/`](./fifo) | Ordered consumption in `PerGroupID` mode with custom group fields. |
| FIFO Scheduled Retry | [`fifo-scheduled-retry/`](./fifo-scheduled-retry) | FIFO route using `WithScheduledRetry` (scheduler identity, DLQ, max retry count, backoff) wired with `WithSchedulerClient` and a failing handler. |
| Local Scheduler Simulator | [`localscheduler/`](./localscheduler) | LocalStack-only helper that fires the retry schedules EventBridge Scheduler would fire, closing the scheduled-retry loop locally. |
| Typed | [`typed/`](./typed) | Generic, type-safe message handling via `typed.WrapHandler` + `typed.JSONCodec`. |
| Middleware | [`middleware/`](./middleware) | Recovery, logging, Prometheus metrics, and OpenTelemetry tracing middleware (OTLP → OTel Collector → Jaeger). |
| Producer | [`producer/`](./producer) | Single and batch publishing to standard and FIFO SNS topics. |

## Prerequisites

- **Docker** (with the `docker compose` plugin) — runs LocalStack.
- **Terraform** `>= 1.3` — provisions the SQS queues, SNS topics, and subscriptions.
- **Go** `1.26+` — builds and runs the example programs.
- **curl** — used by the `make up` health check.

## Quick start

From this `examples/` directory:

```bash
make up          # start LocalStack and wait until SQS + SNS are ready
make provision   # terraform init + apply (creates queues, topics, subscriptions)
make run-basic   # run an example (Ctrl+C to stop a consumer)
```

When you are done:

```bash
make destroy     # tear down the Terraform-managed resources
make down        # stop LocalStack (use `make down-clean` to also drop its volume)
```

Run `make help` to see every available target.

## 1. Bring up LocalStack

The examples use the existing Docker Compose file at
[`terraform/docker-compose.yml`](./terraform/docker-compose.yml), which starts a
LocalStack container named `loafer-awsx-localstack` exposing SQS and SNS on
`127.0.0.1:4566`.

Using the Makefile (recommended — it waits for the services to become healthy):

```bash
make up
```

Or manually:

```bash
docker compose -f terraform/docker-compose.yml up -d
# wait until SQS + SNS report "available":
curl -s http://localhost:4566/_localstack/health
```

Stop it with `make down` (keeps the data volume) or `make down-clean`
(`docker compose ... down -v`, which also removes the LocalStack data volume).

## 2. Provision resources with Terraform

The Terraform config in [`terraform/`](./terraform) creates everything the
consumer and producer examples expect:

- Standard SQS queue `example-basic-queue`
- FIFO SQS queue `example-fifo-queue.fifo`
- FIFO Scheduled Retry Entry_Queue `example-scheduled.fifo` (explicit deduplication)
- FIFO Scheduled Retry DLQ `example-scheduled-dlq.fifo`
- Standard SNS topic `example-standard-topic`
- FIFO SNS topic `example-fifo-topic.fifo`
- FIFO Scheduled Retry SNS topic `example-scheduled-topic.fifo`
- SNS-to-SQS subscriptions (raw delivery) wiring each topic to its queue
- IAM execution role `example-scheduler-role` (assumed by EventBridge Scheduler)
  — gated behind `enable_scheduler_resources`, which defaults to `true`

> The bundled [`docker-compose.yml`](./terraform/docker-compose.yml) enables the
> `iam` and `scheduler` services (`SERVICES=sqs,sns,iam,scheduler`), so
> `make provision` creates the IAM role too. If you run against a LocalStack
> instance that does not expose IAM (an older `SERVICES=sqs,sns` setup returns a
> 501 on `CreateRole`), disable the role with
> `TF_VAR_enable_scheduler_resources=false make provision`.
>
> **LocalStack's EventBridge Scheduler is mock-only:** `CreateSchedule` succeeds
> but the schedule never actually fires to re-publish the message to the target
> SQS queue. Against LocalStack you can observe the schedule-created,
> delete-original, and retry-metric path, but the real re-delivery and DLQ loop
> only happen against real AWS.

Using the Makefile:

```bash
make provision   # runs `terraform init` then `terraform apply -auto-approve`
```

Or manually:

```bash
cd terraform
terraform init
terraform apply -auto-approve
```

Tear it down with `make destroy` (or `cd terraform && terraform destroy -auto-approve`).

## 3. Environment variables and configuration

The example programs **do not read environment variables**. For simplicity they
hardcode LocalStack-friendly values in each `main.go`:

| Setting | Hardcoded value | Where |
| --- | --- | --- |
| Region | `us-east-1` | all examples |
| Credentials | access key `test`, secret key `test` | all examples |
| Endpoint | `http://localhost:4566` | all examples |
| Account ID | `000000000000` | producer (used to build topic ARNs) |

These match the LocalStack-friendly defaults in
[`terraform/variables.tf`](./terraform/variables.tf), so no overrides are needed
for local development.

The Terraform config, on the other hand, **does** accept overrides. To target a
real AWS account or a different region, override the variables via `-var`, a
`*.tfvars` file, or `TF_VAR_`-prefixed environment variables. For example:

```bash
export TF_VAR_region=eu-west-1
export TF_VAR_account_id=123456789012
export TF_VAR_localstack_endpoint=""   # omit the endpoint override for real AWS
```

**To point the example programs at real AWS**, edit the connection options in the
relevant `main.go`:

- Drop `conn.WithEndpoint(...)` so the SDK uses the real AWS endpoints.
- Drop `conn.WithAccessKey("test", "test")` to fall back to the default AWS
  credential chain (environment variables, shared profiles, IAM roles).
- Update `conn.WithRegion(...)` (and, in the producer, the `region` and
  `accountID` constants) to match your account.

## 4. Running the examples

Run each example from this `examples/` directory. Consumer examples block until
you interrupt them with `Ctrl+C`; the producer runs once and exits.

| Command | Consumes / publishes | Provisioned by Terraform? |
| --- | --- | --- |
| `make run-basic` | queue `example-basic-queue` | ✅ yes |
| `make run-fifo` | FIFO queue `example-fifo-queue.fifo` | ✅ yes |
| `make run-fifo-scheduled-retry` | FIFO queue `example-scheduled.fifo` (+ DLQ) | ✅ yes — but EventBridge Scheduler is mock-only on LocalStack; see note below |
| `make run-localscheduler` | fires schedules → SQS `example-scheduled.fifo` | ✅ yes — LocalStack demo helper; see note below |
| `make run-typed` | queue `example-typed-queue` | ⚠️ **no** — see note below |
| `make run-middleware` | queue `example-middleware-queue` | ⚠️ **no** — see note below |
| `make run-producer` | topics `example-standard-topic`, `example-fifo-topic.fifo`, `example-scheduled-topic.fifo` | ✅ yes |

Equivalent manual commands (from `examples/`): `go run ./basic`, `go run ./fifo`,
`go run ./fifo-scheduled-retry`, `go run ./localscheduler`, `go run ./typed`,
`go run ./middleware`, `go run ./producer`.

### Basic

```bash
make run-basic
```

Consumes `example-basic-queue` with a simple handler that JSON-decodes each
message body and logs it. Publish messages to it via the standard topic with
`make run-producer`.

### FIFO

```bash
make run-fifo
```

Consumes `example-fifo-queue.fifo` in `router.PerGroupID` mode, deriving the
per-group ordering key from the `tenant_id` and `order_id` message attributes so
messages in the same group are processed in order. The producer example
publishes matching FIFO messages.

### FIFO Scheduled Retry

```bash
make run-fifo-scheduled-retry
```

Consumes the FIFO queue `example-scheduled.fifo` with the **Scheduled Retry
model** instead of the default in-place (visibility) retry. The route is built
with `router.WithScheduledRetry(...)`, which sets the scheduler identity
(`WithSchedulerIdentity` — the target Entry_Queue ARN and the EventBridge
Scheduler execution role), the DLQ destination (`WithScheduledDLQ`), the max
retry count (`WithMaxRetryCount(3)`), and the backoff bounds
(`WithBackoff(1s, 30s)`). The scheduler client is wired through the consumer
option `consumer.WithSchedulerClient(...)`, and the success, retry, and
dead-letter metric hooks log each outcome. The handler always returns an error,
so every message exercises the scheduled-retry path: the consumer deletes the
original message and asks EventBridge Scheduler to re-publish it after the
computed backoff, dead-lettering the message once its retry count exceeds the
configured maximum.

Full cycle:

```bash
make provision                 # creates the Entry_Queue, DLQ, topic, and role
make run-fifo-scheduled-retry  # start the scheduled-retry consumer (Ctrl+C to stop)
# in another terminal:
make run-producer              # seed example-scheduled.fifo via the scheduled topic
```

The producer publishes a small FIFO batch to `example-scheduled-topic.fifo`,
which fans out to the `example-scheduled.fifo` Entry_Queue. The consumer then
picks each message up, fails it, and drives it through the scheduled-retry and
(after exhausting retries) DLQ paths.

> **Note:** the Entry_Queue (`example-scheduled.fifo`), the DLQ
> (`example-scheduled-dlq.fifo`), and the seed topic
> (`example-scheduled-topic.fifo`) **are** provisioned by the default
> `make provision` (they are SQS/SNS resources LocalStack Community supports).
> The execution role (`example-scheduler-role`) is created **only when
> `enable_scheduler_resources = true`**, because LocalStack Community does not
> implement IAM (`CreateRole` returns a 501). Their identifiers are exposed as
> Terraform outputs (`scheduled_queue_arn`, `scheduled_dlq_url`,
> `scheduler_role_arn`, `scheduled_topic_arn`), which you can use to fill in the
> placeholder ARNs/URLs in
> [`fifo-scheduled-retry/main.go`](./fifo-scheduled-retry/main.go). Key details:
>
> - the **Entry_Queue** is created with **explicit deduplication**
>   (`content_based_deduplication = false`) — the re-published retry carries an
>   explicit `MessageDeduplicationId`, so the queue must not rely on
>   content-based deduplication or the retry would be dropped as a duplicate;
> - the **DLQ** receives messages that exhaust their retries; and
> - the **execution role** is what EventBridge Scheduler assumes to deliver each
>   schedule (it grants `sqs:SendMessage` on the Entry_Queue; the consumer
>   identity separately needs `scheduler:CreateSchedule` and `iam:PassRole`).
>
> EventBridge Scheduler and IAM are required for the full scheduled-retry path.
> The bundled docker-compose enables both services, so `make up` +
> `make provision` create every resource and the consumer's `CreateSchedule`
> calls succeed. However, **LocalStack's EventBridge Scheduler is mock-only** —
> it accepts `CreateSchedule` but never fires the schedule, so on its own the
> message is not re-published to the Entry_Queue and the retry/DLQ loop does not
> complete locally. You will see the consumer create the schedule, delete the
> original message, and emit the retry metric, and then nothing further.
>
> **To close the loop locally, run the [local scheduler simulator](#local-scheduler-simulator-localstack-only)**
> (`make run-localscheduler`) alongside the consumer. It emulates what
> EventBridge Scheduler does when a schedule fires: it polls the schedules the
> consumer created and, once each one's fire time elapses, performs the SQS
> `SendMessage` the schedule describes and deletes the schedule. The message
> then reappears on the Entry_Queue, its `retry_count` climbs on each failure,
> and once it exceeds `WithMaxRetryCount` the consumer dead-letters it to
> `example-scheduled-dlq.fifo`. Against **real AWS you do not run the
> simulator** — EventBridge Scheduler fires the schedules itself.
>
> If the consumer instead logs `failed to create retry schedule ... Service
> 'scheduler' is not enabled`, your running LocalStack container predates this
> `SERVICES` change — recreate it with `make down-clean && make up` so it picks
> up `SERVICES=sqs,sns,iam,scheduler`.

### Local scheduler simulator (LocalStack only)

```bash
make run-localscheduler
```

A development-only helper that emulates AWS EventBridge Scheduler firing its
schedules, since [LocalStack only mocks the Scheduler](https://docs.localstack.cloud/aws/services/scheduler/)
and never invokes targets. It polls `scheduler list-schedules`, and for every
schedule whose one-time `at(...)` fire time has elapsed it decodes the universal
SQS SendMessage target (`Target.Input`), performs that `SendMessage` against the
Entry_Queue, and deletes the schedule (mirroring `ActionAfterCompletion = DELETE`).
`SendMessage` is fully supported by LocalStack, so this is what makes the retry
re-delivery and eventual DLQ hand-off observable locally.

To watch the full FIFO Scheduled Retry cycle on LocalStack, use three terminals:

```bash
make run-fifo-scheduled-retry   # terminal 1: the scheduled-retry consumer
make run-localscheduler         # terminal 2: the schedule-firing simulator
make run-producer               # terminal 3: seed the Entry_Queue via the topic
```

You will see the consumer fail each message, create a schedule, delete the
original, and emit the retry metric; the simulator re-publish the message when
its backoff elapses; the consumer's `retry_count` climb on each pass; and,
finally, the message land in the DLQ with a `dead-letter:` log once it exceeds
`WithMaxRetryCount`. This helper is unnecessary against real AWS.

### Typed

```bash
make run-typed
```

Consumes `example-typed-queue` and decodes each body into an `OrderPlaced`
struct via `typed.JSONCodec`.

> **Note:** `example-typed-queue` is **not** created by the current Terraform
> config. Create it yourself (for example
> `aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name example-typed-queue`),
> add it to the Terraform config, or change `queueName` in
> [`typed/main.go`](./typed/main.go) to an existing queue such as
> `example-basic-queue` before running.

### Middleware

```bash
make run-middleware
```

Consumes `example-middleware-queue` with a full middleware stack (recovery,
logging, Prometheus metrics, OpenTelemetry tracing). It serves Prometheus
metrics at <http://localhost:9090/metrics> and exports OpenTelemetry spans over
OTLP to an OpenTelemetry Collector, which forwards them to Jaeger for viewing at
<http://localhost:16686>.

> **Note:** `example-middleware-queue` is **not** created by the current
> Terraform config. Create it, add it to the Terraform config, or point
> `queueName` in [`middleware/main.go`](./middleware/main.go) at an existing
> queue before running.

#### Tracing backend: OpenTelemetry Collector + Jaeger

The example exports one span per message over **OTLP HTTP** to an
[OpenTelemetry Collector](https://opentelemetry.io/docs/collector/), which
forwards the traces to [Jaeger](https://www.jaegertracing.io/). Both run as
containers defined in
[`middleware/docker-compose.yml`](./middleware/docker-compose.yml), with the
collector pipeline configured in
[`middleware/otel-collector-config.yaml`](./middleware/otel-collector-config.yaml).

The data flows as follows:

```
middleware example (host)  --OTLP HTTP :4318-->  otel-collector
                                                     |
                                         --OTLP gRPC jaeger:4317-->  jaeger
                                                                       |
                                                       Jaeger UI  http://localhost:16686
```

> The Collector's native Jaeger exporter was
> [removed in 2023](https://opentelemetry.io/blog/2023/jaeger-exporter-collector-migration/)
> because Jaeger ingests OTLP directly, so the collector forwards spans to
> Jaeger over OTLP (`COLLECTOR_OTLP_ENABLED=true`) rather than the legacy
> Jaeger protocol.

Bring the tracing backend up before running the example:

```bash
make observability-up   # start the OTel Collector + Jaeger
make run-middleware      # run the consumer; spans are exported as messages are processed
```

Then open the Jaeger UI at <http://localhost:16686> and select the
`loafer-awsx-middleware-example` service to inspect the spans. Publish messages
to `example-middleware-queue` (for example with a producer) so the handler runs
and emits spans.

Tear the backend down with `make observability-down` (or
`make observability-down-clean` to also remove volumes).

The exporter connects lazily and retries in the background, so the example still
runs if the collector is not up — you simply will not see spans in Jaeger until
the backend is running.

Ports used by the stack:

| Port | Component | Purpose |
| --- | --- | --- |
| `4317` | OTel Collector | OTLP gRPC receiver |
| `4318` | OTel Collector | OTLP HTTP receiver (used by the example) |
| `16686` | Jaeger | Jaeger web UI |

### Producer

```bash
make run-producer
```

Publishes single and batch messages to the standard topic
`example-standard-topic`, the FIFO topic `example-fifo-topic.fifo`, and the FIFO
Scheduled Retry topic `example-scheduled-topic.fifo` (which seeds the
`example-scheduled.fifo` Entry_Queue), then exits. For FIFO topics the
`MessageGroupId` and `MessageDeduplicationId` are auto-generated from the message
attributes.

To observe end-to-end delivery, start a consumer (e.g. `make run-basic` for the
standard queue, `make run-fifo` for the FIFO queue, or
`make run-fifo-scheduled-retry` for the scheduled-retry queue) in one terminal,
then run `make run-producer` in another.

### run-all

```bash
make run-all
```

A convenience helper. Because consumer examples block until interrupted, they
cannot be chained sequentially, so `run-all` runs the producer example (the one
process that publishes and exits). Start a consumer in a separate terminal first
to watch messages arrive.

## Makefile targets

| Target | Description |
| --- | --- |
| `help` | Show the self-documented target list (default goal). |
| `up` | Start LocalStack and wait for SQS + SNS to be ready. |
| `down` | Stop LocalStack (keeps the data volume). |
| `down-clean` | Stop LocalStack and remove its data volume (`down -v`). |
| `observability-up` | Start the OTel Collector + Jaeger tracing backend (middleware example). |
| `observability-down` | Stop the OTel Collector + Jaeger tracing backend. |
| `observability-down-clean` | Stop the tracing backend and remove its volumes. |
| `provision` | `terraform init` then `terraform apply -auto-approve`. |
| `destroy` | `terraform destroy -auto-approve`. |
| `run-basic` | Run the basic consumer example. |
| `run-fifo` | Run the FIFO consumer example. |
| `run-fifo-scheduled-retry` | Run the FIFO Scheduled Retry consumer example. |
| `run-localscheduler` | Run the local EventBridge Scheduler simulator (LocalStack demo only). |
| `run-typed` | Run the typed consumer example. |
| `run-middleware` | Run the middleware consumer example (metrics on `:9090`). |
| `run-producer` | Run the producer example (publishes then exits). |
| `run-all` | Helper that runs the producer example. |

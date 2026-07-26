# loafer-awsx Examples

Runnable examples for the loafer-awsx library, wired to run locally against
[LocalStack](https://www.localstack.cloud/) with infrastructure provisioned by
the Terraform config in [`terraform/`](./terraform).

Each example is a standalone `package main` under this directory:

| Example | Directory | What it shows |
| --- | --- | --- |
| Basic | [`basic/`](./basic) | Standard SQS queue consumption with a simple handler. |
| FIFO | [`fifo/`](./fifo) | Ordered consumption in `PerGroupID` mode with custom group fields. |
| Typed | [`typed/`](./typed) | Generic, type-safe message handling via `typed.WrapHandler` + `typed.JSONCodec`. |
| Middleware | [`middleware/`](./middleware) | Recovery, logging, Prometheus metrics, and OpenTelemetry tracing middleware. |
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
- Standard SNS topic `example-standard-topic`
- FIFO SNS topic `example-fifo-topic.fifo`
- SNS-to-SQS subscriptions (raw delivery) wiring each topic to its queue

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
| `make run-typed` | queue `example-typed-queue` | ⚠️ **no** — see note below |
| `make run-middleware` | queue `example-middleware-queue` | ⚠️ **no** — see note below |
| `make run-producer` | topics `example-standard-topic`, `example-fifo-topic.fifo` | ✅ yes |

Equivalent manual commands (from `examples/`): `go run ./basic`, `go run ./fifo`,
`go run ./typed`, `go run ./middleware`, `go run ./producer`.

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
logging, Prometheus metrics, OpenTelemetry tracing) and serves Prometheus
metrics at <http://localhost:9090/metrics>.

> **Note:** `example-middleware-queue` is **not** created by the current
> Terraform config. Create it, add it to the Terraform config, or point
> `queueName` in [`middleware/main.go`](./middleware/main.go) at an existing
> queue before running.

### Producer

```bash
make run-producer
```

Publishes single and batch messages to the standard topic
`example-standard-topic` and the FIFO topic `example-fifo-topic.fifo`, then
exits. For FIFO topics the `MessageGroupId` and `MessageDeduplicationId` are
auto-generated from the message attributes.

To observe end-to-end delivery, start a consumer (e.g. `make run-basic` for the
standard queue, `make run-fifo` for the FIFO queue) in one terminal, then run
`make run-producer` in another.

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
| `provision` | `terraform init` then `terraform apply -auto-approve`. |
| `destroy` | `terraform destroy -auto-approve`. |
| `run-basic` | Run the basic consumer example. |
| `run-fifo` | Run the FIFO consumer example. |
| `run-typed` | Run the typed consumer example. |
| `run-middleware` | Run the middleware consumer example (metrics on `:9090`). |
| `run-producer` | Run the producer example (publishes then exits). |
| `run-all` | Helper that runs the producer example. |

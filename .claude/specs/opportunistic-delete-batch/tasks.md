# Implementation Plan: Opportunistic Delete Batch

## Overview

This plan implements opt-in batched deletes inside the existing `router`, `consumer`,
`errors`, and `fake` packages, then adds benchmarks in the separate `benchmarks/` module and
updates the documentation. It builds bottom-up so every step compiles and is exercised before
the next depends on it: the sentinel error, route option, optional client interface, and fake
first; then the self-contained `deleteBatcher` with its property and example tests; then the
`dispatcher` and `Consumer.Run` wiring with end-to-end properties and a LocalStack
integration test; then benchmarks; then documentation. No orphaned code is introduced: the
batcher is consumed by the dispatcher, the dispatcher branch is enabled by `Consumer.Run`,
and `Consumer.Run` reads the route option.

The Synchronous_Delete path stays unchanged and remains the default. Tests follow
`.claude/rules/go-tests.md` (table-driven plus `pgregory.net/rapid`, deterministic, no
comments except the property tag, 95% minimum coverage). Batcher-level tests use an internal
`package consumer` file because `deleteBatcher` is unexported, matching `dispatch_test.go`
and `visibility_test.go`. Delegate test writing to the `go-test-engineer` agent and review to
`go-test-reviewer`.

## Tasks

- [x] 1. Foundation: sentinel, route option, client interface, and fake
  - [x] 1.1 Add the `ErrDeleteBatchUnsupported` sentinel
    - In `errors/errors.go` add `ErrDeleteBatchUnsupported = New("sqs client does not support DeleteMessageBatch")` to the existing `var` block with a doc comment
    - Extend `errors/errors_test.go` to assert the sentinel is distinct from every existing sentinel and matches through `errors.Wrap`
    - _Requirements: 3.3, 12.7_

  - [x] 1.2 Add `router.WithDeleteBatch` and `Route.DeleteBatch`
    - In `router/router.go` add the unexported `deleteBatch bool` field to `Route` (let `make format` place it via `fieldalignment`) and the getter `func (r *Route) DeleteBatch() bool` with a doc comment
    - In `router/options.go` add `func WithDeleteBatch() Option` that sets the field and never returns an error, with the doc comment from the design
    - Extend `router/router_test.go` with table cases: default is false; option sets true; option combines without error with `WithScheduledRetry(...)`, `WithDLQ(...)`, `WithRunMode(PerGroupID)`, and `WithRetryModel(VisibilityRetryModel)`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 12.7_

  - [x] 1.3 Add the optional `consumer.BatchDeleteClient` interface
    - In `consumer/client.go` add `BatchDeleteClient` with the `DeleteMessageBatch` signature of `*sqs.Client` and doc comments; leave `SQSClient` unchanged
    - Add `_ BatchDeleteClient = (*sqs.Client)(nil)` to the existing compile-time assertion block
    - _Requirements: 2.1, 3.1, 3.2, 12.7_

  - [x] 1.4 Extend `fake.SQSClient` with `DeleteMessageBatch`
    - In `fake/sqs_client.go` add the `DeleteMessageBatchFunc` field, a `deleteMessageBatchCalls` slice guarded by `mu`, the `DeleteMessageBatch` method (records the input; delegates to the func when set; otherwise returns an output listing every entry `Id` in `Successful` and a nil error), and `DeleteMessageBatchCalls()` returning a copy
    - Add `var _ consumer.BatchDeleteClient = (*SQSClient)(nil)` and doc comments; update the type doc to mention the new method
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 12.7_

- [x] 2. Delete_Batcher
  - [x] 2.1 Implement `deleteBatcher`
    - Create `consumer/delete_batcher.go` with constants `maxDeleteBatch = 10` and `deleteRequestTimeout = 5 * time.Second`, the `deleteBatcher` struct (`client BatchDeleteClient`, `log *slog.Logger`, `handles chan string`, `queueURL string`, `wg sync.WaitGroup`), and `newDeleteBatcher(client, queueURL, capacity, log)` replacing a nil logger with `logger.NewNoOp()`
    - `start(ctx, senders)`: `wg.Add(1)` before each `go b.run(ctx)`; `ctx` is passed as a parameter, never stored
    - `run(ctx)`: block on `range b.handles`, perform the non-blocking Opportunistic_Drain up to `maxDeleteBatch`, then call `send`; no timer and no `ctx.Done()` case
    - `enqueue(handle)`: blocking send on `handles`; `close()`: `close(b.handles)` then `b.wg.Wait()`
    - `send(ctx, batch)`: build entries with `Id: strconv.Itoa(i)` and `ReceiptHandle`; call `DeleteMessageBatch` under `context.WithTimeout(ctx, deleteRequestTimeout)` with `defer cancel()`; on request error log one Error per entry; for each `out.Failed` entry map `Id` back to the batch index and log `"failed to delete message"` with `queue_url`, `receipt_handle`, `code`, `sender_fault`, `error`; log unknown `Id`s with the raw `id` and no receipt handle; read pointer fields via `aws.ToString`; treat a nil output as no failures
    - Keep every function within the `funlen`/`gocyclo` limits (split `send` into request and per-entry reporting helpers if needed)
    - _Requirements: 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 4.9, 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 6.8, 7.3, 7.4_

  - [x]* 2.2 Write property test for exactly-once sending
    - **Property 1: Every committed handle is sent exactly once**
    - **Validates: Requirements 4.1, 4.7, 7.2**
    - In `consumer/delete_batcher_test.go`: generate 0–200 unique handles, 1–8 producer goroutines, 1–8 Senders; a recording `DeleteMessageBatchFunc` on `fake.SQSClient` with small random latency; after `close` compare the multiset of sent `ReceiptHandle`s to the enqueued set; tag: `// Feature: opportunistic-delete-batch, Property 1: ...`

  - [x]* 2.3 Write property test for request shape
    - **Property 2: Every request is well-formed**
    - **Validates: Requirements 4.5, 4.6, 4.8**
    - Same generators plus generated queue URLs and boundary counts 1, 9, 10, 11, 20; assert 1–10 entries, distinct `Id`s matching `^[A-Za-z0-9_-]{1,80}$`, `QueueUrl` equal to the configured URL, every `ReceiptHandle` among the enqueued handles; tag: `// Feature: opportunistic-delete-batch, Property 2: ...`

  - [x]* 2.4 Write property test for failure reporting
    - **Property 5: Failures are logged once per entry and never panic**
    - **Validates: Requirements 6.1, 6.2, 6.3, 6.4, 6.5, 6.7, 6.8**
    - Call `send` directly with generated responses: request error; nil output; `Failed` entries for any subset of sent `Id`s plus unknown `Id`s, with nil or non-nil `Id`/`Code`/`Message` and random `SenderFault`; capture logs with `fake.LogHandler`; assert record count, message text, and attributes; tag: `// Feature: opportunistic-delete-batch, Property 5: ...`

  - [x]* 2.5 Write example tests for sender behavior
    - No timer: with one Sender and one enqueued handle, the request is observed before any second handle is enqueued (4.4, 4.9)
    - Sender count: a blocked `DeleteMessageBatchFunc` observes exactly `senders` concurrent calls (5.1)
    - Backpressure: with all Senders blocked, `capacity` enqueues return and the next one blocks until a Sender is released (5.2, 5.3)
    - Request timeout: `DeleteMessageBatchFunc` sees a deadline at most `deleteRequestTimeout` away and a non-canceled context (7.3, 7.4)
    - Failed delete leaves the message: no retry call is made after a failed entry (6.6)
    - _Requirements: 4.4, 4.9, 5.1, 5.2, 5.3, 6.6, 7.3, 7.4_

- [x] 3. Wiring into the dispatcher and `Consumer.Run`
  - [x] 3.1 Route Commits through the batcher in `dispatcher`
    - In `consumer/dispatch.go` add `batcher *deleteBatcher` to `dispatcher`
    - In `deleteMessage`, when `d.batcher != nil` call `d.batcher.enqueue(msg.Identifier())` and return; leave the existing synchronous body unchanged
    - In `stop`, after closing worker channels and `d.wg.Wait()`, call `d.batcher.close()` when non-nil
    - Update the doc comments of `deleteMessage`, `stop`, and the `dispatcher` type to describe the batched path
    - _Requirements: 2.2, 2.3, 4.1, 4.2, 7.1, 7.2, 7.5, 8.1, 8.2, 8.3, 8.4, 9.1, 9.2_

  - [x] 3.2 Add the capability check and batcher construction to `Consumer.Run`
    - In `consumer/consumer.go`, after the scheduler-client check and before `resolveQueueURL`, when `c.route.DeleteBatch()` type-assert `c.client.(BatchDeleteClient)` and return `errors.ErrDeleteBatchUnsupported` when it fails
    - After `newDispatcher` and before `d.start(ctx)`, when batching is enabled set `d.batcher = newDeleteBatcher(batchClient, queueURL, d.workerPoolSize*d.bufferSize, c.log)` and call `d.batcher.start(context.WithoutCancel(ctx), d.workerPoolSize)`
    - Update the `Run` doc comment with the new failure mode and the shutdown flush
    - Keep `Run` within `funlen`/`gocyclo` (extract a helper such as `batchDeleteClient()` if needed)
    - _Requirements: 2.4, 3.4, 3.5, 3.6, 5.1, 5.4, 7.3, 12.7_

  - [x]* 3.3 Write property test for Standard_Route compatibility
    - **Property 3: Standard routes keep the synchronous path**
    - **Validates: Requirements 2.2, 2.3, 2.4**
    - In `consumer/consumer_test.go` (or a new `consumer/delete_batch_test.go`): generate messages and handler outcomes on a route without the option, using a client type that implements only `consumer.SQSClient`; assert zero batch calls and one `DeleteMessage` per committed message; tag: `// Feature: opportunistic-delete-batch, Property 3: ...`

  - [x]* 3.4 Write property test for committed-set equivalence
    - **Property 4: The committed set is identical on both paths**
    - **Validates: Requirements 4.1, 4.2, 8.1, 8.2, 8.3, 8.4**
    - Run the same generated batch (outcomes success/error/backoff keyed by body; Visibility and Scheduled models with fake scheduler; Parallel and PerGroupID modes with repeated group IDs) on a batch route and a standard route; compare removed handle sets; assert zero `DeleteMessage` calls on the batch route; tag: `// Feature: opportunistic-delete-batch, Property 4: ...`

  - [x]* 3.5 Write property test for shutdown flush and leak freedom
    - **Property 6: Shutdown flushes everything and leaks nothing**
    - **Validates: Requirements 7.1, 7.2, 7.3, 7.5**
    - Cancel the run context while generated handlers are still sleeping; record `ctx.Err()` inside `DeleteMessageBatchFunc`; after `Run` returns assert every committed handle was sent with a non-canceled context; verify with `goleak.VerifyNone`; tag: `// Feature: opportunistic-delete-batch, Property 6: ...`

  - [x]* 3.6 Write property test for fail-fast on unsupported clients
    - **Property 7: Unsupported clients fail fast**
    - **Validates: Requirements 3.4, 3.5, 3.6**
    - Generate route options (retry model with fake scheduler, run mode) plus `WithDeleteBatch()` and a client implementing only `consumer.SQSClient`; assert `errors.Is(err, errors.ErrDeleteBatchUnsupported)`, zero `GetQueueUrl` and `ReceiveMessage` calls, and `goleak.VerifyNone`; tag: `// Feature: opportunistic-delete-batch, Property 7: ...`

  - [x]* 3.7 Write property test for metrics parity
    - **Property 8: Metrics are emitted under the same conditions**
    - **Validates: Requirements 9.1, 9.2**
    - Reuse the Property 4 harness with a counting `MetricsRecorder` fake and a route with `WithDLQ`; compare per-outcome counts between batch and standard runs; tag: `// Feature: opportunistic-delete-batch, Property 8: ...`

  - [x]* 3.8 Write LocalStack integration test
    - In `consumer/consumer_integration_test.go` (`-tags=integration`) add `TestIntegrationDeleteBatchRemovesMessages`: send 25 messages to a standard queue and to a FIFO queue, run a route with `WithDeleteBatch()` and the real `*sqs.Client`, and assert both queues drain (`ApproximateNumberOfMessages` and `ApproximateNumberOfMessagesNotVisible` reach 0)
    - _Requirements: 4.1, 4.4, 4.8_

- [x] 4. Checkpoint
  - Run `make check` (lint, race tests with ≥95% coverage, govulncheck) and `make test-chaos`; confirm the existing suites pass unchanged (2.1, 2.5). Ensure all tests pass, ask the user if questions arise.

- [x] 5. Benchmarks (`benchmarks/` module)
  - [x] 5.1 Extend the in-memory benchmark client
    - In `benchmarks/fakeclient.go` add `deleteLatency time.Duration` and atomic counters `deleteCalls`, `batchCalls`, `batchEntries` to `benchClient`
    - Make `DeleteMessage` sleep `deleteLatency` when non-zero and increment `deleteCalls`
    - Add `DeleteMessageBatch`: sleep `deleteLatency` when non-zero, increment `deleteCalls`, `batchCalls`, and `batchEntries`, add the entry count to `delivered` and close `done` when it reaches `total`, and report every entry as successful
    - Update the package and type doc comments to describe the latency knob and counters
    - _Requirements: 11.6_

  - [x] 5.2 Add the delete benchmarks
    - In `benchmarks/bench_test.go` add the constant `simulatedDeleteRTT = 2 * time.Millisecond` and the helper `benchAWSXDelete(b, groups, latency, batch)` following the flow of `benchAWSX`, adding `awsxrouter.WithDeleteBatch()` when `batch` is true
    - Report `msg/s`, `delete-calls/msg` (`deleteCalls / b.N`), and for batch runs `entries/batch` (`batchEntries / batchCalls`) via `b.ReportMetric`
    - Add `BenchmarkDeleteStandardSync`, `BenchmarkDeleteStandardBatch`, `BenchmarkDeleteFIFOSync`, `BenchmarkDeleteFIFOBatch`, each with `b.Run` sub-benchmarks `latency=0` and `latency=2ms`
    - Leave `benchAWSX`, `benchJC`, and the four existing benchmark functions unchanged
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5, 11.7_

  - [x] 5.3 Run the benchmarks and capture results
    - Run `cd benchmarks && go test -run '^$' -bench 'Delete' -benchtime=2s -count=6` and summarize medians (time/op, msg/s, delete-calls/msg, entries/batch) with machine and Go version for the README
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5_

- [x] 6. Documentation
  - [x] 6.1 Update the README configuration, IAM, and add a Batched deletes section
    - In `README.md` add `WithDeleteBatch()` to the `router` bullet of the Configuration Reference
    - Add a "Batched deletes" section (and a Table of Contents entry) covering: how opportunistic batching forms batches with no added delay; when it pays off (high volume, FIFO quota near 300 calls/s) with the cost estimate; FIFO latency behavior; the `consumer.BatchDeleteClient` requirement and `errors.ErrDeleteBatchUnsupported`; Sender count and backpressure; graceful-shutdown flush and the at-least-once tradeoff
    - In the SQS IAM table note that `DeleteMessageBatch` is authorized by `sqs:DeleteMessage` and needs no extra permission
    - _Requirements: 12.1, 12.2, 12.3_

  - [x] 6.2 Add batched-delete results to the README Benchmarks section
    - Add a table built from task 5.3 (sync vs batch, standard and FIFO, latency 0 and 2ms) with the `delete-calls/msg` and `entries/batch` metrics, the machine/Go version note, and the reproduction command
    - _Requirements: 12.4_

  - [x] 6.3 Update package documentation
    - In `consumer/doc.go` add a `# Batched deletes` section describing the Delete_Batcher, the optional `BatchDeleteClient` interface, the fail-fast check, and the shutdown flush
    - In `router/doc.go` add a short section mentioning `WithDeleteBatch` and pointing to the consumer docs
    - _Requirements: 12.5, 12.6_

  - [x] 6.4 Mention the option in the examples
    - In `examples/README.md` add a note on enabling `router.WithDeleteBatch()` for high-volume routes, and add a commented-out `router.WithDeleteBatch()` line next to the route options in `examples/basic` so the option is discoverable
    - _Requirements: 12.2_

- [ ] 7. Final checkpoint
  - Run `make check`, `make test-integration`, and `make test-chaos`. Ensure all tests pass, ask the user if questions arise. Commit as `feat(consumer): add opportunistic batched deletes` (minor release, no breaking change).

## Notes

- Tasks marked with `*` are optional test sub-tasks and can be skipped for a faster MVP;
  core implementation sub-tasks are never optional. The 95% coverage gate in `make check`
  still applies, so most of them are needed in practice.
- Each task references specific requirement sub-clauses for traceability, and each
  property-based test task references its numbered design property.
- Property tests use `pgregory.net/rapid`, run 100+ iterations, and carry the
  `// Feature: opportunistic-delete-batch, Property {n}: {property text}` tag. Properties 1, 2,
  and 5 live in the internal `consumer/delete_batcher_test.go`; the others use `consumer.New`
  and `Run` with `fake.SQSClient`.
- Requirements 2.1 and 2.5 (no change to `SQSClient` or existing exported signatures) are
  verified at the checkpoints by the existing compile-time assertions and test suites passing
  unchanged.
- `CHANGELOG.md` is not edited; release-please generates it from the `feat` commit.
- Checkpoints (tasks 4 and 7) provide incremental validation points and are not part of the
  dependency graph.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2", "1.3", "5.1"] },
    { "id": 1, "tasks": ["1.4", "2.1"] },
    { "id": 2, "tasks": ["2.2", "2.3", "2.4", "2.5", "3.1"] },
    { "id": 3, "tasks": ["3.2"] },
    { "id": 4, "tasks": ["3.3", "3.4", "3.5", "3.6", "3.7", "3.8", "5.2", "6.1", "6.3", "6.4"] },
    { "id": 5, "tasks": ["5.3"] },
    { "id": 6, "tasks": ["6.2"] }
  ]
}
```

# Implementation Plan: Consumer Flow Control

## Overview

This plan replaces the Consumer's receive-ahead buffering with credit-based admission,
concurrent Pollers, non-blocking dispatch per run mode, and a single batched
Visibility_Manager. The plan is ordered bottom-up so every task compiles and is exercised
before anything depends on it:

1. **Foundation**: route options, the optional `BatchVisibilityClient` interface, and the
   fake.
2. **Standalone components**: `creditPool`, `workSource` with `sharedQueue` and
   `groupScheduler`, and `visibilitySender`. Each is unit- and property-tested in
   isolation through the `export_test.go` bridge.
3. **Visibility swap**: the new `visibilityManager` and the `message` changes replace the
   per-message visibility goroutine. The old per-worker dispatch stays in place, so the
   tree still compiles and the existing suite still passes.
4. **Flow-control wiring**: the dispatcher switches to Credits, the `workSource`, and
   Pollers, and `barrier.go` and the hash assignment are removed. Existing tests are
   migrated, and end-to-end properties are added.
5. **Benchmarks, then documentation.**

No orphaned code is introduced: every standalone component is consumed by task 3.1 or 5.1,
and both are reached through `Consumer.Run`.

Tests follow `.claude/rules/go-tests.md`: an external `consumer_test` package plus the
`export_test.go` bridge, table-driven tests plus `pgregory.net/rapid`, `testing/synctest`
for time, `goleak`, no comments except the property tag, and 95% minimum coverage.
Delegate test writing to the `go-test-engineer` agent and review to `go-test-reviewer`.

## Tasks

- [x] 1. Foundation: route options, client interface, fake
  - [x] 1.1 Add `router.WithPrefetch` and `router.WithPollers`
    - In `router/router.go`, add the `prefetch int` and `pollers int` fields to `Route`
      (`make format` places them), the constants `unsetPrefetch = -1` and
      `defaultPollers = 1`, and the getters `Prefetch() int` and `Pollers() int`.
    - In `New`, seed `prefetch: unsetPrefetch` and `pollers: defaultPollers`. After the
      options are applied, resolve `unsetPrefetch` to `int(r.maxMessages)`.
    - In `router/options.go`, add `WithPrefetch(n int)` (error when `n < 0`) and
      `WithPollers(n int)` (error when `n < 1`), using the existing `fmt.Errorf` style.
    - Add doc comments with the design wording.
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 15.8_

  - [x]* 1.2 Write property test for route option validation and defaults
    - **Property 1: Route option validation and defaults**
    - **Validates: Requirements 1.1–1.9**
    - In `router/router_test.go`: `rapid.Int()` plus boundary values `{-1, 0, 1}` for both
      options; generated `MaxMessages` 1–10 without `WithPrefetch`. Table cases combine
      both options with `WithRunMode(PerGroupID)`, `WithScheduledRetry(...)`, and
      `WithDeleteBatch()` without error (Req 1.10).
    - Tag: `// Feature: consumer-flow-control, Property 1: ...`

  - [x] 1.3 Add the optional `consumer.BatchVisibilityClient` interface
    - In `consumer/client.go`, add `BatchVisibilityClient` with the
      `ChangeMessageVisibilityBatch` signature of `*sqs.Client`, with doc comments.
      `SQSClient` stays unchanged.
    - Add `_ BatchVisibilityClient = (*sqs.Client)(nil)` to the compile-time assertion
      block.
    - _Requirements: 2.1, 9.1, 9.2, 15.8_

  - [x] 1.4 Extend `fake.SQSClient` with `ChangeMessageVisibilityBatch`
    - In `fake/sqs_client.go`, add the `ChangeMessageVisibilityBatchFunc` field, a call slice
      guarded by `mu`, the method (records the input; delegates when the func is set;
      otherwise returns every entry `Id` in `Successful`), and
      `ChangeMessageVisibilityBatchCalls()` returning a copy.
    - Add `var _ consumer.BatchVisibilityClient = (*SQSClient)(nil)`.
    - Extend `fake/sqs_client_test.go` with the default, override, copy, and concurrent-use
      cases.
    - _Requirements: 13.1, 13.2, 13.3, 13.4, 13.5_

- [x] 2. Standalone components
  - [x] 2.1 Implement `creditPool`
    - Create `consumer/credits.go` with `creditPool{tokens, turn chan struct{}}`:
      - `newCreditPool(capacity int)` pre-fills `tokens`;
      - `acquire(ctx, n)` takes `turn`, then takes `n` tokens with a `ctx.Done()` select,
        returning the partial tokens and `ctx.Err()` on cancellation;
      - `release(n)`;
      - `available()`.
    - Expose `NewCreditPool`, `Acquire`, `Release`, and `Available` in
      `consumer/export_test.go`, with doc comments.
    - _Requirements: 3.6, 3.7, 3.9, 3.10_

  - [x]* 2.2 Write property test for credit conservation
    - **Property 2: Credit conservation**
    - **Validates: Requirements 3.6, 3.7, 3.9, 3.10**
    - In `consumer/credits_test.go`: a `rapid.StateMachine` with capacity 1–64 and the
      actions acquire(n ≤ available), release(held), and acquire with a pre-canceled
      context. Assert `Available()` after each step, and add a goleak check. Add a
      `synctest` case where two acquirers contend for a full capacity without deadlocking.
    - Tag: `// Feature: consumer-flow-control, Property 2: ...`

  - [x] 2.3 Add the `workSource` interface and `sharedQueue`
    - Create `consumer/worksource.go` with the `workSource` interface (`submit`, `take`,
      `complete`, `drain`) and `sharedQueue{ch chan *message}`, with capacity passed by
      the caller.
    - `take` selects on `ctx.Done()` and `ch`; `complete` returns nil; `drain` reads
      without blocking.
    - Expose a `NewSharedQueue` bridge.
    - _Requirements: 5.1, 5.2, 5.4_

  - [x] 2.4 Implement `groupScheduler`
    - Create `consumer/group_scheduler.go` with `groupState` and `groupScheduler` as
      designed (`groups` map, `ready chan *groupState`, `customFields`, `mu`).
    - Move the Group_Key derivation out of `dispatcher.groupKey` into the package function
      `groupKey(msg *message, fields []string) string`, and have the existing dispatcher
      call it, so behavior is unchanged.
    - Implement `submit`, `take`, `complete(msg, failed)` (returns the held messages,
      re-queues a group with pending work, deletes empty groups), and `drain` (key order).
    - Expose `NewGroupScheduler` and `NewTestMessage(groupID string, attrs map[string]string,
      receivedAt time.Time)` bridges.
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.6, 6.7, 7.1, 7.2_

  - [x]* 2.5 Write property test for group exclusivity and order
    - **Property 6: Group exclusivity and order**
    - **Validates: Requirements 6.1, 6.2**
    - In `consumer/group_scheduler_test.go`: 1–8 keys, 1–50 messages, and 1–8 worker
      goroutines calling `Take`/`Complete`. Record start and finish events under a mutex,
      then assert at most one active message per key and the per-key start order.
    - Tag: `// Feature: consumer-flow-control, Property 6: ...`

  - [x]* 2.6 Write property test for group key derivation
    - **Property 8: Group key derivation**
    - **Validates: Requirements 6.6**
    - In `consumer/group_scheduler_test.go`: generate the `MessageGroupId`, field names, and
      the placement of each value (native, envelope, both, neither). Assert the key equals
      the expected separator-joined string.
    - Tag: `// Feature: consumer-flow-control, Property 8: ...`

  - [x]* 2.7 Write property test for discarding group state
    - **Property 10: Group state is discarded**
    - **Validates: Requirements 6.7**
    - In `consumer/group_scheduler_test.go`: generated scripts with failures. After every
      message is completed or held, assert that a test-bridge `GroupCount()` returns 0 and
      that `Drain()` is empty.
    - Tag: `// Feature: consumer-flow-control, Property 10: ...`

  - [x] 2.8 Implement the `visibilitySender` implementations
    - Create `consumer/visibility_sender.go` with the `visibilityChange` type, the
      `visibilitySender` interface, `batchVisibilitySender` (ids via `strconv.Itoa`,
      results mapped with the existing `batchIndex`, nil output treated as success, request
      error marks every entry failed, failed entries carry `code` and `sender_fault`), and
      `singleVisibilitySender`.
    - Every request is bounded by `visibilityRequestTimeout`.
    - Chunking to `maxVisibilityBatch = 10` is done by the caller.
    - _Requirements: 2.7, 9.3, 9.5, 9.6, 9.7, 9.8, 9.11, 9.12_

  - [x]* 2.9 Write property test for batch shape
    - **Property 15: Batch shape**
    - **Validates: Requirements 9.3, 9.5, 9.6**
    - In `consumer/visibility_sender_test.go`: generate 1–10 changes. Assert that ids are
      unique and match `^[A-Za-z0-9_-]{1,80}$`, that the queue URL is set, and that each
      change appears exactly once. Also check the single sender's one-call-per-change
      behavior.
    - Tag: `// Feature: consumer-flow-control, Property 15: ...`

- [x] 3. Visibility manager swap
  - [x] 3.1 Rewrite `visibilityManager` and switch `message` to atomic backoff
    - In `consumer/message.go`:
      - change `newMessage(m types.Message, receivedAt time.Time)`;
      - add `receivedAt` and `backoffNanos atomic.Int64`;
      - make `Backoff` store the delay and flag without blocking;
      - add `backoffDelay()`;
      - remove `backoffChannel` and `backoffSignal`;
      - update the `Dispatch` doc comment.
    - Rewrite `consumer/visibility.go`:
      - constants `renewalMargin`, `coalesceWindow`, `maxVisibilitySeconds`,
        `visibilityRequestTimeout`, `releaseTimeout`;
      - `trackedEntry` and the `container/heap` `entryHeap`;
      - `newVisibilityManager(client, queueURL, route, log)`, which picks the batch sender
        when `client.(BatchVisibilityClient)` succeeds;
      - `start`, `close`, `track`, `untrack`, `backoff`, `release`;
      - the run loop with coalescing, the early-extension gain check, `nextTimeout` with
        the 12-hour budget, chunking to 10, applying results under the lock, and skipping
        logs for untracked entries.
      - Split the code into helpers to stay within `funlen` and `gocyclo`.
    - Minimal switch in the current dispatcher:
      - `poll` stamps `time.Now()` after `ReceiveMessage` and calls `vm.track`;
      - `process` calls `vm.backoff`/`vm.untrack` instead of starting a goroutine and
        calling `msg.Dispatch()`;
      - `Consumer.Run` calls `vm.start(context.WithoutCancel(ctx))` and `vm.close()` after
        `d.stop()`.
      - Delete `newScheduledVisibilityManager`: the Scheduled_Retry_Model gets no backoff
        change through `process`.
    - Update the `router.WithExtensionLimit` doc to say a limit of `n` allows exactly `n`
      Extensions.
    - Replace `visibility_test.go` with tests of the new manager through the bridge
      (`NewVisibilityManager`, `Track`, `Untrack`, `Backoff`, `Release`, `Start`, `Close`,
      and the constants).
    - _Requirements: 2.3, 2.7, 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8, 8.9, 8.10, 8.11, 9.4,
      9.9, 9.10, 10.1, 10.2, 10.3, 10.4, 10.5, 11.5, 11.6, 11.9, 15.7_

  - [x]* 3.2 Write property test for extension timing from Receive_Time
    - **Property 11: Extension timing from Receive_Time**
    - **Validates: Requirements 8.1, 8.2, 8.3, 8.7, 8.10**
    - In `consumer/visibility_test.go`, under `synctest`: VT 11–600, limit 1–5, and a
      generated delay between `Track` and the simulated handler start. A recording fake
      batch client asserts the send instants against `receivedAt + VT − margin` and
      `deadline − margin − coalesceWindow`.
    - Tag: `// Feature: consumer-flow-control, Property 11: ...`

  - [x]* 3.3 Write property test for the exact extension limit
    - **Property 12: Extension limit is exact**
    - **Validates: Requirements 8.4, 8.5**
    - Under `synctest`: limit 0–5 and VT 11–600 with a message never untracked until time
      advances past the last possible due. Assert the count and the k-th timeout.
    - Tag: `// Feature: consumer-flow-control, Property 12: ...`

  - [x]* 3.4 Write property test for the 12-hour ceiling
    - **Property 13: 12-hour ceiling**
    - **Validates: Requirements 8.6**
    - Under `synctest`: VT up to 43200 and limit up to 10. Assert
      `sendInstant + timeout ≤ receivedAt + 43200s` for every request.
    - Tag: `// Feature: consumer-flow-control, Property 13: ...`

  - [x]* 3.5 Write property test for no Extension after Outcome
    - **Property 14: No Extension after Outcome**
    - **Validates: Requirements 8.8, 8.9**
    - Under `synctest`: generated `Untrack` instants per message. Assert that no request
      built after `Untrack` returned contains the handle, except its own backoff or
      Release.
    - Tag: `// Feature: consumer-flow-control, Property 14: ...`

  - [x]* 3.6 Write property test for coalescing
    - **Property 16: Coalescing**
    - **Validates: Requirements 9.4**
    - Under `synctest`: 1–40 messages tracked with receive offsets inside one
      `coalesceWindow`. Assert that the first Extension round uses `ceil(count/10)` requests.
    - Tag: `// Feature: consumer-flow-control, Property 16: ...`

  - [x]* 3.7 Write property test for batch failure reporting
    - **Property 17: Batch failure reporting**
    - **Validates: Requirements 9.7–9.11**
    - Generated `ChangeMessageVisibilityBatchFunc` results (error, nil output, random
      `Failed` with valid and invalid ids). Capture logs with `fake/loghandler.go`. Assert
      no panic, one Error per failed tracked or immediate entry with the required
      attributes, and an unchanged deadline (verified by the next send instant).
    - Tag: `// Feature: consumer-flow-control, Property 17: ...`

  - [x]* 3.8 Write property test for backoff semantics
    - **Property 18: Backoff semantics**
    - **Validates: Requirements 10.1–10.5**
    - Through `consumer.New`/`Run` with `fake.SQSClient`: generated delays (negative, zero,
      above 12 h) and 1–20 `Backoff` calls per handler, under both retry models. Assert one
      change with the clamped last delay and no delete (Visibility model), and no change
      (Scheduled model). Add an example test with 1,000 `Backoff` calls that returns.
    - Tag: `// Feature: consumer-flow-control, Property 18: ...`

- [x] 4. Checkpoint
  - Run `make check`. The existing suite must pass with the new Visibility_Manager. Ensure
    all tests pass, ask the user if questions arise.

- [x] 5. Flow-control wiring
  - [x] 5.1 Switch the dispatcher to Credits, `workSource`, and Pollers
    - In `consumer/dispatch.go`:
      - add the `credits`, `src`, `route`, and `receiveBatch` fields and the `workers`
        WaitGroup;
      - build `sharedQueue` or `groupScheduler` with capacity = `WorkerPoolSize + Prefetch`;
      - add `startWorkers`, `stopWorkers`, the `worker` take-loop (release when taken after
        cancellation), `process` returning `failed`, `finish`, `hold` (Warn log with
        `queue_url`, `receipt_handle`, `group_id`), and `releasePending`;
      - remove `channels`, `start`, `dispatch`, `stop`, `workerIndex`, `groupIndex`,
        `orderedGroups`, `failGroup`, `holdForOrder`, and `newBatchBarrier`.
    - Delete `consumer/barrier.go`.
    - Create `consumer/poller.go` with `runPollers` and `poll`:
      - acquire `receiveBatch`, then receive with `MaxNumberOfMessages = receiveBatch`;
      - stamp the Receive_Time;
      - on error, release the Credits and call `retryAfterError` (moved from
        `consumer.go`);
      - release unused Credits;
      - Warn about and skip excess messages;
      - track and submit each message.
    - In `consumer/consumer.go`, rewrite `Run` to the design sequence: fail-fast checks
      unchanged, then vm start, dispatcher build, optional batcher, `startWorkers`,
      `runPollers`, `stopWorkers`, `releasePending`, `batcher.close`, `vm.close`. Keep the
      delete batcher capacity at `WorkerPoolSize × MaxMessages`.
    - Update the `Consumer.Run` doc comment.
    - _Requirements: 2.4, 2.5, 2.6, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 4.1, 4.2, 4.3,
      4.4, 4.5, 5.3, 6.5, 7.3, 7.4, 7.5, 7.6, 7.7, 11.1, 11.2, 11.3, 11.4, 11.7, 11.8, 12.1,
      12.2, 12.3, 12.4, 12.5, 12.6_

  - [x] 5.2 Migrate existing tests off removed internals
    - Delete `consumer/barrier_test.go`.
    - Rework `dispatch_test.go`, `ordering_test.go`, `consumer_test.go`, and any other test
      that uses worker indices, per-worker channels, or the barrier, so they go through
      `consumer.New`/`Run` or the new bridges.
    - Keep every existing behavioral assertion (ordering, DLQ observe-only, scheduled
      outcomes, delete batch, metrics, preservation).
    - _Requirements: 12.1, 12.2, 12.3, 12.4_

  - [x]* 5.3 Write property test for admission bound and request size
    - **Property 3: Admission bound and request size**
    - **Validates: Requirements 3.1–3.5, 4.2**
    - In `consumer/flow_control_test.go`: generated `WorkerPoolSize` 1–32, `Prefetch` 0–20,
      `MaxMessages` 1–10, and `Pollers` 1–4. A scripted `ReceiveMessageFunc` with sizes and
      errors; handlers blocked on per-message channels released in generated order. Assert
      an atomic admitted count ≤ capacity and `MaxNumberOfMessages` on every call.
    - Tag: `// Feature: consumer-flow-control, Property 3: ...`

  - [x]* 5.4 Write property test for exactly one Outcome per admitted message
    - **Property 4: Every admitted message reaches exactly one Outcome**
    - **Validates: Requirements 3.8, 12.1, 12.2, 12.4**
    - Generated outcome scripts under both retry models and both run modes. Count deletes,
      schedules, DLQ publishes, and visibility changes per receipt handle, and assert full
      Credits after `Run` through a bridge accessor.
    - Tag: `// Feature: consumer-flow-control, Property 4: ...`

  - [x]* 5.5 Write property test for Parallel progress despite a stuck worker
    - **Property 5: Parallel progress despite a stuck worker**
    - **Validates: Requirements 5.1–5.4**
    - Under `synctest`: `WorkerPoolSize` 2–32 and one handler blocked until cancellation.
      Assert every other message is deleted.
    - Tag: `// Feature: consumer-flow-control, Property 5: ...`

  - [x]* 5.6 Write property test for group work conservation
    - **Property 7: Group work conservation**
    - **Validates: Requirements 6.3, 6.4, 6.5**
    - Under `synctest`: `w` 1–16 Workers and `g ≥ w` groups with blocking handlers. Assert
      `w` concurrent handler starts after `synctest.Wait`.
    - Tag: `// Feature: consumer-flow-control, Property 7: ...`

  - [x]* 5.7 Write property test for hold on failure
    - **Property 9: Hold on failure**
    - **Validates: Requirements 7.1–7.7**
    - PerGroupID through `Run` with a generated failure position (error or backoff) per
      group under both retry models. Assert held messages never run, are never deleted,
      get no visibility change, and produce one Warn each. Under the Scheduled model,
      assert no holds.
    - Tag: `// Feature: consumer-flow-control, Property 9: ...`

  - [x]* 5.8 Write property test for shutdown releasing pending messages
    - **Property 19: Shutdown releases pending messages**
    - **Validates: Requirements 11.1–11.4, 11.8**
    - Under `synctest`: cancellation at a generated step. Assert that active handlers
      finish before `Run` returns, that each unheld pending message gets exactly one
      timeout-0 change and no handler run, and add a goleak check.
    - Tag: `// Feature: consumer-flow-control, Property 19: ...`

  - [x]* 5.9 Write property test for the fallback without a batch client
    - **Property 20: Fallback without batch client**
    - **Validates: Requirements 2.7**
    - Reuse the 5.4 generator with a wrapper type that embeds only `consumer.SQSClient`.
      Assert no batch calls and the same per-handle timeouts through
      `ChangeMessageVisibility`.
    - Tag: `// Feature: consumer-flow-control, Property 20: ...`

  - [x] 5.10 Example tests for combinations and edge cases
    - In `consumer/flow_control_test.go`:
      - a Delete_Batch_Route with `Pollers=2` in PerGroupID (deletes batched, shutdown
        flush intact);
      - receive-error isolation with two Pollers under `synctest`;
      - a response with more messages than requested (Warn, not admitted);
      - `Consumer.Run` fail-fast paths unchanged.
    - _Requirements: 1.10, 4.3, 4.4, 11.7, 12.5_

  - [x] 5.11 Integration test on MiniStack
    - In `consumer/consumer_integration_test.go` (`-tags=integration`): a FIFO queue in
      PerGroupID mode with `Pollers=2`:
      - a failing head message makes its group tail held and redelivered in order;
      - canceling the Consumer releases pending messages, and a second Consumer receives
        them before the VT elapses.
    - _Requirements: 4.1, 7.1, 11.2_

- [x] 6. Checkpoint
  - Run `make check`, `make test-chaos`, and `make test-integration`. Ensure all tests pass,
    ask the user if questions arise.

- [x] 7. Benchmarks
  - [x] 7.1 Extend the in-memory benchmark client
    - In `benchmarks/fakeclient.go`:
      - implement `ChangeMessageVisibilityBatch`;
      - count visibility requests and `ReceiveMessage` requests;
      - stamp each message at receive so the handler can compute `start − receivedAt`.
    - Keep `DeleteMessage`/`DeleteMessageBatch` accounting unchanged.
    - _Requirements: 14.6_

  - [x] 7.2 Add flow-control benchmarks
    - In `benchmarks/bench_test.go`:
      - add `BenchmarkFlowControl` with `parallel` and `pergroup` scenarios at
        `workers=30/max=10` and a non-zero simulated handler latency, plus a `pollers=2`
        variant;
      - report `visibility_reqs/msg`, `receive_reqs/msg`, and `queue_wait_ms` with
        `b.ReportMetric`.
    - Leave the existing benchmarks unchanged.
    - _Requirements: 14.1, 14.2, 14.3, 14.4, 14.5_

- [x] 8. Documentation
  - [x] 8.1 README
    - In the Configuration Reference, document `WithPrefetch` and `WithPollers` (defaults
      and bounds), and the new `WithExtensionLimit` semantics.
    - Add a "Flow control" section: credit admission, dispatch per run mode, deadline from
      Receive_Time, batched visibility, shutdown release, and the sizing rule from the
      design.
    - IAM table: `ChangeMessageVisibilityBatch` is authorized by
      `sqs:ChangeMessageVisibility`.
    - Benchmarks: the flow-control results and the reproduction command.
    - _Requirements: 15.1, 15.2, 15.3, 15.4_

  - [x] 8.2 Package documentation
    - `consumer/doc.go`: a "Flow control" section on Pollers, the Credit_Pool, dispatch per
      run mode, and the Visibility_Manager.
    - `router/mode.go`: update the `Parallel`/`PerGroupID` doc comments.
    - `router/doc.go`: mention `WithPrefetch`/`WithPollers`.
    - _Requirements: 15.5, 15.6, 15.8_

- [x] 9. Final checkpoint
  - Run `make check`, `make test-integration`, `make test-chaos`, and `make test-bench`.
    Ensure all tests pass, ask the user if questions arise.
  - Suggested commits:
    - `feat(router): add WithPrefetch and WithPollers`
    - `feat(consumer): centralize and batch visibility management`, with a body noting the
      `ExtensionLimit` off-by-one fix and the Backoff timing change
    - `feat(consumer): credit-based admission and dynamic group scheduling`
    - `docs: document consumer flow control`

## Notes

- Tasks marked with `*` are optional test sub-tasks; implementation tasks are never
  optional. The 95% coverage gate in `make check` still applies, so most test tasks are
  needed in practice.
- Each task references specific requirement sub-clauses, and each property-test task
  references exactly one design property.
- Property tests use `pgregory.net/rapid`, run 100+ iterations, use `testing/synctest` for
  time, and carry the `// Feature: consumer-flow-control, Property {n}: {property text}` tag.
- Requirements 2.1–2.3 (no change to `SQSClient`, `Message`, or exported signatures) are
  verified at every checkpoint by the compile-time assertions and the unchanged public test
  suites.
- Task 3.1 is intentionally larger than one file: the `message` backoff change and the
  Visibility_Manager rewrite must land together for the tree to compile.
- `CHANGELOG.md` is not edited by hand; release-please generates it from the commits.
- Checkpoints (tasks 4, 6, 9) are not part of the dependency graph.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.3", "2.1", "2.3", "2.4"] },
    { "id": 1, "tasks": ["1.2", "1.4", "2.2", "2.5", "2.6", "2.7", "2.8"] },
    { "id": 2, "tasks": ["2.9", "3.1"] },
    { "id": 3, "tasks": ["3.2", "3.3", "3.4", "3.5", "3.6", "3.7", "3.8"] },
    { "id": 4, "tasks": ["5.1"] },
    { "id": 5, "tasks": ["5.2"] },
    { "id": 6, "tasks": ["5.3", "5.4", "5.5", "5.6", "5.7", "5.8", "5.9", "5.10", "5.11", "7.1"] },
    { "id": 7, "tasks": ["7.2", "8.1", "8.2"] }
  ]
}
```

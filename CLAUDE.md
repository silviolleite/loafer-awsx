# loafer-awsx

Go library (`github.com/silviolleite/loafer-awsx`) for consuming AWS SQS queues and
publishing to SNS topics in high-throughput, distributed systems. Library code: it is
imported by other services, so its public API, error contracts, and concurrency behavior
are the product.

## Package map

| Package | Responsibility |
| --- | --- |
| `broker` | Orchestrates one `Consumer` per route: coordinated startup, graceful shutdown, fail-fast |
| `consumer` | SQS polling loop, worker-pool dispatch, visibility timeout, commit/backoff, FIFO scheduled retry |
| `router` | `Route` = queue + handler + middleware chain + route-level config (pure values) |
| `middleware` | `Handler`/`Middleware`, `Chain`, built-ins (Recovery, Logging, Metrics, OpenTelemetry) |
| `producer` | SNS publishing (standard and FIFO) |
| `typed` | Generic, type-safe handlers decoded through a `Codec` |
| `client` / `conn` | AWS SDK v2 config factory and SQS/SNS/Scheduler client constructors |
| `idgen` | MessageGroupId / MessageDeduplicationId strategies |
| `errors` | Sentinel errors + `Wrap` that preserves `errors.Is` |
| `logger` | `*slog.Logger` constructors |
| `fake` | Test doubles for the core interfaces (excluded from coverage and lint) |
| `examples/`, `benchmarks/` | Runnable examples (Terraform under `examples/terraform`); `benchmarks/` is a separate Go module |

## Commands

```bash
make format            # goimports -local + fieldalignment -fix
make lint              # format + golangci-lint (.golangci.yml)
make test              # race detector + atomic coverage (excludes examples/fake)
make check             # lint + test + govulncheck — run before declaring work done
make test-integration  # LocalStack (docker compose) + -tags=integration
make test-chaos        # GOMAXPROCS=1, -race -count=30 -shuffle=on
make test-bench        # benchmarks
```

Single package / test: `go test -race -run TestName ./consumer/...`

## Engineering principles

Act as a Staff-level Go engineer. Priorities, in order: **correctness, simplicity,
readability, performance, maintainability**.

- Follow Effective Go. Prefer composition. No abstractions without a second concrete use.
- Concurrency: no data races, no deadlocks, no goroutine leaks. Every blocking or
  long-running operation takes and honors a `context.Context`. Every goroutine has an
  owner and a guaranteed exit path.
- Errors: always handle them. Never `panic` in library code. Wrap with context using
  `%w` or the project's `errors.Wrap`, and keep sentinels matchable with `errors.Is`.
- Every exported symbol has a doc comment. All code, comments, and docs are in English.
- Lint rules that commonly bite: max line length 140, `funlen` 100 lines/50 statements,
  `gocyclo` 15, no magic numbers (`mnd`), `logrus` and `pkg/errors` are banned.

## Dependencies and tools

Never introduce a deprecated library, tool, API, or GitHub Action. Before adding or
recommending one, verify it: check pkg.go.dev (the `golang-pkg-go-dev` skill / `godig`
MCP) for deprecation notices, retractions, and known vulnerabilities, and confirm it is
maintained. If verification is not possible, say so explicitly and present it as a
suggestion rather than applying it. A hook flags deprecated direct dependencies whenever
`go.mod` changes.

## Tests

Test conventions live in `.claude/rules/go-tests.md` and load automatically when touching
`*_test.go`. Headlines: external `<pkg>_test` packages, 95% minimum coverage,
table-driven + property-based (`pgregory.net/rapid`), deterministic, no comments except
the spec property tag. Delegate test writing to the `go-test-engineer` agent and test
review to the `go-test-reviewer` agent.

## Spec-driven workflow

Features are designed requirements-first under `.claude/specs/<feature>/`
(`requirements.md` → `design.md` → `tasks.md`). Use the `/spec` skill to create or
update a spec and `/spec-run` to implement its tasks. Read the relevant spec before
changing code in an area it covers.

## Commits

Conventional Commits, enforced by commitlint (lefthook `commit-msg` hook and CI).
`feat`/`fix`/`!` drive release-please versioning, so pick the type carefully. Scope is
the package name, e.g. `fix(consumer): ...`.

## Go development

Before any Go coding, review, debugging, troubleshooting, or setup task, load the `samber/cc-skills-golang@golang-how-to` skill first — it routes to whichever other Go skills the task needs.

## Required Go skills

The following Go skills from `samber/cc-skills-golang` MUST always be applied when working on this project. Load them at the start of every Go-related task, regardless of whether the user explicitly mentions them.

- `samber/cc-skills-golang@golang-concurrency`
- `samber/cc-skills-golang@golang-context`
- `samber/cc-skills-golang@golang-error-handling`
- `samber/cc-skills-golang@golang-safety`
- `samber/cc-skills-golang@golang-testing`
- `samber/cc-skills-golang@golang-documentation`
- `samber/cc-skills-golang@golang-naming`
- `samber/cc-skills-golang@golang-code-style`

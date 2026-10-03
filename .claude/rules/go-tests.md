---
paths:
  - "**/*_test.go"
---

# Go test conventions

- Test files use the external `<package>_test` package. The only exception is an
  `export_test.go` bridge in the internal package that exposes unexported seams to the
  external tests; its exported helpers carry doc comments like any exported symbol.
- Minimum coverage: 95% per package (`make test` reports it; `examples/` and `fake/` are
  excluded).
- No comments in tests. The single allowed comment is the spec property tag on
  property-based tests: `// Feature: <spec-name>, Property <n>: <property text>`.
  Build tags (`//go:build integration`) and `//nolint` directives are not comments.
- Generate data with `pgregory.net/rapid` instead of hand-picked literals wherever the
  input space is wider than a few cases. Property tests run at least 100 iterations.
- Combine table-driven tests (named cases, `t.Run`) with property-based tests. Cover the
  happy path, every error branch, boundaries, and edge cases (nil, empty, zero, max,
  canceled context).
- Deterministic only: no `time.Sleep` for synchronization, no wall-clock or real network
  dependence, no order-dependent tests. Inject clocks, use channels/`sync` primitives to
  synchronize, and use the doubles in `fake/`. Tests must pass under
  `-race -count=30 -shuffle=on` (`make test-chaos`).
- Concurrency code: assert no goroutine leaks with `go.uber.org/goleak`.
- Assertions use `github.com/stretchr/testify` (`require` for preconditions that make the
  rest of the test meaningless, `assert` otherwise). Assert on values and `errors.Is`
  matches, never on error strings.
- Integration tests use the `integration` build tag, the `Integration` name prefix, and
  LocalStack (`make test-integration`).

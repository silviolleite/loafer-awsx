# Contributing to loafer-awsx

Thanks for your interest in contributing! This document describes the commit
conventions, tooling, and release process used by this project.

## Conventional Commits

All commits MUST follow the [Conventional Commits](https://www.conventionalcommits.org/)
specification. The commit message format is:

```
<type>(<optional scope>): <description>

<optional body>

<optional footer>
```

Common commit types:

- `feat`: a new feature (triggers a minor release)
- `fix`: a bug fix (triggers a patch release)
- `docs`: documentation-only changes
- `test`: adding or fixing tests
- `refactor`: code change that neither fixes a bug nor adds a feature
- `perf`: a performance improvement
- `build`: changes to the build system or dependencies
- `ci`: changes to CI configuration
- `chore`: other changes that don't modify src or test files

Examples:

```
feat(consumer): add PerGroupID worker routing
fix(producer): return ErrEmptyInput on nil publish input
docs(readme): add quickstart example
```

Breaking changes are signalled with a `!` after the type/scope or a
`BREAKING CHANGE:` footer, and trigger a major release:

```
feat(router)!: replace DLQ target ARN with observe-only MaxReceiveCount
```

## Commit message validation

Commit messages are validated in two places:

- **Locally** via a `commit-msg` git hook. The hook is wired through
  [lefthook](https://github.com/evilmartians/lefthook) (see `lefthook.yml`) and
  runs `npx --no -- commitlint --edit` against your commit message using the
  configuration in `commitlint.config.js`.
- **On pull requests** via the `commitlint` GitHub Action
  (`.github/workflows/commitlint.yml`), which runs
  `wagoid/commitlint-github-action@v6` over all commits in the PR.

Both use `@commitlint/config-conventional`, so passing locally means passing in CI.

## Local development setup

Install the Go toolchain and developer tooling:

```bash
make configure
```

`make configure` installs the Go tools (goimports, fieldalignment,
golangci-lint, lefthook, mockery, rapid, govulncheck) and then runs
`make setup-dev`.

If you only need the commit-message tooling and git hooks, run:

```bash
make setup-dev
```

`make setup-dev` runs `npm install` to install the commitlint dev dependencies
declared in `package.json` and then `lefthook install` to register the
`commit-msg` hook in your local clone.

## Verification

Before opening a PR, run the standard checks:

```bash
make check        # lint + tests (race + coverage) + govulncheck
```

Additional targets:

- `make check-vuln` runs `govulncheck ./...` to scan for known vulnerabilities.
- `make test-chaos` runs the stress suite
  (`GOMAXPROCS=1 go test ./... -race -count=30 -shuffle=on -timeout 15m`) to
  surface race conditions and flakiness.

## Releases

Releases are automated with
[release-please](https://github.com/googleapis/release-please) via the
`.github/workflows/release-please.yml` workflow.

On every push to `main`, the workflow first runs the CI workflow
(`.github/workflows/ci.yml`), then runs the release-please action. From the
Conventional Commits history, release-please:

1. Opens (or updates) a release pull request that bumps the version and updates
   `CHANGELOG.md` based on the commits since the last release.
2. When that release PR is merged into `main`, it creates a GitHub release and
   tags the commit with a `v`-prefixed tag (for example `v0.2.0`).

The version state lives in `.release-please-manifest.json` and the release
behavior is configured in `.release-please-config.json` (Go release type, root
package, `CHANGELOG.md` path). Because commit types drive the version bump,
writing accurate Conventional Commit messages is what keeps the changelog and
versioning correct.

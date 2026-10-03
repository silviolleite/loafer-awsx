---
name: spec
description: Requirements-first, spec-driven feature design for loafer-awsx. Creates or updates a spec under .claude/specs/<feature>/ in three gated phases — requirements.md (EARS acceptance criteria), design.md (architecture + numbered correctness properties), tasks.md (traceable implementation plan with a dependency graph). Use when the user wants to plan, specify, or design a new feature, a behavior change, or a rewrite before coding, or asks to update an existing spec.
argument-hint: "<feature-name or description>"
---

# Spec workflow (requirements-first)

Specs live in `.claude/specs/<feature-name>/` (kebab-case). Existing specs are the best
reference for depth and tone: `fifo-scheduled-retry` is the canonical example.

The workflow has three phases. **Each phase ends with a stop: show the user the document,
ask for approval or changes, and do not start the next phase until they approve.** Never
write code in this skill — implementation is `/spec-run`.

## Phase 0 — Scope

1. Derive the feature name from `$ARGUMENTS`. If `.claude/specs/<name>/` exists, this is
   an update: read all three files and change only what the request touches, keeping
   requirement, property, and task numbering stable (append; do not renumber).
2. Read `CLAUDE.md`, the packages the feature touches, and any related spec.
3. If the request is ambiguous on something that changes the requirements (scope,
   defaults, backward compatibility), ask before writing.

## Phase 1 — requirements.md

Follow [references/requirements.md](references/requirements.md).

- Introduction states what changes, what is explicitly out of scope, and the tradeoffs
  the feature accepts.
- Glossary defines every domain term as `Capitalized_Snake` and the criteria use only
  those terms.
- Each requirement: a user story plus numbered EARS acceptance criteria. Every criterion
  is testable, has one subject and one `SHALL`, and is numbered `<req>.<n>`.
- Include requirements for backward compatibility, configuration validation, error
  surfacing, and observability whenever they apply.

Stop and ask for approval.

## Phase 2 — design.md

Follow [references/design.md](references/design.md).

- Verify every external behavior the design relies on (AWS service semantics, SDK APIs,
  library versions) against current documentation before committing to it; record the
  findings in "Research summary and key design decisions". Do not design around a
  deprecated API or library — check pkg.go.dev (`golang-pkg-go-dev` skill / `godig`).
- Load the relevant samber Go skills through `golang-how-to` (design patterns,
  concurrency, context, error handling, structs/interfaces) and design accordingly.
- Use real Go signatures, file paths, and package names from this repository.
- Correctness Properties: universally quantified ("*For any* …") statements, each ending
  with `**Validates: Requirements x.y, …**`. Every acceptance criterion that can be a
  property should be covered by one; the rest are covered by example or integration
  tests listed in the Testing Strategy.

Stop and ask for approval.

## Phase 3 — tasks.md

Follow [references/tasks.md](references/tasks.md).

- Bottom-up order so every task compiles and is exercised before something depends on
  it; no orphaned code — each piece is wired in by a later task.
- Each leaf task names concrete files and symbols and ends with
  `_Requirements: x.y, …_`. Property-test tasks also carry the property number and the
  `**Validates:**` line.
- Optional test sub-tasks are marked `- [ ]*`; implementation tasks are never optional.
- Add `Checkpoint` tasks after major milestones, and documentation/example tasks
  (README, package docs, `examples/`) when the public API changes.
- End with Notes and a `Task Dependency Graph` JSON of parallelizable waves (checkpoints
  excluded).

Stop and ask for approval. Then tell the user that `/spec-run <feature-name>` executes
the plan.

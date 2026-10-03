# tasks.md template

```markdown
# Implementation Plan: <Feature Title>

## Overview

<Build order and why (bottom-up), which packages are touched, and how each piece is
consumed by a later task so no orphaned code is introduced.>

## Tasks

- [ ] 1. <Group title>
  - [ ] 1.1 <Implementation task>
    - <Concrete change: file, symbol, signature, behavior>
    - <Doc comments for new exported symbols>
    - _Requirements: 1.1, 1.4_

  - [ ]* 1.2 Write property test for <behavior>
    - **Property 3: <property name from design.md>**
    - **Validates: Requirements 2.5**
    - <Test file, generators, assertions>; tag: `// Feature: <feature-name>, Property 3: ...`

- [ ] 2. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [ ] N. Documentation and runnable examples
  - [ ] N.1 <README section / package doc / examples program / Makefile target>
    - _Requirements: …_

## Notes

- Tasks marked with `*` are optional test sub-tasks and can be skipped for a faster MVP;
  core implementation sub-tasks are never optional.
- Each task references specific requirement sub-clauses for traceability, and each
  property-based test task references its numbered design property.
- Property tests use `pgregory.net/rapid`, run 100+ iterations, live in `_test`
  packages, and carry the `// Feature: <feature-name>, Property {n}: {property text}` tag.
- Checkpoints provide incremental validation points and are not part of the dependency
  graph.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "2.1"] },
    { "id": 1, "tasks": ["1.2", "2.2"] }
  ]
}
```
```

Rules:

- Leaf tasks are small enough to implement and verify in one sitting (one or two files).
- Every requirement criterion is referenced by at least one task; every design property
  by exactly one property-test task.
- A task in wave `k` depends only on tasks in waves `< k`.

# design.md template

```markdown
# Design Document

## Overview

<How the feature works end to end, in a few paragraphs. Which packages change and which
stay untouched.>

### Research summary and key design decisions

<Each external behavior the design depends on, confirmed against current documentation,
and the decision it forces. Alternatives considered and why they were rejected.>

## Architecture

<Flow and component diagram (mermaid is fine). Where new branches slot into existing
code paths, with the real function names. Invariants (e.g., "a message is deleted only
after …").>

## Components and Interfaces

### <Component or interface>

<Go signatures, file location, responsibilities, collaborators, test doubles in `fake/`.>

## Data Models

<Types, message attributes, config structs, defaults, validation rules, formulas.>

## Error Handling

<Each failure mode → sentinel error (in `errors/`), wrapping, logging, and the resulting
message disposition. No panics.>

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid
executions of a system — essentially, a formal statement about what the system should
do. Properties serve as the bridge between human-readable specifications and
machine-verifiable correctness guarantees.*

### Property 1: <Name>

*For any* <generated inputs and constraints>, <what must hold>.

**Validates: Requirements x.y, x.z**

## Testing Strategy

### Property-based tests

Property tests use `pgregory.net/rapid`, run a minimum of 100 iterations, live in
`_test` packages, and each is tagged with a comment referencing its design property:
`// Feature: <feature-name>, Property {n}: {property text}`.

- **Property 1** — <generators, including boundary generators; what is asserted; which
  fakes observe the behavior>.

### Example and integration tests (PBT not appropriate)

<Criteria covered by examples or LocalStack integration tests (`-tags=integration`).>

### Unit tests

<Remaining targeted unit tests: validation errors, backward compatibility.>
```

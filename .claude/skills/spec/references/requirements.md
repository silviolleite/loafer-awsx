# requirements.md template

```markdown
# Requirements Document

## Introduction

<What the feature adds or changes, why, and for whom. What is explicitly out of scope.
Which existing behavior stays unchanged (and is the default). Accepted tradeoffs, stated
plainly — they are also captured as requirements below.>

## Glossary

- **Term_Name**: <definition>. <Relationship to other glossary terms.>

## Requirements

### Requirement 1: <Short title>

**User Story:** As a <role>, I want <capability>, so that <benefit>.

#### Acceptance Criteria

1. THE <System_Term> SHALL <response>.
2. WHEN <trigger>, THE <System_Term> SHALL <response>.
3. WHILE <state>, THE <System_Term> SHALL <response>.
4. WHERE <optional feature is enabled/configured>, THE <System_Term> SHALL <response>.
5. IF <unwanted condition>, THEN THE <System_Term> SHALL <response>.
```

## EARS patterns

| Pattern | Form | Use for |
| --- | --- | --- |
| Ubiquitous | `THE <system> SHALL <response>` | Always-true behavior |
| Event-driven | `WHEN <trigger>, THE <system> SHALL …` | Reaction to an event |
| State-driven | `WHILE <state>, THE <system> SHALL …` | Behavior during a state |
| Optional feature | `WHERE <feature>, THE <system> SHALL …` | Configurable behavior |
| Unwanted behavior | `IF <condition>, THEN THE <system> SHALL …` | Errors, invalid input, failures |
| Complex | `WHILE <state>, WHEN <trigger>, THE <system> SHALL …` | Combinations |

Rules:

- One `SHALL` per criterion; split compound behavior into separate criteria.
- Use glossary terms verbatim; no pronouns, no "etc.", no "appropriately/quickly".
- Negative guarantees use `SHALL NOT`.
- Quantify limits (counts, durations, sizes) and say whether they are inclusive.
- Criteria are numbered `<requirement>.<n>` and referenced that way by design and tasks.

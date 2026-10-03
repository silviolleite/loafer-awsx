---
name: spec-run
description: Implements tasks from an approved spec in .claude/specs/<feature>/tasks.md, one task at a time, verifying each against its requirements and design properties and checking it off. Use when the user asks to implement, continue, or execute a spec, a spec task (e.g. "do task 2.3"), or the next pending task.
argument-hint: "<feature-name> [task-id | next | all]"
---

# Execute spec tasks

Arguments: `$ARGUMENTS` → `<feature-name>` plus an optional task id (`2.3`), `next`
(default), or `all`.

## Before the first task

1. Read `.claude/specs/<feature-name>/requirements.md`, `design.md`, and `tasks.md` in
   full. If any is missing or the spec looks unapproved, stop and suggest `/spec`.
2. Load `golang-how-to` (samber/cc-skills-golang) and the skills it routes to for the
   task at hand.

## Per task

1. Pick the task: the requested id, or the first unchecked non-optional leaf task whose
   dependencies (earlier waves in the Task Dependency Graph) are all checked. Skip
   optional `*` tasks unless the user asked for them or for `all`.
2. Re-read the requirement criteria in its `_Requirements:_` line and, for property
   tasks, the property text in `design.md`.
3. Implement exactly what the task describes — nothing from later tasks. If the design
   turns out to be wrong or incomplete, stop and propose a spec change instead of
   silently deviating.
4. Tests: delegate test-writing tasks to the `go-test-engineer` agent, passing the
   feature name, property numbers, and target files. Property tests carry the tag
   `// Feature: <feature-name>, Property <n>: <property text>`.
5. Verify: `go build ./... && go vet ./...` and `go test -race ./<touched-pkgs>/...`.
   For a `Checkpoint` task run `make check` and report the result.
6. Mark the task `[x]` in `tasks.md` (and its parent when all children are done).
7. Report: what changed (files), which criteria/properties it satisfies, verification
   output. With `next`, stop here and wait. With `all`, continue to the next task but
   stop at every Checkpoint and on any failure or open question.

Do not commit unless the user asks. When asked, use Conventional Commits with the
package as scope.

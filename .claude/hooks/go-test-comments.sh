#!/usr/bin/env bash
# PostToolUse (Edit|Write|MultiEdit): enforce "no comments in tests" for external _test packages.
# Allowed: the spec property tag (// Feature: ...), //go: directives, and //nolint.
set -uo pipefail

file=$(jq -r '.tool_input.file_path // empty')
[[ "$file" == *_test.go && -f "$file" ]] || exit 0
grep -qE '^package [a-z0-9_]+_test$' "$file" || exit 0

violations=$(grep -nE '^\s*(//|/\*)' "$file" | grep -vE '^[0-9]+:\s*//(go:|nolint| Feature: )')
[[ -z "$violations" ]] && exit 0

{
  echo "$file: comments are not allowed in tests (see .claude/rules/go-tests.md)."
  echo "Remove them; only the '// Feature: <spec>, Property <n>: ...' tag is allowed."
  echo "$violations"
} >&2
exit 2

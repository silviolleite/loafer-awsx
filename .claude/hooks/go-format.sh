#!/usr/bin/env bash
# PostToolUse (Edit|Write|MultiEdit): format the edited Go file the same way `make format` does.
set -uo pipefail

file=$(jq -r '.tool_input.file_path // empty')
[[ "$file" == *.go && -f "$file" ]] || exit 0

if command -v goimports >/dev/null 2>&1; then
  out=$(goimports -local github.com/silviolleite/loafer-awsx -w "$file" 2>&1)
else
  out=$(gofmt -w "$file" 2>&1)
fi
rc=$?

if [[ $rc -ne 0 ]]; then
  echo "Formatting failed for $file (fix the syntax error):" >&2
  echo "$out" >&2
  exit 2
fi

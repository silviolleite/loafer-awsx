#!/usr/bin/env bash
# Stop: when Go files changed, make sure the module still builds and passes go vet before
# Claude hands control back. Blocks once; on the retry (stop_hook_active) it lets Claude stop.
set -uo pipefail

input=$(cat)
[[ "$(jq -r '.stop_hook_active // false' <<<"$input")" == "true" ]] && exit 0
cd "${CLAUDE_PROJECT_DIR:-.}" || exit 0

[[ -n "$(git status --porcelain -- '*.go' 2>/dev/null)" ]] || exit 0

if ! out=$( { go build ./... && go vet ./...; } 2>&1 ); then
  jq -n --arg reason "go build/go vet failed after your Go changes. Fix before finishing:
$out" '{decision: "block", reason: $reason}'
fi
exit 0

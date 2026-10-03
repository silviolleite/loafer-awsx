#!/usr/bin/env bash
# PostToolUse (Edit|Write|MultiEdit|Bash): after go.mod changes, flag deprecated or retracted
# direct dependencies using the module proxy (`go list -m -u`).
set -uo pipefail

input=$(cat)
tool=$(jq -r '.tool_name' <<<"$input")
cd "${CLAUDE_PROJECT_DIR:-.}" || exit 0

modules=()
if [[ "$tool" == "Bash" ]]; then
  cmd=$(jq -r '.tool_input.command // empty' <<<"$input")
  grep -qE 'go (get|mod (tidy|edit))' <<<"$cmd" || exit 0
  while IFS= read -r gomod; do
    modules+=("$(dirname "$gomod")")
  done < <(git status --porcelain -- go.mod '*/go.mod' | awk '{print $NF}')
else
  file=$(jq -r '.tool_input.file_path // empty' <<<"$input")
  [[ "$(basename "$file")" == "go.mod" ]] || exit 0
  modules+=("$(dirname "$file")")
fi
[[ ${#modules[@]} -gt 0 ]] || exit 0

report=""
for dir in "${modules[@]}"; do
  deps=$(cd "$dir" && go mod edit -json | jq -r '.Require[]? | select(.Indirect != true) | .Path')
  [[ -n "$deps" ]] || continue
  # shellcheck disable=SC2086
  found=$(cd "$dir" && go list -m -u \
    -f '{{if .Deprecated}}{{.Path}}@{{.Version}} DEPRECATED: {{.Deprecated}}{{end}}{{if .Retracted}}{{.Path}}@{{.Version}} RETRACTED: {{.Retracted}}{{end}}' \
    $deps 2>/dev/null | sed '/^$/d')
  [[ -n "$found" ]] && report+="[$dir/go.mod]"$'\n'"$found"$'\n'
done

[[ -z "$report" ]] && exit 0
{
  echo "Deprecated or retracted direct dependencies detected. Replace them with the recommended"
  echo "successor (verify on pkg.go.dev) or explain to the user why they must stay:"
  echo "$report"
} >&2
exit 2

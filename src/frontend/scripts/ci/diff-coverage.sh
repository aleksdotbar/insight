#!/usr/bin/env bash
# New-code coverage gate: fails when coverage of lines changed vs the base
# branch is below the threshold. Single source of truth for local + CI.
# Consumes the Cobertura report produced by `vitest run --coverage`.
#
# Scope note: coverage is logic-line based (statements/branches/functions).
# Bare JSX markup lines are NOT counted by the v8 provider, so a purely-markup
# change has no coverable lines and passes. The gate enforces coverage of new
# LOGIC (handlers, hooks, conditionals, data transforms).
set -euo pipefail

REPORT="${COBERTURA:-coverage/cobertura-coverage.xml}"
BASE_REF="${BASE_REF:-main}"
DIFF_MIN="${DIFF_MIN:-80}"
SUMMARY="${GITHUB_STEP_SUMMARY:-/dev/stdout}"

if ! command -v diff-cover >/dev/null 2>&1; then
  echo "error: diff-cover not found — install with 'pipx install diff-cover' (or 'pip install diff-cover')." >&2
  exit 1
fi
if [ ! -f "$REPORT" ]; then
  echo "error: coverage report '$REPORT' not found — run 'pnpm test:coverage' first." >&2
  exit 1
fi

diff-cover "$REPORT" \
  --compare-branch "origin/${BASE_REF}" \
  --fail-under "$DIFF_MIN" \
  --markdown-report "$SUMMARY"

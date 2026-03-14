#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

required_patterns=(
  "docs/"
  "node_modules/"
  "apps/web/.next/"
  "services/api/.venv/"
  "data/"
)

for pattern in "${required_patterns[@]}"; do
  if ! grep -Fxq "$pattern" .gitignore; then
    echo "missing .gitignore pattern: $pattern" >&2
    exit 1
  fi
done

tracked_generated="$(
  git ls-files | grep -E '^(docs/|node_modules/|apps/web/\.next/|services/api/\.venv/|data/|.*__pycache__/|.*\.pyc$|apps/web/tsconfig\.tsbuildinfo$)' || true
)"

if [[ -n "$tracked_generated" ]]; then
  echo "tracked generated or ignored content detected:" >&2
  echo "$tracked_generated" >&2
  exit 1
fi

echo "repo hygiene checks passed"

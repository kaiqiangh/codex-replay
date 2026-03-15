#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
API_BASE_URL="${API_BASE_URL:-http://127.0.0.1:8000/api/v1}"
WEB_BASE_URL="${WEB_BASE_URL:-http://127.0.0.1:3000}"
FIXTURE_PATH="$ROOT_DIR/scripts/ci/fixtures/smoke-trace.jsonl"

"$ROOT_DIR/scripts/ci/wait-for-http.sh" "$API_BASE_URL/health" 90
"$ROOT_DIR/scripts/ci/wait-for-http.sh" "$WEB_BASE_URL" 90

"$ROOT_DIR/scripts/ci/assert-http.sh" "$API_BASE_URL/health" "\"status\":\"ok\""
"$ROOT_DIR/scripts/ci/assert-http.sh" "$API_BASE_URL/ready" "\"status\":\"ready\""
"$ROOT_DIR/scripts/ci/assert-http.sh" "$WEB_BASE_URL" "Upload trace" >/dev/null
"$ROOT_DIR/scripts/ci/assert-http.sh" "$WEB_BASE_URL/runs" "Replay queue" >/dev/null

import_response="$(
  curl \
    --fail \
    --silent \
    --show-error \
    --location \
    --request POST \
    --form "file=@${FIXTURE_PATH};type=application/jsonl" \
    "$API_BASE_URL/imports/file"
)"

run_id="$(
  python3 - "$import_response" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
print(payload["data"]["run_id"])
PY
)"

if [[ -z "$run_id" ]]; then
  echo "smoke import did not return a run_id" >&2
  exit 1
fi

"$ROOT_DIR/scripts/ci/assert-http.sh" "$API_BASE_URL/runs" "$run_id" >/dev/null
"$ROOT_DIR/scripts/ci/assert-http.sh" "$API_BASE_URL/runs/$run_id" "\"id\":\"$run_id\"" >/dev/null
"$ROOT_DIR/scripts/ci/assert-http.sh" "$WEB_BASE_URL/runs/$run_id" "Replay workspace" >/dev/null

export_response="$(
  curl \
    --fail \
    --silent \
    --show-error \
    --location \
    --request POST \
    --header "Content-Type: application/json" \
    --data '{"format":"bundle","include_raw_artifacts":true}' \
    "$API_BASE_URL/runs/$run_id/exports"
)"

download_url="$(
  python3 - "$export_response" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
print(payload["data"]["download_url"])
PY
)"

if [[ "$download_url" != http://* && "$download_url" != https://* ]]; then
  api_origin="${API_BASE_URL%/api/v1}"
  download_url="${api_origin}${download_url}"
fi

bundle_path="$(mktemp "${TMPDIR:-/tmp}/codex-replay-smoke-bundle.XXXXXX.zip")"
trap 'rm -f "$bundle_path"' EXIT

curl --fail --silent --show-error --location "$download_url" --output "$bundle_path"

python3 - "$bundle_path" <<'PY'
import sys
import zipfile

with zipfile.ZipFile(sys.argv[1]) as bundle:
    names = set(bundle.namelist())
    required = {"manifest.json", "run.json", "events.jsonl", "checksums.json"}
    missing = required - names
    if missing:
        raise SystemExit(f"missing bundle entries: {sorted(missing)}")
PY

echo "smoke checks passed"

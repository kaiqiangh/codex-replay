#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 <url> [timeout-seconds]" >&2
  exit 64
fi

url="$1"
timeout_seconds="${2:-60}"
deadline=$((SECONDS + timeout_seconds))

while (( SECONDS < deadline )); do
  if curl --fail --silent --show-error --location "$url" >/dev/null 2>&1; then
    exit 0
  fi
  sleep 2
done

echo "timed out waiting for $url" >&2
exit 1

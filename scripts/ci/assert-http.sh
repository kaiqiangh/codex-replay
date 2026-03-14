#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 <url> [expected-substring]" >&2
  exit 64
fi

url="$1"
expected="${2:-}"

body="$(curl --fail --silent --show-error --location "$url")"

if [[ -n "$expected" ]] && ! grep -Fq "$expected" <<<"$body"; then
  echo "expected response from $url to contain: $expected" >&2
  echo "$body" >&2
  exit 1
fi

printf '%s\n' "$body"

#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

version="${1:-${GITHUB_REF_NAME:-dev}}"
release_dir="$ROOT_DIR/dist/release"
archive_name="codex-replay-${version}.tar.gz"
metadata_name="codex-replay-${version}-release.json"

mkdir -p "$release_dir"

git archive \
  --format=tar.gz \
  --prefix="codex-replay-${version}/" \
  --output="$release_dir/$archive_name" \
  HEAD

python3 - "$release_dir/$metadata_name" "$version" <<'PY'
import json
import subprocess
import sys
from datetime import datetime, timezone

output_path = sys.argv[1]
version = sys.argv[2]
commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()

payload = {
    "name": "codex-replay",
    "version": version,
    "commit": commit,
    "generated_at": datetime.now(timezone.utc).isoformat(),
}

with open(output_path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2)
    handle.write("\n")
PY

(
  cd "$release_dir"
  shasum -a 256 "$archive_name" "$metadata_name" > checksums.txt
)

echo "release artifacts created in $release_dir"

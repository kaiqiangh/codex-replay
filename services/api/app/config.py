from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    root_dir: Path
    data_dir: Path
    artifact_dir: Path
    raw_dir: Path
    blob_dir: Path
    export_dir: Path
    db_path: Path
    codex_home: Path
    discovery_interval_seconds: int
    blob_preview_limit: int
    blob_inline_limit: int


def get_settings() -> Settings:
    root_dir = Path(__file__).resolve().parents[3]
    data_dir = Path(os.environ.get("CODEX_REPLAY_DATA_DIR", root_dir / "data")).expanduser()
    artifact_dir = data_dir / "artifacts"
    raw_dir = artifact_dir / "raw"
    blob_dir = artifact_dir / "blobs"
    export_dir = data_dir / "exports"
    db_path = Path(os.environ.get("CODEX_REPLAY_DB_PATH", data_dir / "replay.db")).expanduser()
    codex_home = Path(os.environ.get("CODEX_REPLAY_CODEX_HOME", Path.home() / ".codex")).expanduser()
    return Settings(
        root_dir=root_dir,
        data_dir=data_dir,
        artifact_dir=artifact_dir,
        raw_dir=raw_dir,
        blob_dir=blob_dir,
        export_dir=export_dir,
        db_path=db_path,
        codex_home=codex_home,
        discovery_interval_seconds=int(os.environ.get("CODEX_REPLAY_DISCOVERY_INTERVAL", "300")),
        blob_preview_limit=int(os.environ.get("CODEX_REPLAY_BLOB_PREVIEW_LIMIT", "2400")),
        blob_inline_limit=int(os.environ.get("CODEX_REPLAY_BLOB_INLINE_LIMIT", "32000")),
    )

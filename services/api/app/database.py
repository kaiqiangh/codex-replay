from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS discovered_sources (
    id TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    absolute_path TEXT,
    artifact_name TEXT,
    session_id TEXT,
    thread_name TEXT,
    source_updated_at TEXT,
    discovered_at TEXT NOT NULL,
    status TEXT NOT NULL,
    fingerprint TEXT,
    run_id TEXT,
    is_active INTEGER NOT NULL DEFAULT 0,
    warning TEXT,
    metadata_json TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_sources_kind_status ON discovered_sources(source_kind, status);
CREATE INDEX IF NOT EXISTS idx_sources_session_id ON discovered_sources(session_id);
DROP INDEX IF EXISTS idx_sources_provider_path_kind;
CREATE UNIQUE INDEX IF NOT EXISTS idx_sources_provider_kind_path_session
ON discovered_sources(provider, source_kind, absolute_path, session_id);

CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    source_id TEXT,
    provider TEXT NOT NULL,
    provider_run_id TEXT,
    session_id TEXT,
    repo_name TEXT,
    repo_root TEXT,
    source_name TEXT,
    source_path TEXT,
    import_mode TEXT NOT NULL,
    run_status TEXT NOT NULL,
    is_partial INTEGER NOT NULL DEFAULT 0,
    parse_status TEXT NOT NULL,
    provider_version TEXT,
    normalization_version TEXT NOT NULL,
    adapter_version TEXT NOT NULL,
    prompt TEXT,
    review_attention TEXT NOT NULL DEFAULT 'low',
    status_reason TEXT,
    summary_markdown TEXT,
    summary_json TEXT,
    validation_summary TEXT,
    changed_files_summary TEXT,
    failure_summary TEXT,
    reviewer_notes TEXT,
    warnings_json TEXT,
    started_at TEXT,
    ended_at TEXT,
    duration_ms INTEGER,
    total_events INTEGER NOT NULL DEFAULT 0,
    total_commands INTEGER NOT NULL DEFAULT 0,
    total_tests INTEGER NOT NULL DEFAULT 0,
    total_errors INTEGER NOT NULL DEFAULT 0,
    total_files_changed INTEGER NOT NULL DEFAULT 0,
    first_error_seq INTEGER,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (source_id) REFERENCES discovered_sources(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_runs_provider ON runs(provider);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(run_status);
CREATE INDEX IF NOT EXISTS idx_runs_started_at ON runs(started_at DESC);

CREATE TABLE IF NOT EXISTS artifacts (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    artifact_type TEXT NOT NULL,
    filename TEXT,
    mime_type TEXT,
    sha256 TEXT,
    size_bytes INTEGER NOT NULL DEFAULT 0,
    storage_path TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_artifacts_run_id ON artifacts(run_id);

CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    parent_seq INTEGER,
    provider_event_id TEXT,
    provider_event_type TEXT,
    event_type TEXT NOT NULL,
    title TEXT,
    status TEXT,
    timestamp TEXT,
    step_group TEXT,
    step_label TEXT,
    message_text TEXT,
    raw_payload_json TEXT,
    artifact_ref_id TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE,
    FOREIGN KEY (artifact_ref_id) REFERENCES artifacts(id) ON DELETE SET NULL,
    UNIQUE (run_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_events_run_seq ON events(run_id, seq);
CREATE INDEX IF NOT EXISTS idx_events_run_type ON events(run_id, event_type);

CREATE TABLE IF NOT EXISTS command_events (
    event_id TEXT PRIMARY KEY,
    command_text TEXT NOT NULL,
    cwd TEXT,
    shell TEXT,
    exit_code INTEGER,
    duration_ms INTEGER,
    stdout_preview TEXT,
    stderr_preview TEXT,
    stdout_artifact_id TEXT,
    stderr_artifact_id TEXT,
    retry_group_id TEXT,
    retry_index INTEGER,
    is_validation INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE,
    FOREIGN KEY (stdout_artifact_id) REFERENCES artifacts(id) ON DELETE SET NULL,
    FOREIGN KEY (stderr_artifact_id) REFERENCES artifacts(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS test_events (
    event_id TEXT PRIMARY KEY,
    command_text TEXT,
    framework TEXT,
    result TEXT,
    passed_count INTEGER,
    failed_count INTEGER,
    skipped_count INTEGER,
    duration_ms INTEGER,
    stdout_preview TEXT,
    stderr_preview TEXT,
    FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS diff_events (
    event_id TEXT PRIMARY KEY,
    file_path TEXT NOT NULL,
    normalized_path TEXT NOT NULL,
    change_type TEXT NOT NULL,
    lines_added INTEGER NOT NULL DEFAULT 0,
    lines_removed INTEGER NOT NULL DEFAULT 0,
    hunks_count INTEGER NOT NULL DEFAULT 0,
    diff_text TEXT,
    diff_artifact_id TEXT,
    FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE,
    FOREIGN KEY (diff_artifact_id) REFERENCES artifacts(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_diff_events_path ON diff_events(normalized_path);

CREATE TABLE IF NOT EXISTS error_events (
    event_id TEXT PRIMARY KEY,
    error_code TEXT,
    error_type TEXT,
    severity TEXT,
    message TEXT NOT NULL,
    related_seq INTEGER,
    is_terminal INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tool_events (
    event_id TEXT PRIMARY KEY,
    tool_type TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    input_preview TEXT,
    output_preview TEXT,
    duration_ms INTEGER,
    FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS skill_signals (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    name TEXT NOT NULL,
    mode TEXT NOT NULL,
    confidence REAL NOT NULL,
    event_ids_json TEXT NOT NULL,
    evidence_source TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE,
    FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_skill_signals_run_id ON skill_signals(run_id);

CREATE TABLE IF NOT EXISTS insights (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    code TEXT NOT NULL,
    severity TEXT NOT NULL,
    title TEXT NOT NULL,
    message TEXT NOT NULL,
    recommendation TEXT,
    event_ids_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_insights_run_id ON insights(run_id);

CREATE TABLE IF NOT EXISTS exports (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    export_format TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE,
    FOREIGN KEY (artifact_id) REFERENCES artifacts(id) ON DELETE CASCADE
);
"""


class Database:
    def __init__(self, db_path: Path):
        self.db_path = db_path

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as connection:
            connection.executescript(SCHEMA_SQL)

    def fetchone(self, query: str, params: Sequence[Any] = ()) -> Optional[sqlite3.Row]:
        with self.connection() as connection:
            return connection.execute(query, params).fetchone()

    def fetchall(self, query: str, params: Sequence[Any] = ()) -> List[sqlite3.Row]:
        with self.connection() as connection:
            return connection.execute(query, params).fetchall()

    def execute(self, query: str, params: Sequence[Any] = ()) -> None:
        with self.connection() as connection:
            connection.execute(query, params)

    def executemany(self, query: str, params: Iterable[Sequence[Any]]) -> None:
        with self.connection() as connection:
            connection.executemany(query, params)

    def replace_run(self, run: Dict[str, Any], events: List[Dict[str, Any]], details: Dict[str, List[Dict[str, Any]]], insights: List[Dict[str, Any]], skills: List[Dict[str, Any]], artifacts: List[Dict[str, Any]]) -> None:
        with self.connection() as connection:
            connection.execute("DELETE FROM runs WHERE id = ?", (run["id"],))
            connection.execute(
                """
                INSERT INTO runs (
                    id, source_id, provider, provider_run_id, session_id, repo_name, repo_root, source_name, source_path,
                    import_mode, run_status, is_partial, parse_status, provider_version, normalization_version,
                    adapter_version, prompt, review_attention, status_reason, summary_markdown, summary_json,
                    validation_summary, changed_files_summary, failure_summary, reviewer_notes, warnings_json,
                    started_at, ended_at, duration_ms, total_events, total_commands, total_tests, total_errors,
                    total_files_changed, first_error_seq
                ) VALUES (
                    :id, :source_id, :provider, :provider_run_id, :session_id, :repo_name, :repo_root, :source_name, :source_path,
                    :import_mode, :run_status, :is_partial, :parse_status, :provider_version, :normalization_version,
                    :adapter_version, :prompt, :review_attention, :status_reason, :summary_markdown, :summary_json,
                    :validation_summary, :changed_files_summary, :failure_summary, :reviewer_notes, :warnings_json,
                    :started_at, :ended_at, :duration_ms, :total_events, :total_commands, :total_tests, :total_errors,
                    :total_files_changed, :first_error_seq
                )
                """,
                run,
            )
            if artifacts:
                connection.executemany(
                    """
                    INSERT INTO artifacts (
                        id, run_id, artifact_type, filename, mime_type, sha256, size_bytes, storage_path
                    ) VALUES (
                        :id, :run_id, :artifact_type, :filename, :mime_type, :sha256, :size_bytes, :storage_path
                    )
                    """,
                    artifacts,
                )
            if events:
                connection.executemany(
                    """
                    INSERT INTO events (
                        id, run_id, seq, parent_seq, provider_event_id, provider_event_type, event_type, title,
                        status, timestamp, step_group, step_label, message_text, raw_payload_json, artifact_ref_id
                    ) VALUES (
                        :id, :run_id, :seq, :parent_seq, :provider_event_id, :provider_event_type, :event_type, :title,
                        :status, :timestamp, :step_group, :step_label, :message_text, :raw_payload_json, :artifact_ref_id
                    )
                    """,
                    events,
                )
            for table, rows in details.items():
                if not rows:
                    continue
                columns = rows[0].keys()
                column_sql = ", ".join(columns)
                value_sql = ", ".join(f":{column}" for column in columns)
                connection.executemany(f"INSERT INTO {table} ({column_sql}) VALUES ({value_sql})", rows)
            if insights:
                connection.executemany(
                    """
                    INSERT INTO insights (
                        id, run_id, code, severity, title, message, recommendation, event_ids_json
                    ) VALUES (
                        :id, :run_id, :code, :severity, :title, :message, :recommendation, :event_ids_json
                    )
                    """,
                    insights,
                )
            if skills:
                connection.executemany(
                    """
                    INSERT INTO skill_signals (
                        id, run_id, event_id, name, mode, confidence, event_ids_json, evidence_source
                    ) VALUES (
                        :id, :run_id, :event_id, :name, :mode, :confidence, :event_ids_json, :evidence_source
                    )
                    """,
                    skills,
                )


def row_to_dict(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    return dict(row)


def rows_to_dicts(rows: Sequence[sqlite3.Row]) -> List[Dict[str, Any]]:
    return [dict(row) for row in rows]


def json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)

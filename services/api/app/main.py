from __future__ import annotations

import asyncio
import os
import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from .codex import DiscoveryService, ReplayService, now_iso
from .config import get_settings
from .database import Database, json_dump, row_to_dict, rows_to_dicts


settings = get_settings()
database = Database(settings.db_path)
replay_service = ReplayService(settings, database)
discovery_service = DiscoveryService(settings, database, replay_service)

RUN_SELECT = """
SELECT runs.*,
       (
           SELECT ds.thread_name
           FROM discovered_sources ds
           WHERE ds.session_id = runs.session_id
             AND ds.thread_name IS NOT NULL
           ORDER BY ds.discovered_at DESC
           LIMIT 1
       ) AS session_title
FROM runs
"""


def response(data: Any, request_id: Optional[str] = None) -> Dict[str, Any]:
    return {
        "data": data,
        "meta": {
            "request_id": request_id or f"req_{uuid4().hex[:10]}",
            "version": "v1",
        },
    }


def error(status_code: int, code: str, message: str, details: Optional[Dict[str, Any]] = None) -> HTTPException:
    raise HTTPException(
        status_code=status_code,
        detail={
            "error": {
                "code": code,
                "message": message,
                "details": details or {},
            },
            "meta": {
                "request_id": f"req_{uuid4().hex[:10]}",
                "version": "v1",
            },
        },
    )


async def scan_loop() -> None:
    await asyncio.to_thread(discovery_service.scan)
    while True:
        await asyncio.sleep(settings.discovery_interval_seconds)
        try:
            await asyncio.to_thread(discovery_service.scan)
        except Exception:
            discovery_service.last_scan_warning = "Background discovery scan failed."


@asynccontextmanager
async def lifespan(_: FastAPI):
    replay_service.ensure_directories()
    database.initialize()
    task = asyncio.create_task(scan_loop())
    try:
        yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


import contextlib

app = FastAPI(title="codex-replay API", version="0.1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(HTTPException)
async def http_exception_handler(_, exc: HTTPException):
    if isinstance(exc.detail, dict) and "error" in exc.detail:
        return fastapi.responses.JSONResponse(status_code=exc.status_code, content=exc.detail)
    return fastapi.responses.JSONResponse(status_code=exc.status_code, content={"error": {"code": "http_error", "message": str(exc.detail), "details": {}}, "meta": {"request_id": f"req_{uuid4().hex[:10]}", "version": "v1"}})


import fastapi.responses


@app.get("/api/v1/health")
def health() -> Dict[str, Any]:
    return response({"status": "ok"})


@app.get("/api/v1/ready")
def ready() -> Dict[str, Any]:
    return response(
        {
            "status": "ready",
            "database": "ok" if settings.db_path.parent.exists() else "missing",
            "artifact_store": "ok" if settings.artifact_dir.exists() else "missing",
        }
    )


@app.get("/api/v1/discovery/status")
def discovery_status() -> Dict[str, Any]:
    status_counts = rows_to_dicts(
        database.fetchall(
            "SELECT status, COUNT(*) AS count FROM discovered_sources GROUP BY status ORDER BY status"
        )
    )
    return response(
        {
            "last_scan_at": discovery_service.last_scan_at,
            "in_progress": discovery_service.scan_in_progress,
            "warning": discovery_service.last_scan_warning,
            "counts": discovery_service.scan_counts,
            "status_counts": status_counts,
        }
    )


@app.post("/api/v1/discovery/scan")
def discovery_scan() -> Dict[str, Any]:
    return response(discovery_service.scan())


@app.get("/api/v1/discovery/sources")
def discovery_sources(
    status: Optional[str] = Query(default=None),
    source_kind: Optional[str] = Query(default=None),
    query: Optional[str] = Query(default=None, alias="q"),
    limit: int = Query(default=25, ge=1, le=200),
) -> Dict[str, Any]:
    sql = "SELECT * FROM discovered_sources WHERE 1=1"
    params: List[Any] = []
    if status:
        sql += " AND status = ?"
        params.append(status)
    if source_kind:
        sql += " AND source_kind = ?"
        params.append(source_kind)
    if query:
        sql += " AND (COALESCE(thread_name, '') LIKE ? OR COALESCE(artifact_name, '') LIKE ? OR COALESCE(session_id, '') LIKE ?)"
        params.extend([f"%{query}%"] * 3)
    sql += " ORDER BY COALESCE(source_updated_at, discovered_at) DESC LIMIT ?"
    params.append(limit)
    items = rows_to_dicts(database.fetchall(sql, params))
    for item in items:
        item["is_active"] = bool(item["is_active"])
    return response({"items": items, "total": len(items)})


@app.post("/api/v1/imports/file")
async def import_file(file: UploadFile = File(...), provider: str = "auto", source_name: Optional[str] = None) -> Dict[str, Any]:
    if provider not in {"auto", "codex"}:
        error(400, "unsupported_provider", "Only Codex imports are supported in v0.1.")
    raw = await file.read()
    try:
        result = replay_service.import_bytes(raw, original_name=file.filename or "upload.bin", source_name=source_name, import_mode="manual")
    except Exception as exc:
        error(400, "invalid_import_format", "Could not parse the uploaded artifact.", {"reason": str(exc)})
    return response(result)


@app.post("/api/v1/imports/path")
def import_path(payload: Dict[str, Any]) -> Dict[str, Any]:
    raw_path = payload.get("path")
    if not raw_path:
        error(400, "missing_path", "A local file path is required.")
    path = Path(raw_path).expanduser()
    if not path.exists():
        error(404, "path_not_found", "The specified file path does not exist.")
    try:
        result = replay_service.import_path(path, source_name=payload.get("source_name"), import_mode="manual")
    except Exception as exc:
        error(400, "invalid_import_format", "Could not parse the local artifact.", {"reason": str(exc)})
    return response(result)


@app.get("/api/v1/runs")
def list_runs(
    provider: Optional[str] = None,
    status: Optional[str] = None,
    q: Optional[str] = None,
    limit: int = Query(default=25, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    sort: str = Query(default="started_at_desc"),
) -> Dict[str, Any]:
    sql = f"{RUN_SELECT} WHERE 1=1"
    params: List[Any] = []
    if provider:
        sql += " AND provider = ?"
        params.append(provider)
    if status:
        sql += " AND run_status = ?"
        params.append(status)
    if q:
        sql += " AND (COALESCE(repo_name, '') LIKE ? OR COALESCE(prompt, '') LIKE ? OR COALESCE(source_name, '') LIKE ?)"
        params.extend([f"%{q}%"] * 3)
    order_by = "started_at DESC" if sort != "started_at_asc" else "started_at ASC"
    total_row = row_to_dict(database.fetchone(f"SELECT COUNT(*) AS total FROM ({sql})", params))
    sql += f" ORDER BY {order_by} LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    items = rows_to_dicts(database.fetchall(sql, params))
    for item in items:
        item["status"] = item.pop("run_status")
    return response({"items": items, "total": total_row["total"] if total_row else len(items)})


def load_run(run_id: str) -> Dict[str, Any]:
    run = row_to_dict(database.fetchone(f"{RUN_SELECT} WHERE runs.id = ?", (run_id,)))
    if not run:
        error(404, "run_not_found", "Run not found.")
    return run


@app.get("/api/v1/runs/{run_id}")
def get_run(run_id: str) -> Dict[str, Any]:
    run = load_run(run_id)
    return response(
        {
            **run,
            "status": run["run_status"],
            "counts": {
                "events": run["total_events"],
                "commands": run["total_commands"],
                "tests": run["total_tests"],
                "errors": run["total_errors"],
                "files_changed": run["total_files_changed"],
            },
            "summary_status": "ready" if run["summary_markdown"] else "pending",
            "insights_status": "ready",
        }
    )


@app.delete("/api/v1/runs/{run_id}")
def delete_run(run_id: str) -> Dict[str, Any]:
    load_run(run_id)
    replay_service.delete_run(run_id)
    return response({"deleted": True, "run_id": run_id})


@app.get("/api/v1/runs/{run_id}/events")
def get_events(run_id: str, types: Optional[str] = None, limit: int = 200, offset: int = 0, include_payload: bool = False) -> Dict[str, Any]:
    load_run(run_id)
    sql = "SELECT * FROM events WHERE run_id = ?"
    params: List[Any] = [run_id]
    if types:
        wanted = [item.strip() for item in types.split(",") if item.strip()]
        if wanted:
            sql += f" AND event_type IN ({', '.join('?' for _ in wanted)})"
            params.extend(wanted)
    total_row = row_to_dict(database.fetchone(f"SELECT COUNT(*) AS total FROM ({sql})", params))
    sql += " ORDER BY seq LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    items = rows_to_dicts(database.fetchall(sql, params))
    if not include_payload:
        for item in items:
            item.pop("raw_payload_json", None)
    return response({"items": items, "total": total_row["total"] if total_row else len(items)})


@app.get("/api/v1/runs/{run_id}/events/{event_id}")
def get_event(run_id: str, event_id: str) -> Dict[str, Any]:
    event = row_to_dict(database.fetchone("SELECT * FROM events WHERE run_id = ? AND id = ?", (run_id, event_id)))
    if not event:
        error(404, "event_not_found", "Event not found.")
    detail = replay_service.get_event_detail(run_id, event_id)
    payload = json.loads(event["raw_payload_json"]) if event["raw_payload_json"] else {}
    event["raw_payload"] = payload
    return response({**event, "detail": detail})


@app.get("/api/v1/runs/{run_id}/timeline")
def get_timeline(run_id: str) -> Dict[str, Any]:
    load_run(run_id)
    rows = rows_to_dicts(
        database.fetchall(
            """
            SELECT e.seq, e.id AS event_id, e.event_type, e.title AS label, e.status,
                   EXISTS(SELECT 1 FROM diff_events d WHERE d.event_id = e.id) AS has_diff,
                   EXISTS(SELECT 1 FROM error_events er WHERE er.event_id = e.id) AS has_error,
                   EXISTS(SELECT 1 FROM skill_signals s WHERE s.event_id = e.id) AS has_skill
            FROM events e
            WHERE e.run_id = ?
            ORDER BY e.seq
            """,
            (run_id,),
        )
    )
    for row in rows:
        row["has_diff"] = bool(row["has_diff"])
        row["has_error"] = bool(row["has_error"])
        row["has_skill"] = bool(row["has_skill"])
    return response({"items": rows})


@app.get("/api/v1/runs/{run_id}/diffs")
def list_diffs(run_id: str, path: Optional[str] = None, limit: int = 200, offset: int = 0) -> Dict[str, Any]:
    load_run(run_id)
    sql = """
    SELECT d.event_id, e.seq, d.file_path, d.change_type, d.lines_added, d.lines_removed
    FROM diff_events d
    JOIN events e ON e.id = d.event_id
    WHERE e.run_id = ?
    """
    params: List[Any] = [run_id]
    if path:
        sql += " AND d.normalized_path LIKE ?"
        params.append(f"%{path}%")
    total_row = row_to_dict(database.fetchone(f"SELECT COUNT(*) AS total FROM ({sql})", params))
    sql += " ORDER BY e.seq LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    return response({"items": rows_to_dicts(database.fetchall(sql, params)), "total": total_row["total"] if total_row else 0})


@app.get("/api/v1/runs/{run_id}/diffs/{event_id}")
def get_diff(run_id: str, event_id: str) -> Dict[str, Any]:
    detail = replay_service.get_event_detail(run_id, event_id)
    diff = detail.get("diff")
    if not diff:
        error(404, "diff_not_found", "Diff not found.")
    return response(diff)


@app.get("/api/v1/runs/{run_id}/summary")
def get_summary(run_id: str) -> Dict[str, Any]:
    run = load_run(run_id)
    return response({"markdown": run["summary_markdown"] or "", "status": "ready" if run["summary_markdown"] else "pending"})


@app.get("/api/v1/runs/{run_id}/insights")
def get_insights(run_id: str) -> Dict[str, Any]:
    load_run(run_id)
    items = rows_to_dicts(database.fetchall("SELECT * FROM insights WHERE run_id = ? ORDER BY created_at ASC", (run_id,)))
    for item in items:
        item["event_ids"] = json.loads(item.pop("event_ids_json"))
    return response({"items": items})


@app.get("/api/v1/runs/{run_id}/skills")
def get_skills(run_id: str) -> Dict[str, Any]:
    load_run(run_id)
    items = rows_to_dicts(
        database.fetchall(
            """
            SELECT s.*, e.seq AS first_seq
            FROM skill_signals s
            JOIN events e ON e.id = s.event_id
            WHERE s.run_id = ?
            ORDER BY
                CASE s.mode
                    WHEN 'explicit' THEN 0
                    WHEN 'declared' THEN 1
                    WHEN 'implicit' THEN 2
                    WHEN 'inferred' THEN 3
                    ELSE 4
                END,
                e.seq
            """,
            (run_id,),
        )
    )
    for item in items:
        item["event_ids"] = json.loads(item.pop("event_ids_json"))
    return response({"items": items})


@app.post("/api/v1/runs/{run_id}/exports")
def create_export(run_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    load_run(run_id)
    export = replay_service.build_export(run_id, include_raw_artifacts=bool(payload.get("include_raw_artifacts", True)))
    return response(
        {
            "export_id": export["export_id"],
            "artifact_id": export["artifact_id"],
            "download_url": f"/api/v1/exports/{export['export_id']}/download",
        }
    )


@app.get("/api/v1/exports/{export_id}/download")
def download_export(export_id: str) -> FileResponse:
    export = row_to_dict(
        database.fetchone(
            """
            SELECT e.id, a.storage_path, a.filename
            FROM exports e
            JOIN artifacts a ON a.id = e.artifact_id
            WHERE e.id = ?
            """,
            (export_id,),
        )
    )
    if not export:
        error(404, "export_not_found", "Export not found.")
    return FileResponse(path=export["storage_path"], filename=export["filename"], media_type="application/zip")

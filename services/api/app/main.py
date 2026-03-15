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

from .codex import DiscoveryService, ReplayService, now_iso, run_state_label
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
    state: Optional[str] = None,
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
    if state == "ready":
        sql += " AND run_status = 'completed' AND COALESCE(is_partial, 0) = 0"
    elif state == "partial":
        sql += " AND run_status != 'failed' AND (run_status = 'unknown' OR COALESCE(is_partial, 0) = 1)"
    elif state == "unresolved":
        sql += " AND run_status = 'failed'"
    if q:
        sql += " AND (COALESCE(repo_name, '') LIKE ? OR COALESCE(prompt, '') LIKE ? OR COALESCE(source_name, '') LIKE ?)"
        params.extend([f"%{q}%"] * 3)
    sort_map = {
        "started_at_desc": "started_at DESC",
        "started_at_asc": "started_at ASC",
        "review_attention_desc": """
            CASE review_attention
                WHEN 'high' THEN 0
                WHEN 'medium' THEN 1
                ELSE 2
            END,
            started_at DESC
        """,
        "review_attention_asc": """
            CASE review_attention
                WHEN 'low' THEN 0
                WHEN 'medium' THEN 1
                ELSE 2
            END,
            started_at DESC
        """,
    }
    order_by = sort_map.get(sort, sort_map["started_at_desc"])
    total_row = row_to_dict(database.fetchone(f"SELECT COUNT(*) AS total FROM ({sql})", params))
    sql += f" ORDER BY {order_by} LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    items = [hydrate_run(item) for item in rows_to_dicts(database.fetchall(sql, params))]
    return response({"items": items, "total": total_row["total"] if total_row else len(items)})


def load_run(run_id: str) -> Dict[str, Any]:
    run = row_to_dict(database.fetchone(f"{RUN_SELECT} WHERE runs.id = ?", (run_id,)))
    if not run:
        error(404, "run_not_found", "Run not found.")
    return run


def run_state_key(run: Dict[str, Any]) -> str:
    if run.get("run_status") == "failed":
        return "unresolved"
    if run.get("is_partial") or run.get("run_status") == "unknown":
        return "partial"
    return "ready"


def hydrate_run(run: Dict[str, Any]) -> Dict[str, Any]:
    counts = replay_service.get_run_counts(run["id"], run_row=run)
    summary_payload = replay_service.build_summary_payload(run["id"], run_row=run)
    summary_json = summary_payload["json"]
    status = run["run_status"]
    return {
        **run,
        "status": status,
        "state_key": run_state_key(run),
        "state_label": run_state_label(status or "unknown", bool(run.get("is_partial"))),
        "counts": counts,
        "summary_status": "ready" if summary_payload["markdown"] else "pending",
        "insights_status": "ready",
        "task_summary": summary_json["task_summary"],
        "validation_summary": summary_json["validation_summary"],
        "changed_files_summary": summary_json["changed_files_summary"],
        "failure_summary": summary_json["failure_summary"],
        "reviewer_notes": summary_json["reviewer_notes"],
        "summary_markdown": summary_payload["markdown"],
        "first_error_seq": counts["first_error_seq"],
        "total_events": counts["events"],
        "total_commands": counts["commands"],
        "total_tests": counts["tests"],
        "total_errors": counts["errors"],
        "total_files_changed": counts["files_changed"],
    }


@app.get("/api/v1/runs/{run_id}")
def get_run(run_id: str) -> Dict[str, Any]:
    run = load_run(run_id)
    return response(hydrate_run(run))


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
    return response({"items": replay_service.get_visible_timeline(run_id)})


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
    summary_payload = replay_service.build_summary_payload(run_id, run_row=run)
    return response(
        {
            "markdown": summary_payload["markdown"],
            "json": summary_payload["json"],
            "status": "ready" if summary_payload["markdown"] else "pending",
        }
    )


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
    return response({"items": replay_service.get_clean_skill_signals(run_id)})


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

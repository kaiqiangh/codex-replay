from __future__ import annotations

import io
import importlib
import json
import os
import sys
import zipfile
from pathlib import Path

from fastapi.testclient import TestClient


def write_jsonl(path: Path, entries):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(item) for item in entries), encoding="utf-8")


def sample_entries():
    return [
        {
            "timestamp": "2026-03-14T10:00:00Z",
            "type": "session_meta",
            "payload": {
                "id": "sess_123",
                "timestamp": "2026-03-14T10:00:00Z",
                "cwd": "/Users/kai/dev/demo-repo",
                "cli_version": "0.115.0-alpha.11",
            },
        },
        {
            "timestamp": "2026-03-14T10:00:01Z",
            "type": "event_msg",
            "payload": {"type": "task_started", "turn_id": "turn_1"},
        },
        {
            "timestamp": "2026-03-14T10:00:02Z",
            "type": "event_msg",
            "payload": {
                "type": "user_message",
                "message": "[$frontend-design](/tmp/SKILL.md) improve the auth error state and rerun tests",
            },
        },
        {
            "timestamp": "2026-03-14T10:00:03Z",
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "name": "exec_command",
                "call_id": "call_1",
                "arguments": json.dumps({"cmd": "pytest tests/auth -q", "workdir": "/Users/kai/dev/demo-repo"}),
            },
        },
        {
            "timestamp": "2026-03-14T10:00:05Z",
            "type": "response_item",
            "payload": {
                "type": "function_call_output",
                "call_id": "call_1",
                "output": "Process exited with code 1\nOutput:\n1 failed, 2 passed",
            },
        },
        {
            "timestamp": "2026-03-14T10:00:06Z",
            "type": "response_item",
            "payload": {
                "type": "custom_tool_call",
                "name": "apply_patch",
                "input": "*** Begin Patch\n*** Update File: backend/auth.py\n@@\n- old\n+ new\n*** End Patch\n",
            },
        },
        {
            "timestamp": "2026-03-14T10:00:07Z",
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "name": "exec_command",
                "call_id": "call_2",
                "arguments": json.dumps({"cmd": "pytest tests/auth -q", "workdir": "/Users/kai/dev/demo-repo"}),
            },
        },
        {
            "timestamp": "2026-03-14T10:00:09Z",
            "type": "response_item",
            "payload": {
                "type": "function_call_output",
                "call_id": "call_2",
                "output": "Process exited with code 0\nOutput:\n3 passed",
            },
        },
        {
            "timestamp": "2026-03-14T10:00:10Z",
            "type": "event_msg",
            "payload": {"type": "task_complete", "turn_id": "turn_1"},
        },
    ]


def custom_tool_list_entries():
    return [
        {
            "timestamp": "2026-03-14T10:00:00Z",
            "type": "session_meta",
            "payload": {
                "id": "sess_listy",
                "timestamp": "2026-03-14T10:00:00Z",
                "cwd": "/Users/kai/dev/demo-repo",
                "cli_version": "0.115.0-alpha.11",
            },
        },
        {
            "timestamp": "2026-03-14T10:00:01Z",
            "type": "event_msg",
            "payload": {"type": "task_started", "turn_id": "turn_1"},
        },
        {
            "timestamp": "2026-03-14T10:00:02Z",
            "type": "response_item",
            "payload": {
                "type": "custom_tool_call",
                "name": "multi_tool_use.parallel",
                "input": [
                    {"recipient_name": "functions.exec_command", "parameters": {"cmd": "git status --short"}},
                    {"recipient_name": "functions.exec_command", "parameters": {"cmd": "pnpm --filter web build"}},
                ],
            },
        },
        {
            "timestamp": "2026-03-14T10:00:03Z",
            "type": "event_msg",
            "payload": {"type": "task_complete", "turn_id": "turn_1"},
        },
    ]


def setup_app(tmp_path: Path):
    codex_home = tmp_path / ".codex"
    os.environ["CODEX_REPLAY_CODEX_HOME"] = str(codex_home)
    os.environ["CODEX_REPLAY_DATA_DIR"] = str(tmp_path / "data")
    write_jsonl(codex_home / "sessions" / "2026" / "03" / "14" / "rollout-2026-03-14T10-00-00-sess_123.jsonl", sample_entries())
    write_jsonl(
        codex_home / "session_index.jsonl",
        [{"id": "sess_123", "thread_name": "Auth fix", "updated_at": "2026-03-14T10:00:10Z"}],
    )
    api_root = Path(__file__).resolve().parents[1]
    if str(api_root) not in sys.path:
        sys.path.insert(0, str(api_root))
    for module_name in ["app.main", "app.codex", "app.config", "app.database"]:
        sys.modules.pop(module_name, None)
    app_main = importlib.import_module("app.main")
    app = app_main.app
    discovery_service = app_main.discovery_service
    database = app_main.database

    database.initialize()
    discovery_service.scan()
    return TestClient(app)


def test_discovery_and_run_import(tmp_path: Path):
    client = setup_app(tmp_path)
    discovery = client.get("/api/v1/discovery/sources").json()["data"]["items"]
    assert any(item["source_kind"] == "codex_trace" for item in discovery)
    runs = client.get("/api/v1/runs").json()["data"]["items"]
    assert len(runs) == 1
    run_id = runs[0]["id"]
    timeline = client.get(f"/api/v1/runs/{run_id}/timeline").json()["data"]["items"]
    assert any(item["has_diff"] for item in timeline)
    skills = client.get(f"/api/v1/runs/{run_id}/skills").json()["data"]["items"]
    assert skills[0]["name"] == "frontend-design"


def test_manual_import_and_export_round_trip(tmp_path: Path):
    client = setup_app(tmp_path)
    upload = io.BytesIO("\n".join(json.dumps(item) for item in sample_entries()).encode("utf-8"))
    imported = client.post("/api/v1/imports/file", files={"file": ("manual.jsonl", upload, "application/jsonl")}).json()["data"]
    export = client.post(f"/api/v1/runs/{imported['run_id']}/exports", json={"format": "bundle", "include_raw_artifacts": True}).json()["data"]
    bundle_response = client.get(export["download_url"])
    assert bundle_response.status_code == 200
    bundle_bytes = io.BytesIO(bundle_response.content)
    with zipfile.ZipFile(bundle_bytes) as bundle:
        assert "manifest.json" in bundle.namelist()
        assert "run.json" in bundle.namelist()
    round_trip = client.post("/api/v1/imports/file", files={"file": ("bundle.zip", io.BytesIO(bundle_response.content), "application/zip")}).json()["data"]
    assert round_trip["provider"] == "codex"


def test_missing_codex_home(tmp_path: Path):
    os.environ["CODEX_REPLAY_CODEX_HOME"] = str(tmp_path / "missing-home")
    os.environ["CODEX_REPLAY_DATA_DIR"] = str(tmp_path / "data")
    api_root = Path(__file__).resolve().parents[1]
    if str(api_root) not in sys.path:
        sys.path.insert(0, str(api_root))
    for module_name in ["app.main", "app.codex", "app.config", "app.database"]:
        sys.modules.pop(module_name, None)
    app_main = importlib.import_module("app.main")
    discovery_service = app_main.discovery_service
    database = app_main.database

    database.initialize()
    result = discovery_service.scan()
    assert "warning" in result


def test_repeated_scan_preserves_ready_trace_status(tmp_path: Path):
    client = setup_app(tmp_path)

    first_trace = next(
        item for item in client.get("/api/v1/discovery/sources").json()["data"]["items"]
        if item["source_kind"] == "codex_trace"
    )
    assert first_trace["status"] == "ready"

    client.post("/api/v1/discovery/scan")

    second_trace = next(
        item for item in client.get("/api/v1/discovery/sources").json()["data"]["items"]
        if item["source_kind"] == "codex_trace"
    )
    assert second_trace["status"] == "ready"
    assert second_trace["run_id"] == first_trace["run_id"]


def test_manual_import_supports_custom_tool_calls_with_list_input(tmp_path: Path):
    client = setup_app(tmp_path)
    upload = io.BytesIO("\n".join(json.dumps(item) for item in custom_tool_list_entries()).encode("utf-8"))

    imported = client.post(
        "/api/v1/imports/file",
        files={"file": ("listy.jsonl", upload, "application/jsonl")},
    ).json()["data"]

    tool_events = client.get(f"/api/v1/runs/{imported['run_id']}/events", params={"types": "tool_call"}).json()["data"]["items"]
    assert len(tool_events) == 1

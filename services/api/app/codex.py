from __future__ import annotations

import hashlib
import io
import json
import os
import re
import shutil
import zipfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from uuid import uuid4

from .config import Settings
from .database import Database, json_dump, row_to_dict, rows_to_dicts
from .models import ParsedEvent, ParsedRun, ParsedSkill


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def make_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:12]}"


def parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def duration_ms(started_at: Optional[str], ended_at: Optional[str]) -> Optional[int]:
    start = parse_iso(started_at)
    end = parse_iso(ended_at)
    if not start or not end:
        return None
    return max(int((end - start).total_seconds() * 1000), 0)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def title_from_text(value: str, fallback: str) -> str:
    clean = " ".join(value.strip().split())
    if not clean:
        return fallback
    if len(clean) > 88:
        return clean[:85].rstrip() + "..."
    return clean


def is_test_command(command_text: str) -> bool:
    lowered = command_text.lower()
    needles = [
        "pytest",
        "pnpm test",
        "npm test",
        "vitest",
        "jest",
        "playwright test",
        "go test",
        "cargo test",
        "ruff check",
        "eslint",
        "mypy",
        "tsc ",
    ]
    return any(needle in lowered for needle in needles)


def detect_framework(command_text: str) -> str:
    lowered = command_text.lower()
    if "pytest" in lowered:
        return "pytest"
    if "vitest" in lowered:
        return "vitest"
    if "jest" in lowered:
        return "jest"
    if "playwright" in lowered:
        return "playwright"
    if "go test" in lowered:
        return "go-test"
    if "cargo test" in lowered:
        return "cargo-test"
    return "unknown"


def stringify_output(raw_output: Any) -> str:
    if isinstance(raw_output, str):
        return raw_output
    if raw_output is None:
        return ""
    if isinstance(raw_output, (list, dict)):
        try:
            return json.dumps(raw_output, ensure_ascii=True)
        except TypeError:
            return str(raw_output)
    return str(raw_output)


def preview_text(value: Any, limit: int = 400) -> str:
    text = stringify_output(value).strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def parse_exec_output(raw_output: Any) -> Dict[str, Any]:
    text_output = stringify_output(raw_output)
    exit_code = None
    duration = None
    stdout = text_output
    stderr = ""
    exit_match = re.search(r"Process exited with code (-?\d+)", text_output)
    if exit_match:
        exit_code = int(exit_match.group(1))
    duration_match = re.search(r"Wall time:\s*([0-9.]+)\s*seconds?", text_output)
    if duration_match:
        duration = int(float(duration_match.group(1)) * 1000)
    if "\nOutput:\n" in text_output:
        stdout = text_output.split("\nOutput:\n", 1)[1]
    return {
        "exit_code": exit_code,
        "duration_ms": duration,
        "stdout": stdout,
        "stderr": stderr,
    }


def normalize_path(path_value: Optional[str]) -> str:
    if not path_value:
        return ""
    return path_value.replace("\\", "/")


BOOKKEEPING_PROVIDER_TYPES = {
    "event_msg:token_count",
    "response_item:reasoning",
    "response_item:token_count",
}

SUMMARY_NOISE_PATTERNS = [
    "AGENTS.md instructions",
    "<INSTRUCTIONS>",
    "</INSTRUCTIONS>",
    "<environment_context>",
    "</environment_context>",
    "## Skills",
    "### Available skills",
    "### How to use skills",
    "JavaScript REPL (Node)",
    "Top-level bindings persist across cells",
    "If a cell throws",
    "codex.emitImage",
    "import.meta.resolve",
    "view_image",
    "js_repl",
    "current_date",
    "timezone",
    "request_user_input",
]

SUMMARY_NOISE_WORDS = {
    "skill",
    "skills",
    "instruction",
    "instructions",
    "environment",
    "timezone",
    "current_date",
    "cwd",
    "shell",
    "path",
    "policy",
    "license",
    "metadata",
    "binding",
    "bindings",
    "kernel",
}

SKILL_BLOCKLIST = {
    "codex_home",
    "skillname",
    "request_user_input",
    "current_date",
    "timezone",
}

MODE_RANK = {
    "explicit": 0,
    "declared": 1,
    "implicit": 2,
    "inferred": 3,
}


def is_parser_diagnostic_provider_type(provider_event_type: Optional[str]) -> bool:
    return (provider_event_type or "") in BOOKKEEPING_PROVIDER_TYPES


def clean_summary_text(value: str) -> str:
    return (
        value.replace("\r", "\n")
        .replace("`", "")
        .replace("$", " ")
        .replace("\t", " ")
        .replace("/Users/", " /Users/")
        .strip()
    )


def score_summary_candidate(line: str) -> int:
    lowered = line.lower()
    score = 0
    if re.search(r"\b(implement|improve|fix|review|add|build|design|audit|continue|rewrite|reset)\b", lowered):
        score += 40
    if re.search(r"\b(replay|trace|run|ux|ui|catalog|inspector|landing|home|event|ledger)\b", lowered):
        score += 22
    if 16 <= len(line) <= 140:
        score += 18
    if 4 <= len(line.split()) <= 18:
        score += 12
    if line.startswith("PLEASE IMPLEMENT THIS PLAN"):
        score -= 10
    if line.startswith(("-", "*", "#")):
        score -= 6
    if re.match(r"^\d+[.)]\s+", line):
        score -= 8
    if any(pattern.lower() in lowered for pattern in SUMMARY_NOISE_PATTERNS):
        score -= 50
    if any(word in lowered for word in SUMMARY_NOISE_WORDS):
        score -= 18
    if "/Users/" in line or "SKILL.md" in line or "(file:" in lowered:
        score -= 28
    if "<" in line and ">" in line:
        score -= 18
    return score


def derive_task_summary(value: Optional[str], fallback: str) -> str:
    raw = clean_summary_text(value or "")
    candidates = []
    for line in raw.splitlines():
        stripped = " ".join(line.strip().split())
        if not stripped:
            continue
        stripped = re.sub(r"\[\s*\$?([A-Za-z0-9_.\-]+)\s*\]\([^)]+SKILL\.md\)", " ", stripped, flags=re.IGNORECASE)
        stripped = re.sub(r"^\[(.+?)\]\([^)]+\)$", r"\1", stripped)
        stripped = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", stripped)
        stripped = re.sub(r"\$[A-Za-z0-9_.\-]+", " ", stripped)
        stripped = re.sub(r"</?[^>]+>", " ", stripped)
        stripped = " ".join(stripped.split())
        if not stripped:
            continue
        candidates.append((score_summary_candidate(stripped), stripped))
    if candidates:
        best_score, best_line = max(candidates, key=lambda item: item[0])
        if best_score > 8:
            best_line = re.sub(r"^PLEASE IMPLEMENT THIS PLAN:\s*", "", best_line, flags=re.IGNORECASE).strip()
            if len(best_line) > 96:
                best_line = best_line[:95].rstrip() + "…"
            return best_line
    return fallback


def normalize_skill_name(name: str) -> Optional[str]:
    cleaned = name.strip()
    if not cleaned or cleaned.isdigit():
        return None
    lowered = cleaned.lower()
    if lowered in SKILL_BLOCKLIST:
        return None
    if cleaned.upper() == cleaned and any(char.isalpha() for char in cleaned):
        return None
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_.\-]{0,63}", cleaned):
        return None
    return lowered


def collapse_skills(skills: List[ParsedSkill]) -> List[ParsedSkill]:
    merged: Dict[str, ParsedSkill] = {}
    for skill in skills:
        normalized_name = normalize_skill_name(skill.name)
        if not normalized_name:
            continue
        skill.name = normalized_name
        skill.event_ids = list(dict.fromkeys([skill.event_id, *skill.event_ids]))
        existing = merged.get(normalized_name)
        if not existing:
            merged[normalized_name] = skill
            continue
        existing.event_ids = list(dict.fromkeys([*existing.event_ids, *skill.event_ids]))
        if skill.confidence > existing.confidence:
            existing.confidence = skill.confidence
        if MODE_RANK.get(skill.mode, 99) < MODE_RANK.get(existing.mode, 99):
            existing.mode = skill.mode
            existing.evidence_source = skill.evidence_source
            existing.event_id = skill.event_id
    return list(merged.values())


def extract_skill_names(text: str) -> List[str]:
    candidates = re.findall(r"\[\$([A-Za-z0-9_.\-]+)\]\(", text)
    candidates.extend(re.findall(r"\$([A-Za-z0-9_.\-]+)", text))
    names = []
    for candidate in candidates:
        normalized = normalize_skill_name(candidate)
        if normalized:
            names.append(normalized)
    return sorted(set(names))


def infer_skills(prompt: str) -> List[Tuple[str, float]]:
    lowered = prompt.lower()
    inferred: List[Tuple[str, float]] = []
    if any(word in lowered for word in ["ui", "ux", "design", "layout"]):
        inferred.append(("frontend-design", 0.68))
    if any(word in lowered for word in ["review", "audit", "inspect"]):
        inferred.append(("code-review", 0.54))
    if any(word in lowered for word in ["api", "backend"]):
        inferred.append(("backend-patterns", 0.52))
    if any(word in lowered for word in ["test", "pytest", "failing"]):
        inferred.append(("testing", 0.5))
    return inferred


def run_state_label(run_status: str, is_partial: bool) -> str:
    if run_status == "failed":
        return "Unresolved replay"
    if run_status == "completed" and not is_partial:
        return "Ready replay"
    if is_partial or run_status == "unknown":
        return "Partial replay"
    return "Ready replay"


def is_actionable_error_event(event: ParsedEvent) -> bool:
    if not (event.status == "error" or event.error):
        return False
    if is_parser_diagnostic_provider_type(event.provider_event_type):
        return False
    if event.error and event.error.get("error_code") == "unsupported_event":
        return False
    return True


def parse_test_counts(output: str) -> Tuple[Optional[int], Optional[int], Optional[int], str]:
    passed = failed = skipped = None
    result = "unknown"
    summary_match = re.search(
        r"(?:(\d+)\s+failed)?[, ]*(?:(\d+)\s+passed)?[, ]*(?:(\d+)\s+skipped)?",
        output.lower(),
    )
    if summary_match and any(summary_match.groups()):
        failed = int(summary_match.group(1)) if summary_match.group(1) else 0
        passed = int(summary_match.group(2)) if summary_match.group(2) else 0
        skipped = int(summary_match.group(3)) if summary_match.group(3) else 0
        if failed:
            result = "failed"
        elif passed:
            result = "passed"
    elif "error" in output.lower():
        result = "failed"
    return passed, failed, skipped, result


def parse_apply_patch(input_text: str) -> List[Dict[str, Any]]:
    files: List[Dict[str, Any]] = []
    current: Optional[Dict[str, Any]] = None
    for line in input_text.splitlines():
        if line.startswith("*** Add File: "):
            if current:
                files.append(current)
            path = line.split(": ", 1)[1]
            current = {"file_path": path, "change_type": "create", "diff_lines": [], "lines_added": 0, "lines_removed": 0, "hunks": 0}
        elif line.startswith("*** Update File: "):
            if current:
                files.append(current)
            path = line.split(": ", 1)[1]
            current = {"file_path": path, "change_type": "modify", "diff_lines": [], "lines_added": 0, "lines_removed": 0, "hunks": 0}
        elif line.startswith("*** Delete File: "):
            if current:
                files.append(current)
            path = line.split(": ", 1)[1]
            current = {"file_path": path, "change_type": "delete", "diff_lines": [], "lines_added": 0, "lines_removed": 0, "hunks": 0}
        elif line.startswith("*** Move to: "):
            if current:
                current["change_type"] = "rename"
                current["diff_lines"].append(line)
        elif current is not None:
            current["diff_lines"].append(line)
            if line.startswith("@@"):
                current["hunks"] += 1
            elif line.startswith("+") and not line.startswith("+++"):
                current["lines_added"] += 1
            elif line.startswith("-") and not line.startswith("---"):
                current["lines_removed"] += 1
    if current:
        files.append(current)
    for item in files:
        item["diff_text"] = "\n".join(item.pop("diff_lines")).strip()
        item["normalized_path"] = normalize_path(item["file_path"])
        item["hunks_count"] = item.pop("hunks")
    return files


class CodexParser:
    def __init__(self, settings: Settings):
        self.settings = settings

    def parse_file(self, path: Path, source_name: Optional[str] = None) -> ParsedRun:
        return self.parse_bytes(path.read_bytes(), source_name=source_name or path.name, source_path=str(path))

    def parse_bytes(self, raw_bytes: bytes, source_name: str, source_path: Optional[str] = None) -> ParsedRun:
        if zipfile.is_zipfile(io.BytesIO(raw_bytes)):
            return self.parse_export_bundle(raw_bytes, source_name, source_path)
        return self.parse_codex_jsonl(raw_bytes, source_name, source_path)

    def parse_export_bundle(self, raw_bytes: bytes, source_name: str, source_path: Optional[str]) -> ParsedRun:
        with zipfile.ZipFile(io.BytesIO(raw_bytes)) as bundle:
            run_json = json.loads(bundle.read("run.json").decode("utf-8"))
            events = []
            for line in bundle.read("events.jsonl").decode("utf-8").splitlines():
                if line.strip():
                    event_data = json.loads(line)
                    events.append(
                        ParsedEvent(
                            id=event_data["id"],
                            event_type=event_data["event_type"],
                            title=event_data.get("title") or event_data["event_type"].replace("_", " ").title(),
                            timestamp=event_data.get("timestamp"),
                            status=event_data.get("status") or "unknown",
                            provider_event_type=event_data.get("provider_event_type"),
                            message_text=event_data.get("message_text"),
                            raw_payload=event_data.get("raw_payload") or {},
                            step_group=event_data.get("step_group"),
                            step_label=event_data.get("step_label"),
                            command=event_data.get("detail", {}).get("command"),
                            test=event_data.get("detail", {}).get("test"),
                            diff=event_data.get("detail", {}).get("diff"),
                            error=event_data.get("detail", {}).get("error"),
                            tool=event_data.get("detail", {}).get("tool"),
                        )
                    )
            skills = []
            if "skills.json" in bundle.namelist():
                for item in json.loads(bundle.read("skills.json").decode("utf-8")):
                    skills.append(
                        ParsedSkill(
                            name=item["name"],
                            mode=item["mode"],
                            confidence=float(item["confidence"]),
                            event_id=item["event_id"],
                            event_ids=item["event_ids"],
                            evidence_source=item.get("evidence_source", "bundle"),
                        )
                    )
            return ParsedRun(
                provider=run_json.get("provider", "codex"),
                provider_run_id=run_json.get("provider_run_id"),
                session_id=run_json.get("session_id"),
                repo_name=run_json.get("repo_name"),
                repo_root=run_json.get("repo_root"),
                source_name=source_name,
                source_path=source_path,
                prompt=run_json.get("prompt"),
                run_status=run_json.get("run_status", "completed"),
                is_partial=bool(run_json.get("is_partial")),
                parse_status="success",
                provider_version=run_json.get("provider_version"),
                started_at=run_json.get("started_at"),
                ended_at=run_json.get("ended_at"),
                duration_ms=run_json.get("duration_ms"),
                warnings=[],
                events=events,
                skills=skills,
            )

    def parse_codex_jsonl(self, raw_bytes: bytes, source_name: str, source_path: Optional[str]) -> ParsedRun:
        warnings: List[str] = []
        events: List[ParsedEvent] = []
        skills: Dict[Tuple[str, str], ParsedSkill] = {}
        pending_calls: Dict[str, ParsedEvent] = {}
        first_user_prompt: Optional[str] = None
        session_meta: Dict[str, Any] = {}
        first_timestamp: Optional[str] = None
        last_timestamp: Optional[str] = None
        provider_version: Optional[str] = None
        run_status = "unknown"
        is_partial = True
        seq = 0

        for line_number, raw_line in enumerate(raw_bytes.decode("utf-8", errors="replace").splitlines(), start=1):
            if not raw_line.strip():
                continue
            try:
                entry = json.loads(raw_line)
            except json.JSONDecodeError:
                warnings.append(f"Line {line_number} could not be parsed as JSON.")
                continue
            timestamp = entry.get("timestamp")
            first_timestamp = first_timestamp or timestamp
            last_timestamp = timestamp or last_timestamp
            outer_type = entry.get("type")
            payload = entry.get("payload") or {}
            inner_type = payload.get("type") if isinstance(payload, dict) else None

            if outer_type == "session_meta":
                session_meta = payload
                provider_version = payload.get("cli_version")
                continue

            if outer_type == "event_msg" and inner_type == "task_complete":
                run_status = "completed"
                is_partial = False
                continue

            if outer_type == "event_msg" and inner_type == "token_count":
                continue

            if outer_type == "event_msg" and inner_type == "task_started":
                seq += 1
                events.append(
                    ParsedEvent(
                        id=make_id("evt"),
                        event_type="run_started",
                        title="Task started",
                        timestamp=timestamp,
                        status="ok",
                        provider_event_type=inner_type,
                        message_text=None,
                        raw_payload=entry,
                    )
                )
                continue

            if outer_type == "event_msg" and inner_type in {"user_message", "agent_message"}:
                message = payload.get("message") or ""
                if inner_type == "user_message" and first_user_prompt is None and message.strip():
                    first_user_prompt = message.strip()
                seq += 1
                event = ParsedEvent(
                    id=make_id("evt"),
                    event_type="message" if inner_type == "user_message" else "summary",
                    title=title_from_text(message, "User message" if inner_type == "user_message" else "Agent update"),
                    timestamp=timestamp,
                    status="ok",
                    provider_event_type=inner_type,
                    message_text=message,
                    raw_payload=entry,
                )
                events.append(event)
                for skill_name in extract_skill_names(message):
                    skills[(event.id, skill_name)] = ParsedSkill(
                        name=skill_name,
                        mode="explicit",
                        confidence=1.0,
                        event_id=event.id,
                        event_ids=[event.id],
                        evidence_source="message_text",
                    )
                continue

            if outer_type == "response_item" and inner_type == "function_call":
                function_name = payload.get("name") or "tool"
                args_text = payload.get("arguments") or "{}"
                try:
                    arguments = json.loads(args_text)
                except json.JSONDecodeError:
                    arguments = {"raw": args_text}
                seq += 1
                if function_name == "exec_command":
                    command_text = arguments.get("cmd") or ""
                    event_type = "test" if is_test_command(command_text) else "command"
                    event = ParsedEvent(
                        id=make_id("evt"),
                        event_type=event_type,
                        title=title_from_text(command_text, "Run command"),
                        timestamp=timestamp,
                        status="unknown",
                        provider_event_type=function_name,
                        message_text=command_text,
                        raw_payload={"entry": entry, "arguments": arguments},
                        command={
                            "command_text": command_text,
                            "cwd": arguments.get("workdir"),
                            "shell": arguments.get("shell"),
                            "exit_code": None,
                            "duration_ms": None,
                            "stdout_preview": "",
                            "stderr_preview": "",
                            "retry_group_id": arguments.get("prefix_rule", [command_text[:32]])[0] if isinstance(arguments.get("prefix_rule"), list) else None,
                            "retry_index": 0,
                            "is_validation": 1 if event_type == "test" else 0,
                        } if event_type == "command" else None,
                        test={
                            "command_text": command_text,
                            "framework": detect_framework(command_text),
                            "result": "unknown",
                            "passed_count": None,
                            "failed_count": None,
                            "skipped_count": None,
                            "duration_ms": None,
                            "stdout_preview": "",
                            "stderr_preview": "",
                        } if event_type == "test" else None,
                    )
                else:
                    event = ParsedEvent(
                        id=make_id("evt"),
                        event_type="tool_call",
                        title=title_from_text(function_name, "Tool call"),
                        timestamp=timestamp,
                        status="unknown",
                        provider_event_type=function_name,
                        message_text=None,
                        raw_payload={"entry": entry, "arguments": arguments},
                        tool={
                            "tool_type": "shell" if function_name in {"exec_command", "write_stdin"} else "internal",
                            "tool_name": function_name,
                            "input_preview": preview_text(arguments, 400),
                            "output_preview": "",
                            "duration_ms": None,
                        },
                    )
                pending_calls[payload.get("call_id", event.id)] = event
                events.append(event)
                continue

            if outer_type == "response_item" and inner_type == "function_call_output":
                call_id = payload.get("call_id")
                if call_id in pending_calls:
                    parsed_output = parse_exec_output(payload.get("output") or "")
                    event = pending_calls[call_id]
                    if event.command:
                        event.status = "ok" if parsed_output["exit_code"] in (0, None) else "error"
                        event.command["exit_code"] = parsed_output["exit_code"]
                        event.command["duration_ms"] = parsed_output["duration_ms"]
                        event.command["stdout_preview"] = parsed_output["stdout"][:400]
                        event.command["stderr_preview"] = parsed_output["stderr"][:400]
                    elif event.test:
                        passed, failed, skipped, result = parse_test_counts(parsed_output["stdout"])
                        event.status = "ok" if result == "passed" else "error" if result == "failed" else "warning"
                        event.test["result"] = result
                        event.test["passed_count"] = passed
                        event.test["failed_count"] = failed
                        event.test["skipped_count"] = skipped
                        event.test["duration_ms"] = parsed_output["duration_ms"]
                        event.test["stdout_preview"] = parsed_output["stdout"][:400]
                        event.test["stderr_preview"] = parsed_output["stderr"][:400]
                    elif event.tool:
                        event.status = "ok"
                        event.tool["output_preview"] = preview_text(payload.get("output"), 400)
                    pending_calls.pop(call_id, None)
                continue

            if outer_type == "response_item" and inner_type == "custom_tool_call":
                tool_name = payload.get("name") or "custom_tool"
                if tool_name == "apply_patch":
                    patch_input = payload.get("input") or ""
                    parsed_files = parse_apply_patch(patch_input)
                    if not parsed_files:
                        seq += 1
                        events.append(
                            ParsedEvent(
                                id=make_id("evt"),
                                event_type="diff",
                                title="Apply patch",
                                timestamp=timestamp,
                                status="ok",
                                provider_event_type=tool_name,
                                message_text=None,
                                raw_payload=entry,
                                diff={
                                    "file_path": "(unknown)",
                                    "normalized_path": "",
                                    "change_type": "modify",
                                    "lines_added": 0,
                                    "lines_removed": 0,
                                    "hunks_count": 0,
                                    "diff_text": patch_input,
                                },
                            )
                        )
                    for file_patch in parsed_files:
                        seq += 1
                        events.append(
                            ParsedEvent(
                                id=make_id("evt"),
                                event_type="diff",
                                title=title_from_text(file_patch["file_path"], "Patched file"),
                                timestamp=timestamp,
                                status="ok",
                                provider_event_type=tool_name,
                                message_text=None,
                                raw_payload={"entry": entry, "patch": file_patch},
                                diff=file_patch,
                            )
                        )
                else:
                    seq += 1
                    events.append(
                        ParsedEvent(
                            id=make_id("evt"),
                            event_type="tool_call",
                            title=title_from_text(tool_name, "Custom tool"),
                            timestamp=timestamp,
                            status="ok",
                            provider_event_type=tool_name,
                            message_text=None,
                            raw_payload=entry,
                            tool={
                                "tool_type": "internal",
                                "tool_name": tool_name,
                                "input_preview": preview_text(payload.get("input"), 400),
                                "output_preview": "",
                                "duration_ms": None,
                            },
                        )
                    )
                continue

            if outer_type == "response_item" and inner_type == "custom_tool_call_output":
                continue

            if outer_type == "response_item" and inner_type == "message":
                role = payload.get("role")
                content = payload.get("content") or []
                text_parts = [part.get("text", "") for part in content if isinstance(part, dict) and part.get("type") == "input_text"]
                text = "\n".join(part for part in text_parts if part).strip()
                if role == "user" and first_user_prompt is None and text:
                    first_user_prompt = text
                if text:
                    seq += 1
                    event = ParsedEvent(
                        id=make_id("evt"),
                        event_type="message",
                        title=title_from_text(text, f"{role or 'message'} entry"),
                        timestamp=timestamp,
                        status="ok",
                        provider_event_type=f"{outer_type}:{role}",
                        message_text=text,
                        raw_payload=entry,
                    )
                    events.append(event)
                    for skill_name in extract_skill_names(text):
                        skills[(event.id, skill_name)] = ParsedSkill(
                            name=skill_name,
                            mode="explicit",
                            confidence=1.0,
                            event_id=event.id,
                            event_ids=[event.id],
                            evidence_source="message_text",
                        )
                continue

            if outer_type == "response_item" and inner_type in {"reasoning", "token_count"}:
                continue

            if outer_type == "turn_context":
                continue

            seq += 1
            events.append(
                ParsedEvent(
                    id=make_id("evt"),
                    event_type="warning",
                    title=title_from_text(str(inner_type or outer_type), "Unknown event"),
                    timestamp=timestamp,
                    status="warning",
                    provider_event_type=f"{outer_type}:{inner_type}",
                    message_text=None,
                    raw_payload=entry,
                    error={
                        "error_code": "unsupported_event",
                        "error_type": "warning",
                        "severity": "low",
                        "message": f"Unsupported event shape preserved: {outer_type}/{inner_type}",
                        "related_seq": None,
                        "is_terminal": 0,
                    },
                )
            )

        prompt_text = first_user_prompt or source_name
        for inferred_name, confidence in infer_skills(prompt_text or ""):
            if any(existing.name == inferred_name for existing in skills.values()):
                continue
            anchor_event_id = events[0].id if events else make_id("evt")
            skills[(anchor_event_id, inferred_name)] = ParsedSkill(
                name=inferred_name,
                mode="inferred",
                confidence=confidence,
                event_id=anchor_event_id,
                event_ids=[anchor_event_id],
                evidence_source="prompt_heuristic",
            )

        repo_root = session_meta.get("cwd")
        return ParsedRun(
            provider="codex",
            provider_run_id=session_meta.get("id"),
            session_id=session_meta.get("id"),
            repo_name=Path(repo_root).name if repo_root else None,
            repo_root=repo_root,
            source_name=source_name,
            source_path=source_path,
            prompt=prompt_text,
            run_status=run_status,
            is_partial=is_partial if run_status != "completed" else False,
            parse_status="partial" if warnings else "success",
            provider_version=provider_version,
            started_at=session_meta.get("timestamp") or first_timestamp,
            ended_at=None if is_partial and run_status != "completed" else last_timestamp,
            duration_ms=duration_ms(session_meta.get("timestamp") or first_timestamp, last_timestamp if run_status == "completed" else None),
            warnings=warnings,
            events=events,
            skills=collapse_skills(list(skills.values())),
        )


class InsightEngine:
    def build(self, parsed: ParsedRun) -> Tuple[List[Dict[str, Any]], str, Dict[str, Any]]:
        insights: List[Dict[str, Any]] = []
        changed_files = [event.diff["normalized_path"] for event in parsed.events if event.diff]
        unique_files = sorted({path for path in changed_files if path})
        tests = [event for event in parsed.events if event.test]
        commands = [event for event in parsed.events if event.command]
        errors = [event for event in parsed.events if is_actionable_error_event(event)]
        review_attention = "low"

        retry_groups = Counter(
            event.command.get("retry_group_id")
            for event in commands
            if event.command and event.command.get("retry_group_id")
        )
        if any(count >= 2 for count in retry_groups.values()):
            insights.append(self._insight(parsed, "retry_loops", "medium", "Retry loop detected", "Repeated command patterns suggest the agent retried a failing step more than once.", "Inspect the first failing command and compare subsequent edits before accepting the run.", [event.id for event in commands[:3]]))
        if len(unique_files) >= 4 and len(tests) <= 1:
            insights.append(self._insight(parsed, "wide_change_weak_validation", "high", "Wide changes with narrow validation", f"{len(unique_files)} files changed while only {len(tests)} validation command ran.", "Run broader validation before trusting this patch.", [event.id for event in parsed.events if event.diff or event.test]))
        if len(commands) >= 5 and len(unique_files) <= 1:
            insights.append(self._insight(parsed, "slow_localization", "medium", "Slow problem localization", "Several shell commands were used before the change narrowed to a small file set.", "Review the run for exploratory churn and ensure the final fix is actually scoped.", [event.id for event in commands[:5]]))
        repeated_diffs = Counter(event.diff["normalized_path"] for event in parsed.events if event.diff)
        churn_paths = [path for path, count in repeated_diffs.items() if count >= 2]
        if churn_paths:
            insights.append(self._insight(parsed, "patch_churn", "medium", "Patch churn on the same files", "One or more files were patched repeatedly during the run.", "Check the final diff carefully; repeated edits often hide partial fixes or reversions.", [event.id for event in parsed.events if event.diff and event.diff["normalized_path"] in churn_paths]))
        if any(path.startswith("backend/") or path.startswith("src/") for path in unique_files) and not tests:
            insights.append(self._insight(parsed, "core_path_without_tests", "high", "Core paths changed without tests", "The run changed implementation files without any recorded validation step.", "Run the relevant test or build targets before merging.", [event.id for event in parsed.events if event.diff]))
        if parsed.run_status != "completed" and errors:
            insights.append(self._insight(parsed, "unresolved_final_error", "high", "Run appears unresolved", "The source trace ended without a completed state and contains error evidence.", "Treat this replay as incomplete until the source session is finished or rerun.", [errors[-1].id]))
        elif errors and tests and any(test.test and test.test.get("result") == "passed" for test in tests):
            insights.append(self._insight(parsed, "failure_recovered", "info", "Failure recovered later in the run", "The run recorded errors before a later validation pass.", "Review the failing and passing validation steps together to confirm the recovery is real.", [errors[0].id, tests[-1].id]))

        if any(insight["severity"] == "high" for insight in insights):
            review_attention = "high"
        elif insights:
            review_attention = "medium"

        summary = self._summary(parsed, unique_files, tests, errors, review_attention)
        return insights, review_attention, summary

    def _insight(self, parsed: ParsedRun, code: str, severity: str, title: str, message: str, recommendation: str, event_ids: List[str]) -> Dict[str, Any]:
        return {
            "id": make_id("ins"),
            "run_id": "",
            "code": code,
            "severity": severity,
            "title": title,
            "message": message,
            "recommendation": recommendation,
            "event_ids_json": json_dump(event_ids),
        }

    def _summary(self, parsed: ParsedRun, files: List[str], tests: List[ParsedEvent], errors: List[ParsedEvent], review_attention: str) -> Dict[str, Any]:
        task_summary = derive_task_summary(parsed.prompt or parsed.source_name, parsed.source_name or "Imported run")
        validation_bits = []
        for test in tests[:3]:
            result = test.test.get("result") if test.test else "unknown"
            validation_bits.append(f"`{test.test.get('command_text')}`: {result}")
        validation_summary = ", ".join(validation_bits) if validation_bits else "No validation commands were recorded."
        changed_summary = ", ".join(files[:5]) if files else "No file diffs were captured."
        has_recovery = any(test.test and test.test.get("result") == "passed" for test in tests)
        if errors and has_recovery:
            failure_summary = "Recovered after earlier failures."
        elif errors and parsed.run_status != "completed":
            failure_summary = "Run ended before recovery was shown."
        elif errors:
            failure_summary = "Failures were recorded without a confirmed recovery."
        else:
            failure_summary = "No actionable failures were recorded."
        if errors:
            reviewer_notes = "Start with the first failing step, compare the last patch, and verify the final validation coverage."
        elif len(files) >= 4 and not tests:
            reviewer_notes = "Review the blast radius carefully because the run changed several files without recorded validation."
        else:
            reviewer_notes = "Review the main command sequence, the final patch, and the last recorded validation step."
        markdown = "\n".join(
            [
                "## Run overview",
                f"- Task: {task_summary}",
                f"- State: {run_state_label(parsed.run_status, parsed.is_partial)}",
                f"- Review priority: {review_attention}",
                f"- Files changed: {len(files)}",
                f"- Validation: {validation_summary}",
                f"- Recovery: {failure_summary}",
                "",
                "## What to review",
                f"- {reviewer_notes}",
            ]
        )
        return {
            "markdown": markdown,
            "json": {
                "task_summary": task_summary,
                "validation_summary": validation_summary,
                "changed_files_summary": changed_summary,
                "failure_summary": failure_summary,
                "reviewer_notes": reviewer_notes,
            },
        }


class ReplayService:
    def __init__(self, settings: Settings, database: Database):
        self.settings = settings
        self.database = database
        self.parser = CodexParser(settings)
        self.insights = InsightEngine()

    def ensure_directories(self) -> None:
        for path in [self.settings.data_dir, self.settings.raw_dir, self.settings.blob_dir, self.settings.export_dir]:
            path.mkdir(parents=True, exist_ok=True)

    def import_path(self, path: Path, source_name: Optional[str] = None, source_id: Optional[str] = None, import_mode: str = "manual") -> Dict[str, Any]:
        parsed = self.parser.parse_file(path, source_name=source_name)
        return self._persist_import(parsed, source_bytes=path.read_bytes(), original_name=source_name or path.name, source_path=str(path), source_id=source_id, import_mode=import_mode)

    def import_bytes(self, raw_bytes: bytes, original_name: str, source_name: Optional[str] = None, source_path: Optional[str] = None, source_id: Optional[str] = None, import_mode: str = "manual") -> Dict[str, Any]:
        parsed = self.parser.parse_bytes(raw_bytes, source_name=source_name or original_name, source_path=source_path)
        return self._persist_import(parsed, source_bytes=raw_bytes, original_name=original_name, source_path=source_path, source_id=source_id, import_mode=import_mode)

    def _persist_import(self, parsed: ParsedRun, source_bytes: bytes, original_name: str, source_path: Optional[str], source_id: Optional[str], import_mode: str) -> Dict[str, Any]:
        self.ensure_directories()
        existing_source = row_to_dict(self.database.fetchone("SELECT run_id FROM discovered_sources WHERE id = ?", (source_id,))) if source_id else None
        run_id = existing_source["run_id"] if existing_source and existing_source.get("run_id") else make_id("run")
        self._refresh_event_ids(parsed)
        raw_artifact_id = make_id("art")
        stored_name = f"{run_id}-{Path(original_name).name}"
        stored_path = self.settings.raw_dir / stored_name
        stored_path.write_bytes(source_bytes)

        insights, review_attention, summary = self.insights.build(parsed)

        run_row = {
            "id": run_id,
            "source_id": source_id,
            "provider": parsed.provider,
            "provider_run_id": parsed.provider_run_id,
            "session_id": parsed.session_id,
            "repo_name": parsed.repo_name,
            "repo_root": parsed.repo_root,
            "source_name": parsed.source_name,
            "source_path": source_path,
            "import_mode": import_mode,
            "run_status": parsed.run_status,
            "is_partial": 1 if parsed.is_partial else 0,
            "parse_status": parsed.parse_status,
            "provider_version": parsed.provider_version,
            "normalization_version": "v0.1",
            "adapter_version": "codex-jsonl-v1",
            "prompt": parsed.prompt,
            "review_attention": review_attention,
            "status_reason": "Source session is still active." if parsed.is_partial else None,
            "summary_markdown": summary["markdown"],
            "summary_json": json_dump(summary["json"]),
            "validation_summary": summary["json"]["validation_summary"],
            "changed_files_summary": summary["json"]["changed_files_summary"],
            "failure_summary": summary["json"]["failure_summary"],
            "reviewer_notes": summary["json"]["reviewer_notes"],
            "warnings_json": json_dump(parsed.warnings),
            "started_at": parsed.started_at,
            "ended_at": parsed.ended_at,
            "duration_ms": parsed.duration_ms,
            "total_events": len(parsed.events),
            "total_commands": sum(1 for event in parsed.events if event.command),
            "total_tests": sum(1 for event in parsed.events if event.test),
            "total_errors": sum(1 for event in parsed.events if event.error or event.status == "error"),
            "total_files_changed": len({event.diff["normalized_path"] for event in parsed.events if event.diff}),
            "first_error_seq": next((index + 1 for index, event in enumerate(parsed.events) if event.status == "error" or event.error), None),
        }

        artifacts = [
            {
                "id": raw_artifact_id,
                "run_id": run_id,
                "artifact_type": "raw_import",
                "filename": Path(original_name).name,
                "mime_type": "application/jsonl" if Path(original_name).suffix == ".jsonl" else "application/octet-stream",
                "sha256": sha256_bytes(source_bytes),
                "size_bytes": len(source_bytes),
                "storage_path": str(stored_path),
            }
        ]
        event_rows: List[Dict[str, Any]] = []
        detail_rows: Dict[str, List[Dict[str, Any]]] = {
            "command_events": [],
            "test_events": [],
            "diff_events": [],
            "error_events": [],
            "tool_events": [],
        }
        for seq, event in enumerate(parsed.events, start=1):
            event_rows.append(
                {
                    "id": event.id,
                    "run_id": run_id,
                    "seq": seq,
                    "parent_seq": event.parent_seq,
                    "provider_event_id": None,
                    "provider_event_type": event.provider_event_type,
                    "event_type": event.event_type,
                    "title": event.title,
                    "status": event.status,
                    "timestamp": event.timestamp,
                    "step_group": event.step_group,
                    "step_label": event.step_label,
                    "message_text": event.message_text,
                    "raw_payload_json": json_dump(event.raw_payload),
                    "artifact_ref_id": raw_artifact_id,
                }
            )
            if event.command:
                command_row = dict(event.command)
                command_row["event_id"] = event.id
                stdout_preview = command_row.get("stdout_preview") or ""
                stderr_preview = command_row.get("stderr_preview") or ""
                if len(stdout_preview) > self.settings.blob_preview_limit:
                    stdout_artifact_id = make_id("art")
                    stdout_path = self.settings.blob_dir / f"{stdout_artifact_id}.stdout.txt"
                    stdout_path.write_text(stdout_preview, encoding="utf-8")
                    artifacts.append({
                        "id": stdout_artifact_id,
                        "run_id": run_id,
                        "artifact_type": "stdout_blob",
                        "filename": stdout_path.name,
                        "mime_type": "text/plain",
                        "sha256": sha256_path(stdout_path),
                        "size_bytes": stdout_path.stat().st_size,
                        "storage_path": str(stdout_path),
                    })
                    command_row["stdout_preview"] = stdout_preview[: self.settings.blob_preview_limit]
                    command_row["stdout_artifact_id"] = stdout_artifact_id
                else:
                    command_row["stdout_artifact_id"] = None
                if len(stderr_preview) > self.settings.blob_preview_limit:
                    stderr_artifact_id = make_id("art")
                    stderr_path = self.settings.blob_dir / f"{stderr_artifact_id}.stderr.txt"
                    stderr_path.write_text(stderr_preview, encoding="utf-8")
                    artifacts.append({
                        "id": stderr_artifact_id,
                        "run_id": run_id,
                        "artifact_type": "stderr_blob",
                        "filename": stderr_path.name,
                        "mime_type": "text/plain",
                        "sha256": sha256_path(stderr_path),
                        "size_bytes": stderr_path.stat().st_size,
                        "storage_path": str(stderr_path),
                    })
                    command_row["stderr_preview"] = stderr_preview[: self.settings.blob_preview_limit]
                    command_row["stderr_artifact_id"] = stderr_artifact_id
                else:
                    command_row["stderr_artifact_id"] = None
                detail_rows["command_events"].append(command_row)
            if event.test:
                test_row = dict(event.test)
                test_row["event_id"] = event.id
                detail_rows["test_events"].append(test_row)
            if event.diff:
                diff_row = dict(event.diff)
                diff_row["event_id"] = event.id
                diff_text = diff_row.get("diff_text") or ""
                if len(diff_text) > self.settings.blob_inline_limit:
                    diff_artifact_id = make_id("art")
                    diff_path = self.settings.blob_dir / f"{diff_artifact_id}.diff.txt"
                    diff_path.write_text(diff_text, encoding="utf-8")
                    artifacts.append({
                        "id": diff_artifact_id,
                        "run_id": run_id,
                        "artifact_type": "other",
                        "filename": diff_path.name,
                        "mime_type": "text/plain",
                        "sha256": sha256_path(diff_path),
                        "size_bytes": diff_path.stat().st_size,
                        "storage_path": str(diff_path),
                    })
                    diff_row["diff_artifact_id"] = diff_artifact_id
                    diff_row["diff_text"] = diff_text[: self.settings.blob_preview_limit]
                else:
                    diff_row["diff_artifact_id"] = None
                detail_rows["diff_events"].append(diff_row)
            if event.error:
                error_row = dict(event.error)
                error_row["event_id"] = event.id
                detail_rows["error_events"].append(error_row)
            if event.tool:
                tool_row = dict(event.tool)
                tool_row["event_id"] = event.id
                detail_rows["tool_events"].append(tool_row)

        skill_rows = [
            {
                "id": make_id("skl"),
                "run_id": run_id,
                "event_id": skill.event_id,
                "name": skill.name,
                "mode": skill.mode,
                "confidence": skill.confidence,
                "event_ids_json": json_dump(skill.event_ids),
                "evidence_source": skill.evidence_source,
            }
            for skill in parsed.skills
        ]
        for insight in insights:
            insight["run_id"] = run_id

        self.database.replace_run(run_row, event_rows, detail_rows, insights, skill_rows, artifacts)
        if source_id:
            source_status = "partial" if parsed.is_partial else "ready" if parsed.parse_status in {"success", "partial"} else "failed"
            self.database.execute(
                """
                UPDATE discovered_sources
                SET run_id = ?, status = ?, warning = ?, updated_at = datetime('now')
                WHERE id = ?
                """,
                (run_id, source_status, "; ".join(parsed.warnings) if parsed.warnings else None, source_id),
            )
        return {
            "run_id": run_id,
            "provider": parsed.provider,
            "parse_status": parsed.parse_status,
            "warnings": parsed.warnings,
            "total_events": len(parsed.events),
        }

    def _refresh_event_ids(self, parsed: ParsedRun) -> None:
        mapping: Dict[str, str] = {}
        for event in parsed.events:
            previous_id = event.id
            event.id = make_id("evt")
            mapping[previous_id] = event.id
        for skill in parsed.skills:
            skill.event_id = mapping.get(skill.event_id, skill.event_id)
            skill.event_ids = [mapping.get(event_id, event_id) for event_id in skill.event_ids]

    def get_clean_skill_signals(self, run_id: str) -> List[Dict[str, Any]]:
        raw_items = rows_to_dicts(
            self.database.fetchall("SELECT * FROM skill_signals WHERE run_id = ?", (run_id,))
        )
        seq_by_event_id = {
            row["id"]: row["seq"]
            for row in rows_to_dicts(
                self.database.fetchall("SELECT id, seq FROM events WHERE run_id = ?", (run_id,))
            )
        }
        grouped: Dict[str, Dict[str, Any]] = {}
        for item in raw_items:
            normalized_name = normalize_skill_name(item["name"])
            if not normalized_name:
                continue
            event_ids = [
                event_id
                for event_id in json.loads(item["event_ids_json"])
                if event_id in seq_by_event_id
            ]
            primary_event_id = item["event_id"] if item["event_id"] in seq_by_event_id else None
            if primary_event_id:
                event_ids.insert(0, primary_event_id)
            ordered_event_ids = list(
                dict.fromkeys(sorted(event_ids, key=lambda event_id: seq_by_event_id[event_id]))
            )
            if not ordered_event_ids:
                continue
            merged = grouped.get(normalized_name)
            first_seq = seq_by_event_id[ordered_event_ids[0]]
            if not merged:
                grouped[normalized_name] = {
                    **item,
                    "name": normalized_name,
                    "event_id": ordered_event_ids[0],
                    "event_ids": ordered_event_ids,
                    "first_seq": first_seq,
                }
                continue
            merged["event_ids"] = list(
                dict.fromkeys(
                    sorted(
                        [*merged["event_ids"], *ordered_event_ids],
                        key=lambda event_id: seq_by_event_id[event_id],
                    )
                )
            )
            merged["event_id"] = merged["event_ids"][0]
            merged["first_seq"] = seq_by_event_id[merged["event_id"]]
            if float(item["confidence"]) > float(merged["confidence"]):
                merged["confidence"] = item["confidence"]
            if MODE_RANK.get(item["mode"], 99) < MODE_RANK.get(merged["mode"], 99):
                merged["mode"] = item["mode"]
                merged["evidence_source"] = item.get("evidence_source")
        return sorted(
            grouped.values(),
            key=lambda item: (
                MODE_RANK.get(item["mode"], 99),
                item["first_seq"],
                item["name"],
            ),
        )

    def get_visible_timeline(self, run_id: str) -> List[Dict[str, Any]]:
        events = rows_to_dicts(
            self.database.fetchall(
                """
                SELECT id, seq, event_type, title, status, provider_event_type
                FROM events
                WHERE run_id = ?
                ORDER BY seq
                """,
                (run_id,),
            )
        )
        diff_ids = {
            row["event_id"]
            for row in rows_to_dicts(
                self.database.fetchall(
                    """
                    SELECT d.event_id
                    FROM diff_events d
                    JOIN events e ON e.id = d.event_id
                    WHERE e.run_id = ?
                    """,
                    (run_id,),
                )
            )
        }
        error_rows = {
            row["event_id"]: row
            for row in rows_to_dicts(
                self.database.fetchall(
                    """
                    SELECT er.*, e.provider_event_type, e.status
                    FROM error_events er
                    JOIN events e ON e.id = er.event_id
                    WHERE e.run_id = ?
                    """,
                    (run_id,),
                )
            )
        }
        skill_event_ids = {
            event_id
            for skill in self.get_clean_skill_signals(run_id)
            for event_id in skill["event_ids"]
        }
        items: List[Dict[str, Any]] = []
        for event in events:
            error = error_rows.get(event["id"])
            is_diagnostic = is_parser_diagnostic_provider_type(
                event.get("provider_event_type")
            ) or (error and error.get("error_code") == "unsupported_event")
            if is_diagnostic:
                continue
            has_error = bool(error) or event.get("status") == "error"
            items.append(
                {
                    "seq": event["seq"],
                    "event_id": event["id"],
                    "event_type": event["event_type"],
                    "label": event["title"],
                    "status": event["status"],
                    "has_diff": event["id"] in diff_ids,
                    "has_error": has_error,
                    "has_skill": event["id"] in skill_event_ids,
                }
            )
        return items

    def get_run_counts(self, run_id: str, run_row: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        events = rows_to_dicts(
            self.database.fetchall("SELECT id FROM events WHERE run_id = ?", (run_id,))
        )
        timeline = self.get_visible_timeline(run_id)
        diff_rows = rows_to_dicts(
            self.database.fetchall(
                """
                SELECT d.normalized_path
                FROM diff_events d
                JOIN events e ON e.id = d.event_id
                WHERE e.run_id = ?
                """,
                (run_id,),
            )
        )
        unique_files = {row["normalized_path"] for row in diff_rows if row["normalized_path"]}
        return {
            "events": len(timeline),
            "commands": sum(1 for item in timeline if item["event_type"] == "command"),
            "tests": sum(1 for item in timeline if item["event_type"] == "test"),
            "errors": sum(1 for item in timeline if item["has_error"]),
            "files_changed": len(unique_files) if unique_files else (run_row or {}).get("total_files_changed", 0),
            "first_error_seq": next(
                (item["seq"] for item in timeline if item["has_error"]),
                None,
            ),
            "diagnostics": max(len(events) - len(timeline), 0),
        }

    def build_summary_payload(
        self, run_id: str, run_row: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        run = run_row or row_to_dict(
            self.database.fetchone("SELECT * FROM runs WHERE id = ?", (run_id,))
        )
        if not run:
            raise FileNotFoundError(run_id)
        tests = rows_to_dicts(
            self.database.fetchall(
                """
                SELECT t.command_text, t.result, e.seq
                FROM test_events t
                JOIN events e ON e.id = t.event_id
                WHERE e.run_id = ?
                ORDER BY e.seq
                """,
                (run_id,),
            )
        )
        diff_rows = rows_to_dicts(
            self.database.fetchall(
                """
                SELECT d.normalized_path
                FROM diff_events d
                JOIN events e ON e.id = d.event_id
                WHERE e.run_id = ?
                ORDER BY e.seq
                """,
                (run_id,),
            )
        )
        error_rows = rows_to_dicts(
            self.database.fetchall(
                """
                SELECT er.error_code, e.provider_event_type
                FROM error_events er
                JOIN events e ON e.id = er.event_id
                WHERE e.run_id = ?
                ORDER BY e.seq
                """,
                (run_id,),
            )
        )
        actionable_errors = [
            row
            for row in error_rows
            if not is_parser_diagnostic_provider_type(row.get("provider_event_type"))
            and row.get("error_code") != "unsupported_event"
        ]
        task_summary = derive_task_summary(
            run.get("prompt") or run.get("source_name"),
            run.get("session_title") or run.get("source_name") or "Imported run",
        )
        unique_files = sorted(
            {row["normalized_path"] for row in diff_rows if row["normalized_path"]}
        )
        validation_bits = []
        for test in tests[:3]:
            validation_bits.append(f"`{test['command_text']}`: {test['result'] or 'unknown'}")
        validation_summary = ", ".join(validation_bits) if validation_bits else "No validation commands were recorded."
        if actionable_errors and any(test.get("result") == "passed" for test in tests):
            failure_summary = "Recovered after earlier failures."
        elif actionable_errors and (run.get("is_partial") or run.get("run_status") != "completed"):
            failure_summary = "Run ended before recovery was shown."
        elif actionable_errors:
            failure_summary = "Failures were recorded without a confirmed recovery."
        else:
            failure_summary = "No actionable failures were recorded."
        if actionable_errors:
            reviewer_notes = "Start with the first failing step, compare the last patch, and verify the final validation coverage."
        elif len(unique_files) >= 4 and not tests:
            reviewer_notes = "Review the blast radius carefully because the run changed several files without recorded validation."
        else:
            reviewer_notes = "Review the main command sequence, the final patch, and the last recorded validation step."
        markdown = "\n".join(
            [
                "## Run overview",
                f"- Task: {task_summary}",
                f"- State: {run_state_label(run.get('run_status') or 'unknown', bool(run.get('is_partial')))}",
                f"- Review priority: {run.get('review_attention') or 'low'}",
                f"- Files changed: {len(unique_files)}",
                f"- Validation: {validation_summary}",
                f"- Recovery: {failure_summary}",
                "",
                "## What to review",
                f"- {reviewer_notes}",
            ]
        )
        return {
            "markdown": markdown,
            "json": {
                "task_summary": task_summary,
                "validation_summary": validation_summary,
                "changed_files_summary": ", ".join(unique_files[:5]) if unique_files else "No file diffs were captured.",
                "failure_summary": failure_summary,
                "reviewer_notes": reviewer_notes,
            },
        }

    def delete_run(self, run_id: str) -> None:
        artifacts = rows_to_dicts(self.database.fetchall("SELECT storage_path FROM artifacts WHERE run_id = ?", (run_id,)))
        self.database.execute("DELETE FROM runs WHERE id = ?", (run_id,))
        self.database.execute(
            """
            UPDATE discovered_sources
            SET run_id = NULL,
                status = CASE
                    WHEN source_kind = 'codex_session' THEN 'metadata_only'
                    WHEN status = 'missing' THEN 'missing'
                    ELSE 'pending'
                END
            WHERE run_id = ?
            """,
            (run_id,),
        )
        for artifact in artifacts:
            path = Path(artifact["storage_path"])
            if path.exists():
                path.unlink()

    def build_export(self, run_id: str, include_raw_artifacts: bool = True) -> Dict[str, Any]:
        run = row_to_dict(self.database.fetchone("SELECT * FROM runs WHERE id = ?", (run_id,)))
        if not run:
            raise FileNotFoundError(run_id)
        events = rows_to_dicts(self.database.fetchall("SELECT * FROM events WHERE run_id = ? ORDER BY seq", (run_id,)))
        skills = self.get_clean_skill_signals(run_id)
        insights = rows_to_dicts(self.database.fetchall("SELECT * FROM insights WHERE run_id = ?", (run_id,)))
        artifacts = rows_to_dicts(self.database.fetchall("SELECT * FROM artifacts WHERE run_id = ?", (run_id,)))
        summary_payload = self.build_summary_payload(run_id, run_row=run)
        export_id = make_id("exp")
        export_path = self.settings.export_dir / f"{export_id}.zip"
        checksums: Dict[str, str] = {}
        with zipfile.ZipFile(export_path, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
            run_payload = {
                "id": run["id"],
                "provider": run["provider"],
                "provider_run_id": run["provider_run_id"],
                "session_id": run["session_id"],
                "repo_name": run["repo_name"],
                "repo_root": run["repo_root"],
                "prompt": run["prompt"],
                "run_status": run["run_status"],
                "is_partial": bool(run["is_partial"]),
                "provider_version": run["provider_version"],
                "started_at": run["started_at"],
                "ended_at": run["ended_at"],
                "duration_ms": run["duration_ms"],
                "review_attention": run["review_attention"],
            }
            event_payload_lines = []
            for event in events:
                detail = self.get_event_detail(run_id, event["id"])
                event_payload_lines.append(
                    json.dumps(
                        {
                            **event,
                            "raw_payload": json.loads(event["raw_payload_json"]) if event["raw_payload_json"] else {},
                            "detail": detail,
                        },
                        ensure_ascii=True,
                    )
                )
            files = {
                "run.json": json.dumps(run_payload, ensure_ascii=True, indent=2),
                "events.jsonl": "\n".join(event_payload_lines),
                "summary.md": summary_payload["markdown"],
                "insights.json": json.dumps([
                    {
                        "code": item["code"],
                        "severity": item["severity"],
                        "title": item["title"],
                        "message": item["message"],
                        "recommendation": item["recommendation"],
                        "event_ids": json.loads(item["event_ids_json"]),
                    }
                    for item in insights
                ], ensure_ascii=True, indent=2),
                "skills.json": json.dumps([
                    {
                        "event_id": item["event_id"],
                        "name": item["name"],
                        "mode": item["mode"],
                        "confidence": item["confidence"],
                        "event_ids": item["event_ids"],
                        "evidence_source": item["evidence_source"],
                    }
                    for item in skills
                ], ensure_ascii=True, indent=2),
            }
            if include_raw_artifacts:
                for artifact in artifacts:
                    storage_path = Path(artifact["storage_path"])
                    if storage_path.exists():
                        raw_name = f"raw/{artifact['filename'] or storage_path.name}"
                        bundle.write(storage_path, raw_name)
                        checksums[raw_name] = sha256_path(storage_path)
            for file_name, content in files.items():
                bundle.writestr(file_name, content)
                checksums[file_name] = hashlib.sha256(content.encode("utf-8")).hexdigest()
            manifest = {
                "schema_version": "1.0",
                "run_id": run_id,
                "generated_at": now_iso(),
                "files": sorted(checksums.keys()),
            }
            bundle.writestr("manifest.json", json.dumps(manifest, ensure_ascii=True, indent=2))
            bundle.writestr("checksums.json", json.dumps(checksums, ensure_ascii=True, indent=2))
        artifact_id = make_id("art")
        self.database.execute(
            """
            INSERT INTO artifacts (id, run_id, artifact_type, filename, mime_type, sha256, size_bytes, storage_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                artifact_id,
                run_id,
                "export_bundle",
                export_path.name,
                "application/zip",
                sha256_path(export_path),
                export_path.stat().st_size,
                str(export_path),
            ),
        )
        self.database.execute(
            "INSERT INTO exports (id, run_id, export_format, artifact_id) VALUES (?, ?, ?, ?)",
            (export_id, run_id, "bundle", artifact_id),
        )
        return {
            "export_id": export_id,
            "artifact_id": artifact_id,
            "download_path": str(export_path),
        }

    def get_event_detail(self, run_id: str, event_id: str) -> Dict[str, Any]:
        event = row_to_dict(self.database.fetchone("SELECT * FROM events WHERE run_id = ? AND id = ?", (run_id, event_id)))
        if not event:
            raise FileNotFoundError(event_id)
        for table in ["command_events", "test_events", "diff_events", "error_events", "tool_events"]:
            detail = row_to_dict(self.database.fetchone(f"SELECT * FROM {table} WHERE event_id = ?", (event_id,)))
            if detail:
                if table == "diff_events" and detail.get("diff_artifact_id"):
                    artifact = row_to_dict(self.database.fetchone("SELECT storage_path FROM artifacts WHERE id = ?", (detail["diff_artifact_id"],)))
                    if artifact and Path(artifact["storage_path"]).exists():
                        detail["diff_text"] = Path(artifact["storage_path"]).read_text(encoding="utf-8")
                if table == "command_events":
                    for key, artifact_key in [("stdout_preview", "stdout_artifact_id"), ("stderr_preview", "stderr_artifact_id")]:
                        if detail.get(artifact_key):
                            artifact = row_to_dict(self.database.fetchone("SELECT storage_path FROM artifacts WHERE id = ?", (detail[artifact_key],)))
                            if artifact and Path(artifact["storage_path"]).exists():
                                detail[key] = Path(artifact["storage_path"]).read_text(encoding="utf-8")
                return {table.replace("_events", ""): detail}
        return {}


class DiscoveryService:
    def __init__(self, settings: Settings, database: Database, replay: ReplayService):
        self.settings = settings
        self.database = database
        self.replay = replay
        self.last_scan_at: Optional[str] = None
        self.last_scan_warning: Optional[str] = None
        self.scan_counts: Dict[str, int] = {}
        self.scan_in_progress = False

    def scan(self) -> Dict[str, Any]:
        self.scan_in_progress = True
        counts = Counter()
        warning = None
        known_source_ids: set[str] = set()
        try:
            codex_home = self.settings.codex_home
            if not codex_home.exists():
                warning = f"Codex home not found at {codex_home}"
                self.last_scan_warning = warning
                return {"counts": {}, "warning": warning}
            session_index = codex_home / "session_index.jsonl"
            session_map: Dict[str, str] = {}
            if session_index.exists():
                for raw_line in session_index.read_text(encoding="utf-8", errors="replace").splitlines():
                    if not raw_line.strip():
                        continue
                    try:
                        item = json.loads(raw_line)
                    except json.JSONDecodeError:
                        warning = "session_index.jsonl contained malformed JSON."
                        continue
                    source_id = self._upsert_source(
                        provider="codex",
                        source_kind="codex_session",
                        absolute_path=str(session_index),
                        artifact_name=session_index.name,
                        session_id=item.get("id"),
                        thread_name=item.get("thread_name"),
                        source_updated_at=item.get("updated_at"),
                        status="metadata_only",
                        fingerprint=f"session-index:{item.get('id')}:{item.get('updated_at')}",
                        is_active=0,
                        metadata={"source": "session_index"},
                    )
                    session_map[item.get("id")] = source_id
                    known_source_ids.add(source_id)
                    counts["sessions"] += 1
            for directory, source_kind in [(codex_home / "sessions", "codex_trace"), (codex_home / "archived_sessions", "codex_archive")]:
                if not directory.exists():
                    continue
                for path in sorted(directory.rglob("*.jsonl")):
                    if path.name == "session_index.jsonl":
                        continue
                    fingerprint = f"{path.stat().st_size}:{int(path.stat().st_mtime)}:{sha256_path(path)}"
                    session_id = self._extract_session_id(path)
                    is_active = 1 if source_kind == "codex_trace" else 0
                    source_id = self._upsert_source(
                        provider="codex",
                        source_kind=source_kind,
                        absolute_path=str(path),
                        artifact_name=path.name,
                        session_id=session_id,
                        thread_name=None,
                        source_updated_at=datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
                        status="pending",
                        fingerprint=fingerprint,
                        is_active=is_active,
                        metadata={"source": source_kind},
                    )
                    known_source_ids.add(source_id)
                    counts[source_kind] += 1
                    existing = row_to_dict(
                        self.database.fetchone(
                            """
                            SELECT ds.fingerprint, ds.run_id, ds.status, r.run_status, r.parse_status, r.is_partial
                            FROM discovered_sources ds
                            LEFT JOIN runs r ON r.id = ds.run_id
                            WHERE ds.id = ?
                            """,
                            (source_id,),
                        )
                    )
                    if existing and existing["fingerprint"] == fingerprint and existing.get("run_id"):
                        self.database.execute(
                            "UPDATE discovered_sources SET status = ?, updated_at = datetime('now') WHERE id = ?",
                            (self._source_status_for_run(existing), source_id),
                        )
                        continue
                    self.database.execute("UPDATE discovered_sources SET status = 'importing', fingerprint = ?, updated_at = datetime('now') WHERE id = ?", (fingerprint, source_id))
                    try:
                        result = self.replay.import_path(path, source_name=path.name, source_id=source_id, import_mode="discovery")
                        counts["imported"] += 1
                        if session_id and session_id in session_map:
                            self.database.execute(
                                "UPDATE discovered_sources SET run_id = ?, status = CASE WHEN status = 'metadata_only' THEN 'metadata_only' ELSE status END WHERE id = ?",
                                (result["run_id"], session_map[session_id]),
                            )
                    except Exception as exc:
                        counts["failed"] += 1
                        self.database.execute(
                            "UPDATE discovered_sources SET status = 'failed', warning = ?, updated_at = datetime('now') WHERE id = ?",
                            (str(exc), source_id),
                        )
            if known_source_ids:
                placeholders = ", ".join("?" for _ in known_source_ids)
                self.database.execute(
                    f"UPDATE discovered_sources SET status = 'missing', updated_at = datetime('now') WHERE source_kind IN ('codex_trace','codex_archive') AND id NOT IN ({placeholders})",
                    tuple(known_source_ids),
                )
            self.last_scan_warning = warning
            self.scan_counts = dict(counts)
            self.last_scan_at = now_iso()
            return {"counts": dict(counts), "warning": warning}
        finally:
            self.scan_in_progress = False

    def _source_status_for_run(self, run: Optional[Dict[str, Any]]) -> str:
        if not run:
            return "pending"
        if bool(run.get("is_partial")):
            return "partial"
        if run.get("parse_status") in {"success", "partial"}:
            return "ready"
        return "failed"

    def _extract_session_id(self, path: Path) -> Optional[str]:
        try:
            with path.open("r", encoding="utf-8", errors="replace") as handle:
                first_line = handle.readline().strip()
                if not first_line:
                    return None
                obj = json.loads(first_line)
                payload = obj.get("payload") or {}
                return payload.get("id")
        except Exception:
            return None

    def _upsert_source(
        self,
        *,
        provider: str,
        source_kind: str,
        absolute_path: Optional[str],
        artifact_name: Optional[str],
        session_id: Optional[str],
        thread_name: Optional[str],
        source_updated_at: Optional[str],
        status: str,
        fingerprint: str,
        is_active: int,
        metadata: Dict[str, Any],
    ) -> str:
        if source_kind == "codex_session":
            existing = row_to_dict(
                self.database.fetchone(
                    """
                    SELECT * FROM discovered_sources
                    WHERE provider = ? AND source_kind = ? AND session_id = ?
                    """,
                    (provider, source_kind, session_id),
                )
            )
        else:
            existing = row_to_dict(
                self.database.fetchone(
                    """
                    SELECT * FROM discovered_sources
                    WHERE provider = ? AND source_kind = ? AND absolute_path IS ?
                    """,
                    (provider, source_kind, absolute_path),
                )
            )
        if existing:
            self.database.execute(
                """
                UPDATE discovered_sources
                SET artifact_name = ?, session_id = COALESCE(?, session_id), thread_name = COALESCE(?, thread_name),
                    source_updated_at = ?, status = ?, fingerprint = ?, is_active = ?, warning = NULL,
                    metadata_json = ?, updated_at = datetime('now')
                WHERE id = ?
                """,
                (
                    artifact_name,
                    session_id,
                    thread_name,
                    source_updated_at,
                    status if existing.get("status") != "missing" else existing.get("status"),
                    fingerprint,
                    is_active,
                    json_dump(metadata),
                    existing["id"],
                ),
            )
            return existing["id"]
        source_id = make_id("src")
        self.database.execute(
            """
            INSERT INTO discovered_sources (
                id, provider, source_kind, absolute_path, artifact_name, session_id, thread_name,
                source_updated_at, discovered_at, status, fingerprint, run_id, is_active, warning, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL, ?)
            """,
            (
                source_id,
                provider,
                source_kind,
                absolute_path,
                artifact_name,
                session_id,
                thread_name,
                source_updated_at,
                now_iso(),
                status,
                fingerprint,
                is_active,
                json_dump(metadata),
            ),
        )
        return source_id

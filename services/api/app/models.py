from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ParsedEvent:
    id: str
    event_type: str
    title: str
    timestamp: Optional[str]
    status: str
    provider_event_type: Optional[str]
    message_text: Optional[str]
    raw_payload: Dict[str, Any]
    parent_seq: Optional[int] = None
    step_group: Optional[str] = None
    step_label: Optional[str] = None
    artifact_ref_id: Optional[str] = None
    command: Optional[Dict[str, Any]] = None
    test: Optional[Dict[str, Any]] = None
    diff: Optional[Dict[str, Any]] = None
    error: Optional[Dict[str, Any]] = None
    tool: Optional[Dict[str, Any]] = None


@dataclass
class ParsedSkill:
    name: str
    mode: str
    confidence: float
    event_id: str
    event_ids: List[str]
    evidence_source: str


@dataclass
class ParsedRun:
    provider: str
    provider_run_id: Optional[str]
    session_id: Optional[str]
    repo_name: Optional[str]
    repo_root: Optional[str]
    source_name: str
    source_path: Optional[str]
    prompt: Optional[str]
    run_status: str
    is_partial: bool
    parse_status: str
    provider_version: Optional[str]
    started_at: Optional[str]
    ended_at: Optional[str]
    duration_ms: Optional[int]
    warnings: List[str] = field(default_factory=list)
    events: List[ParsedEvent] = field(default_factory=list)
    skills: List[ParsedSkill] = field(default_factory=list)
    raw_artifacts: List[Dict[str, Any]] = field(default_factory=list)

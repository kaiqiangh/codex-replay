export type TimelineFilter = "all" | "errors" | "commands" | "diffs" | "tests" | "skills";

export type TimelineLike = {
  event_id: string;
  event_type: string;
  label: string;
  has_diff: boolean;
  has_error: boolean;
  has_skill: boolean;
};

const EVENT_LABELS: Record<string, string> = {
  run_started: "Run started",
  message: "User note",
  summary: "Agent summary",
  command: "Command",
  test: "Validation",
  diff: "Diff",
  tool_call: "Tool call",
  error: "Error",
  warning: "Warning",
};

const NOISE_PATTERNS = [
  "AGENTS.md instructions",
  "<INSTRUCTIONS>",
  "</INSTRUCTIONS>",
  "<environment_context>",
  "</environment_context>",
  "JavaScript REPL",
  "SKILL.md",
  ".agents/skills",
  ".codex/skills",
  "current_date",
  "timezone",
  "request_user_input",
];

const TIMEZONE_OR_SLASH_TOKEN = /^[A-Za-z_+-]+\/[A-Za-z0-9_+-]+$/;
const SIGNIFICANT_EVENT_TYPES = new Set(["command", "test", "diff", "error", "tool_call"]);

function clampText(value: string, limit: number) {
  if (value.length <= limit) {
    return value;
  }
  return `${value.slice(0, Math.max(0, limit - 1)).trimEnd()}…`;
}

export function truncateMiddle(value: string, limit = 40) {
  if (value.length <= limit) {
    return value;
  }
  const head = Math.ceil((limit - 1) / 2);
  const tail = Math.floor((limit - 1) / 2);
  return `${value.slice(0, head)}…${value.slice(-tail)}`;
}

function cleanPresentationText(value: string) {
  return value
    .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1")
    .replace(/`([^`]+)`/g, "$1")
    .replace(/\$[a-z0-9-]+/gi, " ")
    .replace(/\/Users\/[^\s)]+/g, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/[ \t]+/g, " ")
    .replace(/\s*\n\s*/g, "\n")
    .trim();
}

function scoreCandidate(line: string) {
  let score = 0;
  const lowered = line.toLowerCase();
  if (/^(please|you|implement|fix|improve|continue|add|build|review|create|deliver)\b/i.test(line)) {
    score += 60;
  }
  if (line.length >= 10 && line.length <= 160) {
    score += 18;
  }
  if (line.length <= 90) {
    score += 10;
  }
  if (line.split(/\s+/).length >= 4) {
    score += 8;
  }
  if (/\b(implement|improve|fix|continue|add|review|deliver|build|create|refresh|investigate)\b/i.test(line)) {
    score += 24;
  }
  if (/\b(ui|ux|frontend|trace|replay|landing|inspector|mvp|plan|design)\b/i.test(line)) {
    score += 14;
  }
  if (TIMEZONE_OR_SLASH_TOKEN.test(line)) {
    score -= 64;
  }
  if (!line.includes(" ") && line.length < 24) {
    score -= 28;
  }
  if (/^(zsh|bash|fish|sh)$/i.test(line)) {
    score -= 44;
  }
  if (/\.(jsonl|md|ts|tsx|py|js|jsx)$/i.test(line) && /[\\/]/.test(line)) {
    score -= 28;
  }
  if (/\b(skill|fallback|instruction|context|environment|path|policy)\b/i.test(line)) {
    score -= 32;
  }
  if (/^#+\s/.test(line) || /^[-*]\s/.test(line)) {
    score -= 6;
  }
  if (/^\d+[.)]\s+/.test(line)) {
    score -= 28;
  }
  if (line.includes("{") || line.includes("}") || line.includes("</")) {
    score -= 14;
  }
  for (const pattern of NOISE_PATTERNS) {
    if (lowered.includes(pattern.toLowerCase())) {
      score -= 36;
    }
  }
  return score;
}

export function humanizeEventType(eventType: string) {
  return EVENT_LABELS[eventType] ?? eventType.replaceAll("_", " ");
}

export function normalizeDisplayTitle(value: string | null | undefined, fallback: string) {
  const raw = cleanPresentationText(value ?? "");
  const candidates = raw
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .filter((line) => !/^#+\s+/.test(line))
    .filter((line) => !TIMEZONE_OR_SLASH_TOKEN.test(line))
    .filter((line) => !/^(cwd|shell|timezone|current_date)\b/i.test(line))
    .filter((line) => !/^\d+[.)]\s+/.test(line))
    .filter((line) => !/^[-*]\s+/.test(line))
    .filter((line) => !/^[-*]\s*(use|description|license|metadata|args)\b/i.test(line))
    .filter((line) => !/^[-*]\s*[\w/-]+:\s/.test(line))
    .filter((line) => !/^(?:[-*]\s*)?missing\/blocked:/i.test(line))
    .filter((line) => !/\bUse this skill\b/i.test(line))
    .filter((line) => !/\bThis skill should be used\b/i.test(line))
    .filter((line) => !/\bIf a named skill\b/i.test(line));

  const best = candidates
    .map((line, index) => ({ line, index, score: scoreCandidate(line) }))
    .sort((left, right) => right.score - left.score || right.index - left.index)[0];

  const chosen = best && best.score > 20 ? best.line : fallback;
  return clampText(chosen || fallback, 96);
}

export function normalizeTimelineLabel(label: string, eventType: string) {
  const normalized = normalizeDisplayTitle(label, humanizeEventType(eventType));
  if (
    (eventType === "message" || eventType === "summary") &&
    (
      !normalized.includes(" ") ||
      normalized.length < 14 ||
      normalized.toLowerCase().includes("agents.md instructions") ||
      normalized.toLowerCase().includes("filesystem sandboxing defines")
    )
  ) {
    return humanizeEventType(eventType);
  }
  return normalized;
}

export function compactArtifactName(value: string | null | undefined) {
  if (!value) {
    return "Unnamed artifact";
  }
  const rolloutMatch = value.match(
    /^rollout-(\d{4}-\d{2}-\d{2})T(\d{2})-(\d{2})-(\d{2})-([^.]+)\.jsonl$/,
  );
  if (rolloutMatch) {
    const [, date, hour, minute, , session] = rolloutMatch;
    return `rollout ${date} ${hour}:${minute} • ${session.slice(0, 8)}`;
  }
  return truncateMiddle(value, 42);
}

export function compactPath(value: string | null | undefined, repoRoot?: string | null) {
  if (!value) {
    return "Unavailable";
  }
  if (repoRoot && value.startsWith(repoRoot)) {
    const relative = value.slice(repoRoot.length).replace(/^\/+/, "");
    return relative || ".";
  }
  const parts = value.split("/").filter(Boolean);
  if (parts.length <= 3) {
    return value;
  }
  return parts.slice(-3).join("/");
}

export function summarizeDelimitedText(value: string | null | undefined) {
  if (!value) {
    return [];
  }
  return value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

export function parseSummaryMarkdown(markdown: string | null | undefined) {
  if (!markdown) {
    return [];
  }
  return markdown
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      if (line.startsWith("## ")) {
        return { kind: "heading" as const, text: line.slice(3).trim() };
      }
      if (line.startsWith("- ")) {
        return { kind: "bullet" as const, text: line.slice(2).trim() };
      }
      return { kind: "paragraph" as const, text: line };
    });
}

export function matchesTimelineFilter(item: TimelineLike, filter: TimelineFilter) {
  switch (filter) {
    case "errors":
      return item.has_error || item.event_type === "error";
    case "commands":
      return item.event_type === "command" || item.event_type === "tool_call";
    case "diffs":
      return item.has_diff || item.event_type === "diff";
    case "tests":
      return item.event_type === "test";
    case "skills":
      return item.has_skill;
    default:
      return true;
  }
}

export function timelineTone(item: TimelineLike) {
  if (item.has_error || item.event_type === "error") {
    return "danger";
  }
  if (item.event_type === "test") {
    return "warning";
  }
  if (item.has_diff || item.event_type === "diff") {
    return "accent";
  }
  if (item.event_type === "command" || item.event_type === "tool_call" || item.has_skill) {
    return "signal";
  }
  return "neutral";
}

export function pickInitialTimelineEventId(items: TimelineLike[]) {
  if (!items.length) {
    return null;
  }

  const firstEvidence = items.find((item) => {
    if (item.has_error || item.has_diff) {
      return true;
    }
    if (SIGNIFICANT_EVENT_TYPES.has(item.event_type)) {
      return true;
    }
    return false;
  });

  if (firstEvidence) {
    return firstEvidence.event_id;
  }

  const firstMeaningfulNarrative = items.find((item) => {
    if (item.event_type === "summary") {
      return true;
    }
    if (item.event_type === "message") {
      const normalized = normalizeTimelineLabel(item.label, item.event_type);
      return normalized !== "User note";
    }
    return false;
  });

  return firstMeaningfulNarrative?.event_id ?? items[0].event_id;
}

export function previewMessageText(value: string, limit = 900) {
  const cleaned = cleanPresentationText(value).replace(/\n{3,}/g, "\n\n").trim();
  if (cleaned.length <= limit) {
    return cleaned;
  }
  return `${cleaned.slice(0, Math.max(0, limit - 1)).trimEnd()}…`;
}

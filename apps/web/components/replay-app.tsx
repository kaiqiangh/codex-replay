"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import {
  startTransition,
  useDeferredValue,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ChangeEvent,
  type FormEvent,
  type KeyboardEvent,
} from "react";

import {
  API_BASE,
  apiGet,
  apiPost,
  apiUpload,
  formatDate,
  formatDuration,
} from "../lib/api";
import {
  compactArtifactName,
  compactPath,
  humanizeEventType,
  isLowSignalMessage,
  matchesTimelineFilter,
  normalizeDisplayTitle,
  normalizeTimelineLabel,
  parseSummaryMarkdown,
  pickInitialTimelineEventId,
  previewMessageText,
  summarizeDelimitedText,
  timelineTone,
  type TimelineFilter,
} from "../lib/presentation";

type DiscoveryItem = {
  id: string;
  source_kind: string;
  artifact_name?: string | null;
  thread_name?: string | null;
  session_id?: string | null;
  source_updated_at?: string | null;
  status: string;
  run_id?: string | null;
  is_active: boolean;
  warning?: string | null;
};

type DiscoveryData = {
  items: DiscoveryItem[];
  total: number;
};

type DiscoveryStatus = {
  last_scan_at?: string | null;
  in_progress: boolean;
  warning?: string | null;
  counts: Record<string, number>;
};

type RunCounts = {
  events: number;
  commands: number;
  tests: number;
  errors: number;
  files_changed: number;
  diagnostics?: number;
};

type RunItem = {
  id: string;
  provider: string;
  repo_name?: string | null;
  repo_root?: string | null;
  session_title?: string | null;
  source_name?: string | null;
  prompt?: string | null;
  task_summary?: string | null;
  validation_summary?: string | null;
  failure_summary?: string | null;
  reviewer_notes?: string | null;
  summary_markdown?: string | null;
  status: string;
  state_key?: "ready" | "partial" | "unresolved";
  state_label?: string;
  parse_status: string;
  started_at?: string | null;
  ended_at?: string | null;
  duration_ms?: number | null;
  total_events: number;
  total_files_changed: number;
  total_commands: number;
  total_tests: number;
  total_errors: number;
  review_attention?: "high" | "medium" | "low";
  counts?: RunCounts;
};

type RunListData = {
  items: RunItem[];
  total: number;
};

type RunDetail = RunItem & {
  first_error_seq?: number | null;
  counts: RunCounts;
};

type TimelineItem = {
  seq: number;
  event_id: string;
  event_type: string;
  label: string;
  status: string;
  has_diff: boolean;
  has_error: boolean;
  has_skill: boolean;
};

type EventDetailEnvelope = {
  id: string;
  seq: number;
  event_type: string;
  title: string;
  status: string;
  timestamp?: string | null;
  message_text?: string | null;
  raw_payload?: unknown;
  detail: Record<string, any>;
};

type Insight = {
  id: string;
  code: string;
  severity: string;
  title: string;
  message: string;
  recommendation?: string | null;
  event_ids: string[];
};

type Skill = {
  id: string;
  event_id: string;
  name: string;
  mode: string;
  confidence: number;
  event_ids: string[];
  first_seq: number;
};

type SummaryData = {
  markdown: string;
  status: string;
  json: {
    task_summary: string;
    validation_summary: string;
    changed_files_summary: string;
    failure_summary: string;
    reviewer_notes: string;
  };
};

type DiffListItem = {
  event_id: string;
  seq: number;
  file_path: string;
  change_type: string;
  lines_added: number;
  lines_removed: number;
};

type DiffDetail = {
  event_id?: string;
  file_path: string;
  normalized_path?: string;
  change_type: string;
  lines_added: number;
  lines_removed: number;
  diff_text: string;
};

const TIMELINE_FILTERS: Array<{ key: TimelineFilter; label: string }> = [
  { key: "all", label: "All" },
  { key: "errors", label: "Errors" },
  { key: "commands", label: "Commands" },
  { key: "diffs", label: "Diffs" },
  { key: "tests", label: "Tests" },
  { key: "skills", label: "Skills" },
];

const RUN_SORT_OPTIONS = [
  { value: "review_attention_desc", label: "Priority first" },
  { value: "started_at_desc", label: "Newest first" },
  { value: "started_at_asc", label: "Oldest first" },
];

const PROVIDER_OPTIONS = [
  { value: "all", label: "All providers" },
  { value: "codex", label: "Codex" },
];

const STATE_OPTIONS = [
  { value: "all", label: "All states" },
  { value: "ready", label: "Ready replay" },
  { value: "partial", label: "Partial replay" },
  { value: "unresolved", label: "Unresolved replay" },
];

const LEDGER_SORT_OPTIONS = [
  { value: "oldest", label: "Oldest first" },
  { value: "newest", label: "Newest first" },
] as const;

const DETAIL_TABS = [
  { value: "evidence", label: "Evidence" },
  { value: "diffs", label: "Diff review" },
  { value: "raw", label: "Raw payload" },
] as const;

function classNames(...values: Array<string | false | null | undefined>) {
  return values.filter(Boolean).join(" ");
}

function truncateId(value: string) {
  return value.length > 12 ? `${value.slice(0, 8)}…${value.slice(-4)}` : value;
}

function toneForStatus(status?: string | null) {
  if (!status) {
    return "neutral" as const;
  }
  if (status === "ready" || status === "completed" || status === "ok") {
    return "good" as const;
  }
  if (status === "partial" || status === "unknown") {
    return "warn" as const;
  }
  if (status === "failed" || status === "error" || status === "unresolved") {
    return "bad" as const;
  }
  return "neutral" as const;
}

function toneForAttention(attention?: string | null) {
  if (attention === "high") {
    return "bad" as const;
  }
  if (attention === "medium") {
    return "warn" as const;
  }
  return "good" as const;
}

function toneForState(stateKey?: string | null, fallbackStatus?: string | null) {
  if (stateKey === "unresolved") {
    return "bad" as const;
  }
  if (stateKey === "partial") {
    return "warn" as const;
  }
  if (stateKey === "ready") {
    return "good" as const;
  }
  return toneForStatus(fallbackStatus);
}

function timelineMarkers(item: TimelineItem) {
  const markers = [humanizeEventType(item.event_type)];
  if (item.has_error && item.event_type !== "error") {
    markers.push("error");
  }
  if (item.has_diff && item.event_type !== "diff") {
    markers.push("diff");
  }
  if (item.has_skill) {
    markers.push("skill");
  }
  return markers;
}

function runDisplayTitle(
  run: Pick<RunItem, "session_title" | "task_summary" | "prompt" | "source_name" | "repo_name" | "id">,
) {
  if (run.session_title) {
    return run.session_title;
  }
  return normalizeDisplayTitle(
    run.task_summary ?? run.prompt ?? run.source_name,
    run.source_name ? compactArtifactName(run.source_name) : run.repo_name ?? run.id,
  );
}

function runDisplayNote(
  run: Pick<RunItem, "reviewer_notes" | "failure_summary" | "validation_summary">,
) {
  const source = run.reviewer_notes ?? run.failure_summary ?? run.validation_summary;
  return source ? normalizeDisplayTitle(source, "Replay snapshot ready.") : "Replay snapshot ready.";
}

function attentionCopy(level?: string | null) {
  if (level === "high") {
    return "Open now: unresolved evidence, thin validation, or strong failure signals.";
  }
  if (level === "medium") {
    return "Open soon: reviewable run with churn, warnings, or partial confidence.";
  }
  return "Open later: contained run with proportionate validation evidence.";
}

function buildRunsPath({
  query,
  provider,
  state,
  sort,
  limit = 100,
}: {
  query: string;
  provider: string;
  state: string;
  sort: string;
  limit?: number;
}) {
  const params = new URLSearchParams();
  params.set("limit", String(limit));
  params.set("sort", sort);
  if (query) {
    params.set("q", query);
  }
  if (provider !== "all") {
    params.set("provider", provider);
  }
  if (state !== "all") {
    params.set("state", state);
  }
  return `/runs?${params.toString()}`;
}

function StatusBadge({
  label,
  tone,
}: {
  label: string;
  tone: "neutral" | "good" | "warn" | "bad" | "accent";
}) {
  return <span className={`status-badge status-${tone}`}>{label}</span>;
}

function EmptyPanel({
  title,
  copy,
  action,
}: {
  title: string;
  copy: string;
  action?: React.ReactNode;
}) {
  return (
    <div className="empty-panel">
      <h3>{title}</h3>
      <p>{copy}</p>
      {action}
    </div>
  );
}

function SectionHeading({
  eyebrow,
  title,
  meta,
}: {
  eyebrow: string;
  title: string;
  meta?: React.ReactNode;
}) {
  return (
    <div className="section-heading">
      <div>
        <div className="section-eyebrow">{eyebrow}</div>
        <h2>{title}</h2>
      </div>
      {meta}
    </div>
  );
}

function MetricTile({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="metric-tile">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function FilterChip({
  label,
  active,
  count,
  onClick,
}: {
  label: string;
  active: boolean;
  count: number;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      className={classNames("filter-chip", active && "filter-chip-active")}
      onClick={onClick}
    >
      <span>{label}</span>
      <strong>{count}</strong>
    </button>
  );
}

function SummaryDigest({ markdown }: { markdown: string }) {
  const lines = useMemo(() => parseSummaryMarkdown(markdown), [markdown]);

  if (!lines.length) {
    return <p className="muted">No summary markdown available.</p>;
  }

  return (
    <div className="summary-digest">
      {lines.map((line, index) => {
        if (line.kind === "heading") {
          return (
            <h4 key={`${line.kind}-${index}`} className="summary-heading">
              {line.text}
            </h4>
          );
        }
        if (line.kind === "bullet") {
          return (
            <div key={`${line.kind}-${index}`} className="summary-row">
              <span className="summary-dot" />
              <span>{line.text}</span>
            </div>
          );
        }
        return (
          <p key={`${line.kind}-${index}`} className="summary-paragraph">
            {line.text}
          </p>
        );
      })}
    </div>
  );
}

function PriorityLegend() {
  return (
    <div className="legend-grid">
      <article className="legend-card">
        <StatusBadge label="Review priority high" tone="bad" />
        <p>Unresolved or incomplete run, serious validation gap, or strong failure evidence.</p>
      </article>
      <article className="legend-card">
        <StatusBadge label="Review priority medium" tone="warn" />
        <p>Reviewable run with churn, warnings, or partial confidence.</p>
      </article>
      <article className="legend-card">
        <StatusBadge label="Review priority low" tone="good" />
        <p>Scoped, completed run with proportionate validation coverage.</p>
      </article>
    </div>
  );
}

function EvidenceOutput({
  label,
  value,
  emptyCopy,
}: {
  label: string;
  value?: string | null;
  emptyCopy: string;
}) {
  if (!value?.trim()) {
    return <p className="muted">{emptyCopy}</p>;
  }

  const trimmed = value.trim();
  if (trimmed.length <= 520) {
    return (
      <div className="evidence-output">
        <div className="mini-label">{label}</div>
        <pre className="evidence-pre">{trimmed}</pre>
      </div>
    );
  }

  return (
    <details className="fold-panel">
      <summary>{label}</summary>
      <pre className="evidence-pre">{trimmed}</pre>
    </details>
  );
}

export function LandingPage() {
  const router = useRouter();
  const [discovery, setDiscovery] = useState<DiscoveryData>({ items: [], total: 0 });
  const [runs, setRuns] = useState<RunListData>({ items: [], total: 0 });
  const [status, setStatus] = useState<DiscoveryStatus | null>(null);
  const [importPath, setImportPath] = useState("");
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        setLoading(true);
        setError(null);
        const [discoveryData, runsData, statusData] = await Promise.all([
          apiGet<DiscoveryData>("/discovery/sources?limit=24"),
          apiGet<RunListData>("/runs?limit=12&sort=review_attention_desc"),
          apiGet<DiscoveryStatus>("/discovery/status"),
        ]);
        if (cancelled) {
          return;
        }
        setDiscovery(discoveryData);
        setRuns(runsData);
        setStatus(statusData);
      } catch (loadError) {
        if (!cancelled) {
          setError(loadError instanceof Error ? loadError.message : "Failed to load replay data.");
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    void load();

    return () => {
      cancelled = true;
    };
  }, []);

  const readySources = useMemo(
    () =>
      discovery.items.filter(
        (item) => item.source_kind === "codex_trace" || item.source_kind === "codex_archive",
      ),
    [discovery.items],
  );

  const awaitingSessions = useMemo(
    () =>
      discovery.items.filter(
        (item) => item.source_kind === "codex_session" && !item.run_id,
      ),
    [discovery.items],
  );

  async function refreshDiscovery() {
    try {
      setBusy(true);
      setError(null);
      await apiPost("/discovery/scan");
      const [discoveryData, runsData, statusData] = await Promise.all([
        apiGet<DiscoveryData>("/discovery/sources?limit=24"),
        apiGet<RunListData>("/runs?limit=12&sort=review_attention_desc"),
        apiGet<DiscoveryStatus>("/discovery/status"),
      ]);
      setDiscovery(discoveryData);
      setRuns(runsData);
      setStatus(statusData);
    } catch (refreshError) {
      setError(
        refreshError instanceof Error ? refreshError.message : "Discovery refresh failed.",
      );
    } finally {
      setBusy(false);
    }
  }

  async function handleUpload(event: ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0];
    if (!file) {
      return;
    }
    try {
      setBusy(true);
      setError(null);
      const result = await apiUpload<{ run_id: string }>("/imports/file", file);
      router.push(`/runs/${result.run_id}`);
    } catch (uploadError) {
      setError(uploadError instanceof Error ? uploadError.message : "Upload failed.");
    } finally {
      setBusy(false);
      event.target.value = "";
    }
  }

  async function handlePathImport(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const path = importPath.trim();
    if (!path) {
      setError("Enter a local file path to import.");
      return;
    }
    try {
      setBusy(true);
      setError(null);
      const result = await apiPost<{ run_id: string }>("/imports/path", { path });
      router.push(`/runs/${result.run_id}`);
    } catch (importError) {
      setError(importError instanceof Error ? importError.message : "Path import failed.");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="page-shell page-enter">
      {error ? <div className="error-banner">{error}</div> : null}

      <section className="hero-shell">
        <div className="surface-panel hero-panel">
          <div className="eyebrow">Codex Replay</div>
          <h1 className="hero-title">Replay Codex runs with proof, not guesswork.</h1>
          <p className="hero-copy">
            Import a rollout artifact or point at a local path, then review commands, diffs,
            tests, failures, and skill context without decoding raw JSONL by hand.
          </p>

          <div className="hero-actions">
            <label className="button button-primary">
              <input
                type="file"
                className="sr-only"
                accept=".jsonl,.zip"
                onChange={handleUpload}
                disabled={busy}
              />
              Upload trace
            </label>
            <Link href="/runs" className="button button-secondary">
              Open review queue
            </Link>
            <button
              type="button"
              className="button button-ghost"
              onClick={() => void refreshDiscovery()}
              disabled={busy}
            >
              {busy ? "Refreshing..." : "Refresh local sources"}
            </button>
          </div>

          <form className="path-import-panel" onSubmit={handlePathImport}>
            <div className="mini-label">Local path import</div>
            <div className="path-import-row">
              <input
                type="text"
                value={importPath}
                onChange={(event) => setImportPath(event.target.value)}
                placeholder="~/.../rollout-2026-03-15T09-17-00.jsonl or export bundle.zip"
                aria-label="Local replay artifact path"
              />
              <button type="submit" className="button button-secondary" disabled={busy}>
                Import path
              </button>
            </div>
            <p className="field-hint">
              Use the existing local-path importer for JSONL traces or replay bundles already on
              disk.
            </p>
          </form>
        </div>

        <div className="surface-panel hero-sidecar">
          <SectionHeading eyebrow="Source model" title="Two lanes, two levels of evidence" />
          <div className="lane-definition-list">
            <article className="definition-card">
              <StatusBadge label="Ready to replay" tone="good" />
              <p>
                A Codex rollout artifact exists, was imported, and can be inspected step by step.
              </p>
            </article>
            <article className="definition-card">
              <StatusBadge label="Sessions awaiting trace" tone="warn" />
              <p>
                Session metadata exists, but there is no replayable rollout artifact yet. You only
                have thread identity and timestamps until a trace lands.
              </p>
            </article>
          </div>
          <div className="metric-grid">
            <MetricTile label="Imported runs" value={runs.total} />
            <MetricTile label="Ready traces" value={readySources.length} />
            <MetricTile label="Awaiting trace" value={awaitingSessions.length} />
            <MetricTile
              label="Last scan"
              value={status?.last_scan_at ? formatDate(status.last_scan_at) : "Not yet"}
            />
          </div>
          <div className="signal-note">
            <strong>Local-first:</strong> codex-replay reads Codex-managed files on this machine and
            copies artifacts into its own store so replay review survives source deletion.
          </div>
        </div>
      </section>

      <section className="surface-panel section-block">
        <SectionHeading
          eyebrow="Ready to replay"
          title="Artifacts that can be opened right now"
          meta={
            status?.warning ? <StatusBadge label={status.warning} tone="warn" /> : null
          }
        />
        {loading ? (
          <div className="loading-copy">Loading replayable artifacts…</div>
        ) : readySources.length === 0 ? (
          <EmptyPanel
            title="No replayable artifacts yet"
            copy="Once Codex writes a rollout JSONL under its managed session directories, it will appear here and can be opened immediately."
          />
        ) : (
          <div className="source-grid">
            {readySources.map((item) => (
              <article key={item.id} className="source-card">
                <div className="source-card-copy">
                  <div className="source-card-topline">
                    <h3>{compactArtifactName(item.artifact_name)}</h3>
                    <StatusBadge label={item.is_active ? "Live trace" : "Stable snapshot"} tone="good" />
                  </div>
                  <div className="card-metadata">
                    <span>{item.session_id ? truncateId(item.session_id) : "No session id"}</span>
                    <span>{formatDate(item.source_updated_at)}</span>
                  </div>
                  <p className="muted">
                    {item.is_active
                      ? "Codex is still writing this trace. Discovery refreshes keep the replay current."
                      : "Artifact is complete and ready for review."}
                  </p>
                </div>
                {item.run_id ? (
                  <Link href={`/runs/${item.run_id}`} className="button button-secondary">
                    Open replay
                  </Link>
                ) : (
                  <span className="inline-note">Import pending</span>
                )}
              </article>
            ))}
          </div>
        )}
      </section>

      <section className="surface-panel section-block">
        <SectionHeading
          eyebrow="Sessions awaiting trace"
          title="Session metadata that still needs a rollout file"
        />
        {loading ? (
          <div className="loading-copy">Loading session metadata…</div>
        ) : awaitingSessions.length === 0 ? (
          <EmptyPanel
            title="No metadata-only sessions"
            copy="Every discovered session currently has a matching replay artifact, or discovery has not indexed any sessions yet."
          />
        ) : (
          <div className="source-grid">
            {awaitingSessions.map((item) => (
              <article key={item.id} className="source-card">
                <div className="source-card-copy">
                  <div className="source-card-topline">
                    <h3>{normalizeDisplayTitle(item.thread_name ?? item.session_id, "Unnamed session")}</h3>
                    <StatusBadge label="Metadata only" tone="warn" />
                  </div>
                  <div className="card-metadata">
                    <span>{item.session_id ? truncateId(item.session_id) : "Session record"}</span>
                    <span>{formatDate(item.source_updated_at)}</span>
                  </div>
                  <p className="muted">
                    Thread identity is indexed, but there is no step-by-step replay yet. Wait for a
                    rollout file or import the JSONL manually.
                  </p>
                </div>
              </article>
            ))}
          </div>
        )}
      </section>

      <section className="surface-panel section-block">
        <SectionHeading
          eyebrow="Review queue"
          title="Latest imported runs"
          meta={
            <Link href="/runs" className="button button-ghost">
              View full queue
            </Link>
          }
        />
        {loading ? (
          <div className="loading-copy">Loading imported runs…</div>
        ) : runs.items.length === 0 ? (
          <EmptyPanel
            title="No imported runs yet"
            copy="Upload a trace or use the local path importer to seed the review queue."
          />
        ) : (
          <div className="queue-grid">
            {runs.items.map((run) => (
              <Link href={`/runs/${run.id}`} key={run.id} className="queue-card">
                <div className="card-badges">
                  <StatusBadge
                    label={run.state_label ?? "Ready replay"}
                    tone={toneForState(run.state_key, run.status)}
                  />
                  <StatusBadge
                    label={`Review priority ${run.review_attention ?? "low"}`}
                    tone={toneForAttention(run.review_attention)}
                  />
                </div>
                <h2>{runDisplayTitle(run)}</h2>
                <p className="card-note">{runDisplayNote(run)}</p>
                <div className="card-metadata">
                  <span>{run.repo_name ?? compactPath(run.repo_root)}</span>
                  <span>{formatDate(run.started_at)}</span>
                  <span>{run.total_files_changed} files</span>
                  <span>{run.total_tests} tests</span>
                  <span>{run.total_errors} errors</span>
                </div>
                <div className="card-footnote">{attentionCopy(run.review_attention)}</div>
              </Link>
            ))}
          </div>
        )}
      </section>
    </div>
  );
}

export function RunCatalogPage() {
  const [runs, setRuns] = useState<RunListData>({ items: [], total: 0 });
  const [query, setQuery] = useState("");
  const deferredQuery = useDeferredValue(query);
  const [provider, setProvider] = useState("codex");
  const [state, setState] = useState("all");
  const [sort, setSort] = useState("review_attention_desc");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const controller = new AbortController();

    async function loadRuns() {
      try {
        setLoading(true);
        setError(null);
        const data = await apiGet<RunListData>(
          buildRunsPath({
            query: deferredQuery,
            provider,
            state,
            sort,
          }),
        );
        if (!controller.signal.aborted) {
          setRuns(data);
        }
      } catch (loadError) {
        if (!controller.signal.aborted) {
          setError(loadError instanceof Error ? loadError.message : "Failed to load runs.");
        }
      } finally {
        if (!controller.signal.aborted) {
          setLoading(false);
        }
      }
    }

    void loadRuns();

    return () => controller.abort();
  }, [deferredQuery, provider, sort, state]);

  return (
    <div className="page-shell page-enter">
      <div className="page-heading">
        <div>
          <div className="eyebrow">Replay queue</div>
          <h1>Review imported Codex runs in order of risk.</h1>
          <p>
            Search the queue, filter by provider and replay state, and sort by priority or time.
            Codex is the only provider enabled in v0.1.
          </p>
        </div>
        <Link href="/" className="button button-ghost">
          Home
        </Link>
      </div>

      <section className="surface-panel section-block">
        <div className="toolbar-grid">
          <label className="control-field control-search">
            <span>Search</span>
            <input
              type="search"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Task, repo, source name, or review note"
              aria-label="Search imported runs"
            />
          </label>

          <label className="control-field">
            <span>Sort</span>
            <select value={sort} onChange={(event) => setSort(event.target.value)}>
              {RUN_SORT_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label className="control-field">
            <span>Provider</span>
            <select value={provider} onChange={(event) => setProvider(event.target.value)}>
              {PROVIDER_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label className="control-field">
            <span>Status</span>
            <select value={state} onChange={(event) => setState(event.target.value)}>
              {STATE_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>
        </div>

        <PriorityLegend />
      </section>

      {error ? <div className="error-banner">{error}</div> : null}

      <section className="surface-panel section-block">
        {loading ? (
          <div className="loading-copy">Loading replay queue…</div>
        ) : runs.items.length === 0 ? (
          <EmptyPanel
            title="No runs match the current queue filters"
            copy="Widen the search, change the state filter, or import another Codex trace."
          />
        ) : (
          <>
            <div className="queue-meta">
              <strong>{runs.total}</strong>
              <span>runs in the current queue view</span>
            </div>
            <div className="queue-grid">
              {runs.items.map((run) => (
                <Link href={`/runs/${run.id}`} key={run.id} className="queue-card">
                  <div className="card-badges">
                    <StatusBadge
                      label={run.state_label ?? "Ready replay"}
                      tone={toneForState(run.state_key, run.status)}
                    />
                    <StatusBadge
                      label={`Review priority ${run.review_attention ?? "low"}`}
                      tone={toneForAttention(run.review_attention)}
                    />
                  </div>
                  <h2>{runDisplayTitle(run)}</h2>
                  <p className="card-note">{runDisplayNote(run)}</p>
                  <div className="card-metadata">
                    <span>{run.provider}</span>
                    <span>{run.repo_name ?? "Unknown repo"}</span>
                    <span>{formatDate(run.started_at)}</span>
                    <span>{run.total_events} visible steps</span>
                    <span>{run.total_files_changed} files</span>
                    <span>{run.total_tests} tests</span>
                    <span>{run.total_errors} errors</span>
                    <span>{formatDuration(run.duration_ms)}</span>
                  </div>
                  <div className="card-footnote">{attentionCopy(run.review_attention)}</div>
                </Link>
              ))}
            </div>
          </>
        )}
      </section>
    </div>
  );
}

export function ReplayInspector({ runId }: { runId: string }) {
  const [run, setRun] = useState<RunDetail | null>(null);
  const [timeline, setTimeline] = useState<TimelineItem[]>([]);
  const [summary, setSummary] = useState<SummaryData | null>(null);
  const [insights, setInsights] = useState<Insight[]>([]);
  const [skills, setSkills] = useState<Skill[]>([]);
  const [diffs, setDiffs] = useState<DiffListItem[]>([]);
  const [selectedEventId, setSelectedEventId] = useState<string | null>(null);
  const [selectedDetail, setSelectedDetail] = useState<EventDetailEnvelope | null>(null);
  const [selectedDiffEventId, setSelectedDiffEventId] = useState<string | null>(null);
  const [selectedDiff, setSelectedDiff] = useState<DiffDetail | null>(null);
  const [activeFilter, setActiveFilter] = useState<TimelineFilter>("all");
  const [timelineQuery, setTimelineQuery] = useState("");
  const deferredTimelineQuery = useDeferredValue(timelineQuery);
  const [timelineSort, setTimelineSort] = useState<(typeof LEDGER_SORT_OPTIONS)[number]["value"]>("oldest");
  const [activePane, setActivePane] = useState<(typeof DETAIL_TABS)[number]["value"]>("evidence");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const [diffLoading, setDiffLoading] = useState(false);
  const timelineListRef = useRef<HTMLDivElement | null>(null);
  const pendingScrollIdRef = useRef<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function loadRun() {
      try {
        setLoading(true);
        setError(null);
        const [runData, timelineData, summaryData, insightsData, skillsData, diffData] =
          await Promise.all([
            apiGet<RunDetail>(`/runs/${runId}`),
            apiGet<{ items: TimelineItem[] }>(`/runs/${runId}/timeline`),
            apiGet<SummaryData>(`/runs/${runId}/summary`),
            apiGet<{ items: Insight[] }>(`/runs/${runId}/insights`),
            apiGet<{ items: Skill[] }>(`/runs/${runId}/skills`),
            apiGet<{ items: DiffListItem[] }>(`/runs/${runId}/diffs?limit=200`),
          ]);
        if (cancelled) {
          return;
        }
        setRun(runData);
        setTimeline(timelineData.items);
        setSummary(summaryData);
        setInsights(insightsData.items);
        setSkills(skillsData.items);
        setDiffs(diffData.items);
        setSelectedEventId(pickInitialTimelineEventId(timelineData.items));
        setSelectedDiffEventId(diffData.items[0]?.event_id ?? null);
      } catch (loadError) {
        if (!cancelled) {
          setError(loadError instanceof Error ? loadError.message : "Failed to load run.");
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    void loadRun();

    return () => {
      cancelled = true;
    };
  }, [runId]);

  const orderedTimeline = useMemo(() => {
    const items = [...timeline];
    items.sort((left, right) =>
      timelineSort === "oldest" ? left.seq - right.seq : right.seq - left.seq,
    );
    return items;
  }, [timeline, timelineSort]);

  const filteredTimeline = useMemo(() => {
    const search = deferredTimelineQuery.trim().toLowerCase();
    return orderedTimeline.filter((item) => {
      if (!matchesTimelineFilter(item, activeFilter)) {
        return false;
      }
      if (!search) {
        return true;
      }
      const label = normalizeTimelineLabel(item.label, item.event_type).toLowerCase();
      return (
        label.includes(search) ||
        humanizeEventType(item.event_type).toLowerCase().includes(search)
      );
    });
  }, [activeFilter, deferredTimelineQuery, orderedTimeline]);

  const selectedTimelineItem = useMemo(
    () => timeline.find((item) => item.event_id === selectedEventId) ?? null,
    [selectedEventId, timeline],
  );

  const filterCounts = useMemo(() => {
    return TIMELINE_FILTERS.reduce<Record<TimelineFilter, number>>(
      (accumulator, filter) => {
        accumulator[filter.key] = timeline.filter((item) =>
          matchesTimelineFilter(item, filter.key),
        ).length;
        return accumulator;
      },
      {
        all: 0,
        errors: 0,
        commands: 0,
        diffs: 0,
        tests: 0,
        skills: 0,
      },
    );
  }, [timeline]);

  const displayTitle = run ? runDisplayTitle(run) : runId;
  const validationTrail = useMemo(
    () => summarizeDelimitedText(summary?.json.validation_summary ?? run?.validation_summary),
    [run?.validation_summary, summary?.json.validation_summary],
  );
  const changedFileList = useMemo(
    () =>
      Array.from(
        new Set(diffs.map((item) => compactPath(item.file_path, run?.repo_root))),
      ).slice(0, 8),
    [diffs, run?.repo_root],
  );
  const selectedLead = useMemo(
    () =>
      selectedDetail?.message_text
        ? previewMessageText(selectedDetail.message_text, 260)
        : null,
    [selectedDetail?.message_text],
  );
  const selectedLeadSuppressed = useMemo(
    () => {
      const messageText = selectedDetail?.message_text;
      if (!messageText) {
        return false;
      }
      if (isLowSignalMessage(messageText)) {
        return true;
      }
      const normalizedLabel = normalizeTimelineLabel(
        selectedDetail?.title ?? "",
        selectedDetail?.event_type ?? "message",
      );
      const isGenericNarrative =
        normalizedLabel === humanizeEventType(selectedDetail?.event_type ?? "message");
      const densePromptWall =
        messageText.length > 1600 ||
        messageText.split("\n").filter((line) => line.trim()).length > 24;
      return isGenericNarrative && densePromptWall;
    },
    [selectedDetail?.event_type, selectedDetail?.message_text, selectedDetail?.title],
  );
  const actionableDetailError = useMemo(() => {
    const detailError = selectedDetail?.detail.error as
      | { message?: string; error_code?: string }
      | undefined;
    if (!detailError || detailError.error_code === "unsupported_event") {
      return null;
    }
    return detailError;
  }, [selectedDetail?.detail.error]);

  useEffect(() => {
    if (!filteredTimeline.length) {
      setSelectedEventId(null);
      return;
    }
    if (!filteredTimeline.some((item) => item.event_id === selectedEventId)) {
      startTransition(() => {
        setSelectedEventId(pickInitialTimelineEventId(filteredTimeline));
      });
    }
  }, [filteredTimeline, selectedEventId]);

  useEffect(() => {
    if (!selectedEventId) {
      setSelectedDetail(null);
      return;
    }
    let cancelled = false;

    async function loadDetail() {
      try {
        setDetailLoading(true);
        const detail = await apiGet<EventDetailEnvelope>(`/runs/${runId}/events/${selectedEventId}`);
        if (!cancelled) {
          setSelectedDetail(detail);
        }
      } catch (loadError) {
        if (!cancelled) {
          setError(
            loadError instanceof Error ? loadError.message : "Failed to load event detail.",
          );
        }
      } finally {
        if (!cancelled) {
          setDetailLoading(false);
        }
      }
    }

    void loadDetail();

    return () => {
      cancelled = true;
    };
  }, [runId, selectedEventId]);

  useEffect(() => {
    if (!selectedDiffEventId) {
      setSelectedDiff(null);
      return;
    }
    let cancelled = false;

    async function loadDiff() {
      try {
        setDiffLoading(true);
        const detail = await apiGet<DiffDetail>(`/runs/${runId}/diffs/${selectedDiffEventId}`);
        if (!cancelled) {
          setSelectedDiff(detail);
        }
      } catch (loadError) {
        if (!cancelled) {
          setError(loadError instanceof Error ? loadError.message : "Failed to load diff detail.");
        }
      } finally {
        if (!cancelled) {
          setDiffLoading(false);
        }
      }
    }

    void loadDiff();

    return () => {
      cancelled = true;
    };
  }, [runId, selectedDiffEventId]);

  useEffect(() => {
    if (!selectedEventId || !timelineListRef.current) {
      return;
    }
    if (pendingScrollIdRef.current !== selectedEventId) {
      return;
    }
    const target = timelineListRef.current.querySelector<HTMLElement>(
      `[data-event-id="${selectedEventId}"]`,
    );
    target?.scrollIntoView({ block: "nearest" });
    pendingScrollIdRef.current = null;
  }, [filteredTimeline, selectedEventId]);

  function queueTimelineScroll(eventId: string) {
    pendingScrollIdRef.current = eventId;
  }

  function selectEvent(eventId: string) {
    startTransition(() => {
      setSelectedEventId(eventId);
      setActivePane("evidence");
    });
  }

  function jumpTo(filter: TimelineFilter, predicate: (item: TimelineItem) => boolean) {
    const target = timeline.find(predicate);
    if (!target) {
      return;
    }
    queueTimelineScroll(target.event_id);
    startTransition(() => {
      setTimelineSort("oldest");
      setActiveFilter(filter);
      setSelectedEventId(target.event_id);
      if (target.has_diff) {
        setSelectedDiffEventId(target.event_id);
        setActivePane("diffs");
      } else {
        setActivePane("evidence");
      }
    });
  }

  function handleTimelineKeyDown(event: KeyboardEvent<HTMLElement>) {
    if (!filteredTimeline.length) {
      return;
    }
    if (event.key !== "ArrowDown" && event.key !== "ArrowUp") {
      return;
    }
    event.preventDefault();
    const selectedIndex = filteredTimeline.findIndex((item) => item.event_id === selectedEventId);
    const nextIndex =
      event.key === "ArrowDown"
        ? Math.min(selectedIndex + 1, filteredTimeline.length - 1)
        : Math.max(selectedIndex - 1, 0);
    const nextEvent = filteredTimeline[nextIndex];
    if (!nextEvent) {
      return;
    }
    startTransition(() => {
      setSelectedEventId(nextEvent.event_id);
      setActivePane("evidence");
    });
  }

  async function exportRun() {
    try {
      const data = await apiPost<{ download_url: string }>(`/runs/${runId}/exports`, {
        format: "bundle",
        include_raw_artifacts: true,
      });
      window.open(`${API_BASE}${data.download_url}`, "_blank", "noopener,noreferrer");
    } catch (exportError) {
      setError(exportError instanceof Error ? exportError.message : "Export failed.");
    }
  }

  return (
    <div className="page-shell replay-enter">
      <div className="page-heading">
        <div>
          <div className="eyebrow">Replay workspace</div>
          <h1>{displayTitle}</h1>
          <p>
            {run?.state_label ?? "Replay"} • {run?.repo_name ?? compactPath(run?.repo_root)} •{" "}
            {run?.started_at ? formatDate(run.started_at) : "Unknown start time"} •{" "}
            {run?.source_name ? compactArtifactName(run.source_name) : "Local replay snapshot"}
          </p>
        </div>
        <div className="page-actions">
          <Link href="/" className="button button-ghost">
            Home
          </Link>
          <Link href="/runs" className="button button-secondary">
            Review queue
          </Link>
          <button type="button" className="button button-primary" onClick={() => void exportRun()}>
            Export bundle
          </button>
        </div>
      </div>

      {error ? <div className="error-banner">{error}</div> : null}

      {loading || !run ? (
        <div className="surface-panel loading-copy">Loading replay…</div>
      ) : (
        <>
          <section className="surface-panel inspector-overview">
            <div className="overview-badges">
              <StatusBadge
                label={run.state_label ?? "Ready replay"}
                tone={toneForState(run.state_key, run.status)}
              />
              <StatusBadge
                label={`Review priority ${run.review_attention ?? "low"}`}
                tone={toneForAttention(run.review_attention)}
              />
              <StatusBadge label={run.provider} tone="accent" />
            </div>
            <p className="overview-note">
              {summary?.json.reviewer_notes ??
                "Review the primary command path, the final patch, and the last validation step."}
            </p>
            <div className="metric-grid metric-grid-inline">
              <MetricTile label="Steps" value={run.counts.events} />
              <MetricTile label="Files" value={run.counts.files_changed} />
              <MetricTile label="Tests" value={run.counts.tests} />
              <MetricTile label="Errors" value={run.counts.errors} />
            </div>
          </section>

          <div className="inspector-layout">
            <aside
              className="surface-panel inspector-pane inspector-pane-ledger"
              tabIndex={0}
              onKeyDown={handleTimelineKeyDown}
            >
              <div className="pane-head pane-head-compact">
                <SectionHeading
                  eyebrow="Event ledger"
                  title={`${filteredTimeline.length}/${timeline.length} visible steps`}
                />
                <div className="toolbar-grid toolbar-grid-compact">
                  <label className="control-field control-search">
                    <span>Find a step</span>
                    <input
                      type="search"
                      value={timelineQuery}
                      onChange={(event) => setTimelineQuery(event.target.value)}
                      placeholder="Search the ledger"
                      aria-label="Search the event ledger"
                    />
                  </label>
                  <label className="control-field">
                    <span>Order</span>
                    <select
                      value={timelineSort}
                      onChange={(event) =>
                        setTimelineSort(
                          event.target.value as (typeof LEDGER_SORT_OPTIONS)[number]["value"],
                        )
                      }
                    >
                      {LEDGER_SORT_OPTIONS.map((option) => (
                        <option key={option.value} value={option.value}>
                          {option.label}
                        </option>
                      ))}
                    </select>
                  </label>
                </div>
                <div className="filter-row filter-row-compact">
                  {TIMELINE_FILTERS.map((filter) => (
                    <FilterChip
                      key={filter.key}
                      label={filter.label}
                      active={activeFilter === filter.key}
                      count={filterCounts[filter.key]}
                      onClick={() => setActiveFilter(filter.key)}
                    />
                  ))}
                </div>
                <div className="jump-row jump-row-compact">
                  <button
                    type="button"
                    className="button button-ghost button-small"
                    onClick={() =>
                      jumpTo("errors", (item) => item.has_error || item.event_type === "error")
                    }
                    disabled={!filterCounts.errors}
                  >
                    First error
                  </button>
                  <button
                    type="button"
                    className="button button-ghost button-small"
                    onClick={() => jumpTo("tests", (item) => item.event_type === "test")}
                    disabled={!filterCounts.tests}
                  >
                    First validation
                  </button>
                  <button
                    type="button"
                    className="button button-ghost button-small"
                    onClick={() =>
                      jumpTo("diffs", (item) => item.has_diff || item.event_type === "diff")
                    }
                    disabled={!filterCounts.diffs}
                  >
                    First diff
                  </button>
                </div>
              </div>

              <div ref={timelineListRef} className="pane-scroll timeline-scroll">
                {filteredTimeline.length ? (
                  filteredTimeline.map((item) => (
                    <button
                      type="button"
                      key={item.event_id}
                      data-event-id={item.event_id}
                      className={classNames(
                        "timeline-row",
                        `timeline-tone-${timelineTone(item)}`,
                        selectedEventId === item.event_id && "timeline-row-active",
                      )}
                      onClick={() => selectEvent(item.event_id)}
                    >
                      <div className="timeline-step">{item.seq}</div>
                      <div className="timeline-main">
                        <div className="timeline-title">
                          {normalizeTimelineLabel(item.label, item.event_type)}
                        </div>
                        <div className="timeline-tags">
                          {timelineMarkers(item).map((marker) => (
                            <span key={`${item.event_id}-${marker}`} className="timeline-tag">
                              {marker}
                            </span>
                          ))}
                        </div>
                      </div>
                    </button>
                  ))
                ) : (
                  <EmptyPanel
                    title="No steps match the current ledger filters"
                    copy="Clear the search or switch filters to bring the step stream back into view."
                  />
                )}
              </div>
            </aside>

            <main className="surface-panel inspector-pane inspector-pane-evidence">
              <div className="pane-head">
                <SectionHeading
                  eyebrow="Evidence pane"
                  title={
                    selectedDetail
                      ? normalizeTimelineLabel(selectedDetail.title, selectedDetail.event_type)
                      : "Select a step"
                  }
                  meta={
                    selectedDetail ? (
                      <StatusBadge label={selectedDetail.status} tone={toneForStatus(selectedDetail.status)} />
                    ) : null
                  }
                />
                <div className="segment-control">
                  {DETAIL_TABS.map((tab) => (
                    <button
                      key={tab.value}
                      type="button"
                      className={classNames(
                        "segment-button",
                        activePane === tab.value && "segment-button-active",
                      )}
                      onClick={() => setActivePane(tab.value)}
                    >
                      {tab.label}
                    </button>
                  ))}
                </div>
              </div>

              <div className="pane-scroll evidence-scroll">
                {activePane === "evidence" ? (
                  detailLoading ? (
                    <div className="loading-copy">Loading event detail…</div>
                  ) : selectedDetail ? (
                    <div className="evidence-stack">
                      <section className="content-panel spotlight-panel">
                        <div className="content-metadata">
                          <span>Step {selectedDetail.seq}</span>
                          <span>{humanizeEventType(selectedDetail.event_type)}</span>
                          <span>{formatDate(selectedDetail.timestamp)}</span>
                        </div>
                        <div className="timeline-tags">
                          {(selectedTimelineItem
                            ? timelineMarkers(selectedTimelineItem)
                            : [humanizeEventType(selectedDetail.event_type)]
                          ).map((marker) => (
                            <span key={`selected-${marker}`} className="timeline-tag">
                              {marker}
                            </span>
                          ))}
                        </div>
                        {selectedLead && !selectedLeadSuppressed ? (
                          <p className="lead-text">{selectedLead}</p>
                        ) : null}
                        {selectedLeadSuppressed ? (
                          <p className="muted">
                            This first step is mostly setup or policy scaffolding. Open the full
                            note only if you need the raw session context.
                          </p>
                        ) : null}
                        {selectedDetail.message_text &&
                        ((!selectedLeadSuppressed && selectedLead && selectedLead !== selectedDetail.message_text) ||
                          selectedLeadSuppressed) ? (
                          <details className="fold-panel">
                            <summary>Open full step note</summary>
                            <pre className="evidence-pre">{selectedDetail.message_text}</pre>
                          </details>
                        ) : null}
                        {!selectedLead && !selectedLeadSuppressed ? (
                          <p className="muted">
                            Structured evidence is available below. Raw payload stays collapsed
                            unless you explicitly open it.
                          </p>
                        ) : null}
                      </section>

                      {selectedDetail.detail.command ? (
                        <section className="content-panel">
                          <h3>Command record</h3>
                          <code className="inline-command">
                            {selectedDetail.detail.command.command_text}
                          </code>
                          <div className="content-metadata">
                            <span>Exit {selectedDetail.detail.command.exit_code ?? "?"}</span>
                            <span>{formatDuration(selectedDetail.detail.command.duration_ms)}</span>
                          </div>
                          <EvidenceOutput
                            label="Captured stdout"
                            value={selectedDetail.detail.command.stdout_preview}
                            emptyCopy="No stdout was captured for this command."
                          />
                          <EvidenceOutput
                            label="Captured stderr"
                            value={selectedDetail.detail.command.stderr_preview}
                            emptyCopy="No stderr was captured for this command."
                          />
                        </section>
                      ) : null}

                      {selectedDetail.detail.test ? (
                        <section className="content-panel">
                          <h3>Validation record</h3>
                          <code className="inline-command">
                            {selectedDetail.detail.test.command_text}
                          </code>
                          <div className="content-metadata">
                            <span>Framework: {selectedDetail.detail.test.framework}</span>
                            <span>Result: {selectedDetail.detail.test.result}</span>
                            <span>
                              Passed {selectedDetail.detail.test.passed_count ?? 0} / Failed{" "}
                              {selectedDetail.detail.test.failed_count ?? 0}
                            </span>
                          </div>
                          <EvidenceOutput
                            label="Validation output"
                            value={selectedDetail.detail.test.stdout_preview}
                            emptyCopy="No validation output was captured."
                          />
                        </section>
                      ) : null}

                      {selectedDetail.detail.tool ? (
                        <section className="content-panel">
                          <h3>Tool call</h3>
                          <div className="content-metadata">
                            <span>{selectedDetail.detail.tool.tool_name}</span>
                            <span>{selectedDetail.detail.tool.tool_type}</span>
                          </div>
                          <EvidenceOutput
                            label="Tool input preview"
                            value={selectedDetail.detail.tool.input_preview}
                            emptyCopy="No tool input preview was captured."
                          />
                        </section>
                      ) : null}

                      {actionableDetailError ? (
                        <section className="content-panel content-panel-alert">
                          <h3>Error state</h3>
                          <p>{actionableDetailError.message ?? "An actionable error was recorded."}</p>
                        </section>
                      ) : null}

                      {!selectedDetail.detail.command &&
                      !selectedDetail.detail.test &&
                      !selectedDetail.detail.tool &&
                      !actionableDetailError ? (
                        <section className="content-panel">
                          <h3>Step evidence</h3>
                          <p className="muted">
                            This step contributes context to the replay but does not carry a
                            command, validation, tool call, or actionable error record.
                          </p>
                        </section>
                      ) : null}
                    </div>
                  ) : (
                    <EmptyPanel
                      title="Select a ledger row"
                      copy="The evidence pane stays focused on one step at a time so diff review and summary context remain readable."
                    />
                  )
                ) : null}

                {activePane === "diffs" ? (
                  diffs.length === 0 ? (
                    <EmptyPanel
                      title="No file diffs were captured"
                      copy="This replay has commands and notes, but it did not record any file-level patch evidence."
                    />
                  ) : (
                    <div className="diff-review-layout">
                      <div className="diff-list">
                        {diffs.map((diff) => (
                          <button
                            key={diff.event_id}
                            type="button"
                            className={classNames(
                              "diff-list-row",
                              selectedDiffEventId === diff.event_id && "diff-list-row-active",
                            )}
                            onClick={() => {
                              startTransition(() => {
                                setSelectedDiffEventId(diff.event_id);
                                setActivePane("diffs");
                              });
                            }}
                          >
                            <div className="timeline-step">{diff.seq}</div>
                            <div className="diff-list-copy">
                              <strong>{compactPath(diff.file_path, run.repo_root)}</strong>
                              <span>
                                {diff.change_type} • +{diff.lines_added} / -{diff.lines_removed}
                              </span>
                            </div>
                          </button>
                        ))}
                      </div>

                      <div className="diff-viewer">
                        {diffLoading ? (
                          <div className="loading-copy">Loading diff detail…</div>
                        ) : selectedDiff ? (
                          <section className="content-panel content-panel-fill">
                            <div className="content-metadata">
                              <span>{compactPath(selectedDiff.file_path, run.repo_root)}</span>
                              <span>{selectedDiff.change_type}</span>
                              <span>
                                +{selectedDiff.lines_added} / -{selectedDiff.lines_removed}
                              </span>
                            </div>
                            <pre className="diff-pre">{selectedDiff.diff_text}</pre>
                          </section>
                        ) : (
                          <EmptyPanel
                            title="Select a diff"
                            copy="Diff review is independent from the currently selected step so you can inspect file changes without losing your place in the ledger."
                          />
                        )}
                      </div>
                    </div>
                  )
                ) : null}

                {activePane === "raw" ? (
                  detailLoading ? (
                    <div className="loading-copy">Loading raw payload…</div>
                  ) : selectedDetail ? (
                    <section className="content-panel content-panel-fill">
                      <div className="content-metadata">
                        <span>Step {selectedDetail.seq}</span>
                        <span>{humanizeEventType(selectedDetail.event_type)}</span>
                      </div>
                      <pre className="raw-pre">
                        {JSON.stringify(selectedDetail.raw_payload ?? {}, null, 2)}
                      </pre>
                    </section>
                  ) : (
                    <EmptyPanel
                      title="No raw payload selected"
                      copy="Pick a step from the ledger first, then open the raw payload tab if you need the exact provider event."
                    />
                  )
                ) : null}
              </div>
            </main>

            <aside className="surface-panel inspector-pane inspector-pane-review">
              <div className="pane-head">
                <SectionHeading
                  eyebrow="Review rail"
                  title="Compact review brief"
                  meta={
                    <StatusBadge
                      label={`Review priority ${run.review_attention ?? "low"}`}
                      tone={toneForAttention(run.review_attention)}
                    />
                  }
                />
              </div>

              <div className="pane-scroll review-scroll">
                <section className="content-panel">
                  <h3>Next review pass</h3>
                  <div className="rail-list">
                    <div className="rail-row">
                      <span className="summary-dot" />
                      <span>{summary?.json.failure_summary ?? run.failure_summary}</span>
                    </div>
                    <div className="rail-row">
                      <span className="summary-dot" />
                      <span>
                        {summary?.json.reviewer_notes ??
                          "Review the primary command path, the final patch, and the last validation step."}
                      </span>
                    </div>
                    {run.first_error_seq ? (
                      <div className="rail-row">
                        <span className="summary-dot" />
                        <span>First actionable error appears at step {run.first_error_seq}.</span>
                      </div>
                    ) : null}
                    {run.counts.diagnostics ? (
                      <div className="rail-row">
                        <span className="summary-dot" />
                        <span>
                          {run.counts.diagnostics} parser diagnostic
                          {run.counts.diagnostics === 1 ? "" : "s"} hidden from the review ledger.
                        </span>
                      </div>
                    ) : null}
                  </div>
                </section>

                <section className="content-panel">
                  <h3>Validation</h3>
                  {validationTrail.length ? (
                    <div className="rail-list">
                      {validationTrail.map((item, index) => (
                        <div key={`${item}-${index}`} className="rail-row">
                          <span className="summary-dot" />
                          <span>{item}</span>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p className="muted">No validation commands were recorded.</p>
                  )}
                </section>

                <section className="content-panel">
                  <h3>Changed files</h3>
                  {changedFileList.length ? (
                    <div className="chip-grid">
                      {changedFileList.map((item) => (
                        <button
                          key={item}
                          type="button"
                          className="path-chip"
                          onClick={() => {
                            const target = diffs.find(
                              (diff) => compactPath(diff.file_path, run.repo_root) === item,
                            );
                            if (!target) {
                              return;
                            }
                            startTransition(() => {
                              setSelectedDiffEventId(target.event_id);
                              setActivePane("diffs");
                            });
                          }}
                        >
                          {item}
                        </button>
                      ))}
                    </div>
                  ) : (
                    <p className="muted">No file diffs were captured.</p>
                  )}
                </section>

                <section className="content-panel">
                  <h3>Insights</h3>
                  {insights.length ? (
                    <div className="compact-stack">
                      {insights.map((insight) => (
                        <article key={insight.id} className="insight-card">
                          <div className="card-badges">
                            <StatusBadge
                              label={insight.severity}
                              tone={
                                insight.severity === "high" || insight.severity === "critical"
                                  ? "bad"
                                  : insight.severity === "medium"
                                    ? "warn"
                                    : "accent"
                              }
                            />
                            <span className="inline-note">{insight.code}</span>
                          </div>
                          <h4>{insight.title}</h4>
                          <p>{insight.message}</p>
                        </article>
                      ))}
                    </div>
                  ) : (
                    <p className="muted">No deterministic insights were generated for this replay.</p>
                  )}
                </section>

                <section className="content-panel">
                  <h3>Skill context</h3>
                  {skills.length ? (
                    <div className="compact-stack">
                      {skills.map((skill) => (
                        <div key={skill.id} className="skill-card">
                          <strong>{skill.name}</strong>
                          <span>
                            {skill.mode} • {Math.round(skill.confidence * 100)}%
                          </span>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p className="muted">No skill signals were detected for this run.</p>
                  )}
                </section>

                <details className="fold-panel">
                  <summary>Open session digest</summary>
                  <SummaryDigest markdown={summary?.markdown ?? run.summary_markdown ?? ""} />
                </details>
              </div>
            </aside>
          </div>
        </>
      )}
    </div>
  );
}

"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import {
  startTransition,
  useCallback,
  useDeferredValue,
  useEffect,
  useMemo,
  useRef,
  useState,
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
  status: string;
  parse_status: string;
  started_at?: string | null;
  ended_at?: string | null;
  duration_ms?: number | null;
  total_events: number;
  total_files_changed: number;
  total_commands: number;
  total_errors: number;
  review_attention?: string;
};

type DiscoveryData = {
  items: DiscoveryItem[];
  total: number;
};

type RunListData = {
  items: RunItem[];
  total: number;
};

type DiscoveryStatus = {
  last_scan_at?: string | null;
  in_progress: boolean;
  warning?: string | null;
  counts: Record<string, number>;
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

type RunDetail = {
  id: string;
  provider: string;
  repo_name?: string | null;
  repo_root?: string | null;
  session_title?: string | null;
  source_name?: string | null;
  prompt?: string | null;
  task_summary?: string | null;
  status: string;
  parse_status: string;
  review_attention: string;
  validation_summary?: string | null;
  changed_files_summary?: string | null;
  failure_summary?: string | null;
  reviewer_notes?: string | null;
  started_at?: string | null;
  ended_at?: string | null;
  duration_ms?: number | null;
  first_error_seq?: number | null;
  counts: {
    events: number;
    commands: number;
    tests: number;
    errors: number;
    files_changed: number;
  };
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

const TIMELINE_FILTERS: Array<{ key: TimelineFilter; label: string }> = [
  { key: "all", label: "All" },
  { key: "errors", label: "Errors" },
  { key: "commands", label: "Commands" },
  { key: "diffs", label: "Diffs" },
  { key: "tests", label: "Tests" },
  { key: "skills", label: "Skills" },
];

function classNames(...values: Array<string | false | null | undefined>) {
  return values.filter(Boolean).join(" ");
}

function toneForStatus(status: string) {
  if (status === "completed" || status === "ready" || status === "ok") {
    return "good" as const;
  }
  if (status === "partial") {
    return "accent" as const;
  }
  if (status === "failed" || status === "error") {
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

function timelineMarkers(item: TimelineItem) {
  const markers = [humanizeEventType(item.event_type)];
  if (item.has_error) {
    markers.push("error");
  }
  if (item.has_diff) {
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
    <div className="section-header compact">
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
    <div className="rail-stat">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
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
              <span className="summary-bullet" />
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

export function LandingPage() {
  const router = useRouter();
  const [discovery, setDiscovery] = useState<DiscoveryData>({ items: [], total: 0 });
  const [runs, setRuns] = useState<RunListData>({ items: [], total: 0 });
  const [status, setStatus] = useState<DiscoveryStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);
      const [discoveryData, runsData, statusData] = await Promise.all([
        apiGet<DiscoveryData>("/discovery/sources?limit=24"),
        apiGet<RunListData>("/runs?limit=12"),
        apiGet<DiscoveryStatus>("/discovery/status"),
      ]);
      setDiscovery(discoveryData);
      setRuns(runsData);
      setStatus(statusData);
    } catch (loadError) {
      setError(loadError instanceof Error ? loadError.message : "Failed to load replay data.");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const importable = useMemo(
    () =>
      discovery.items.filter(
        (item) => item.source_kind === "codex_trace" || item.source_kind === "codex_archive",
      ),
    [discovery.items],
  );
  const sessions = useMemo(
    () => discovery.items.filter((item) => item.source_kind === "codex_session"),
    [discovery.items],
  );

  async function refreshDiscovery() {
    try {
      setBusy(true);
      await apiPost("/discovery/scan");
      await load();
    } catch (refreshError) {
      setError(refreshError instanceof Error ? refreshError.message : "Discovery refresh failed.");
    } finally {
      setBusy(false);
    }
  }

  async function handleUpload(event: React.ChangeEvent<HTMLInputElement>) {
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

  return (
    <div className="page-shell page-enter">
      <section className="hero-panel hero-panel-ledger">
        <div className="hero-copy">
          <div className="eyebrow">codex-replay v0.1</div>
          <h1>Agent replay as an evidence board, not a log dump.</h1>
          <p>
            Import a trace or open a recent Codex rollout from this machine,
            then inspect commands, diffs, tests, and reviewer signals without
            digging through raw JSONL.
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
              Import trace
            </label>
            <button
              type="button"
              className="button button-secondary"
              onClick={() => void refreshDiscovery()}
              disabled={busy}
            >
              {busy ? "Refreshing..." : "Refresh local traces"}
            </button>
            <Link href="/runs" className="button button-ghost">
              Browse imported runs
            </Link>
          </div>
        </div>
        <div className="hero-board">
          <div className="hero-strip">
            <span>Local only</span>
            <span>Codex managed paths</span>
            <span>Auto-import enabled</span>
          </div>
          <div className="hero-metrics">
            <div className="metric-card">
              <span>Imported runs</span>
              <strong>{runs.total}</strong>
            </div>
            <div className="metric-card">
              <span>Discovered traces</span>
              <strong>{importable.length}</strong>
            </div>
            <div className="metric-card">
              <span>Recent sessions</span>
              <strong>{sessions.length}</strong>
            </div>
            <div className="metric-card">
              <span>Last scan</span>
              <strong>{status?.last_scan_at ? formatDate(status.last_scan_at) : "Not yet"}</strong>
            </div>
          </div>
        </div>
      </section>

      <section className="callout-panel signal-bar">
        <div>
          <strong>Local-only ingestion.</strong> Discovery reads Codex-managed
          paths under <code>~/.codex</code> and copies replay artifacts into the
          app store so review survives source-file deletion.
        </div>
        <div>
          Missing traces? Run <code>codex exec --json "..." &gt; run.jsonl</code>
          and import the file directly.
        </div>
      </section>

      {error ? <div className="error-banner">{error}</div> : null}

      <div className="landing-grid">
        <section className="surface-panel board-panel">
          <SectionHeading
            eyebrow="Replayable traces"
            title="Recent Codex traces"
            meta={
              status?.warning ? (
                <StatusBadge label={status.warning} tone="warn" />
              ) : null
            }
          />
          {loading ? (
            <div className="loading-copy">Loading local rollout catalog...</div>
          ) : importable.length === 0 ? (
            <EmptyPanel
              title="No local rollout traces yet"
              copy="Once Codex writes rollout JSONL files under ~/.codex/sessions or ~/.codex/archived_sessions, they will appear here automatically."
            />
          ) : (
            <div className="stack-list">
              {importable.map((item) => (
                <article key={item.id} className="source-card">
                  <div className="source-main">
                    <div className="source-title-row">
                      <h3>{compactArtifactName(item.artifact_name)}</h3>
                      <StatusBadge label={item.status} tone={toneForStatus(item.status)} />
                    </div>
                    <div className="card-meta-grid">
                      <span>{item.session_id ? truncateId(item.session_id) : "No session id"}</span>
                      <span>{formatDate(item.source_updated_at)}</span>
                    </div>
                    <p className="muted">
                      {item.warning
                        ? item.warning
                        : item.is_active
                          ? "Active trace. Replay updates on background scans."
                          : "Stable artifact snapshot ready for review."}
                    </p>
                  </div>
                  <div className="source-actions">
                    {item.run_id ? (
                      <Link href={`/runs/${item.run_id}`} className="button button-secondary">
                        Open replay
                      </Link>
                    ) : (
                      <span className="disabled-copy">Import pending</span>
                    )}
                  </div>
                </article>
              ))}
            </div>
          )}
        </section>

        <section className="surface-panel board-panel">
          <SectionHeading eyebrow="Metadata only" title="Recent Codex sessions" />
          {loading ? (
            <div className="loading-copy">Loading recent session index...</div>
          ) : sessions.length === 0 ? (
            <EmptyPanel
              title="No recent sessions available"
              copy="codex-replay reads ~/.codex/session_index.jsonl to surface recent threads even before a replay artifact exists."
            />
          ) : (
            <div className="stack-list">
              {sessions.map((item) => (
                <article key={item.id} className="session-card">
                  <div>
                    <h3>{normalizeDisplayTitle(item.thread_name ?? item.session_id, "Unnamed session")}</h3>
                    <div className="card-meta-grid">
                      <span>{item.session_id ? truncateId(item.session_id) : "Session metadata"}</span>
                      <span>{formatDate(item.source_updated_at)}</span>
                    </div>
                    <p className="muted">
                      {item.run_id
                        ? "Replay artifact matched and ready."
                        : "Replay not yet available. Wait for a rollout file or import a JSONL manually."}
                    </p>
                  </div>
                  {item.run_id ? (
                    <Link href={`/runs/${item.run_id}`} className="button button-ghost">
                      Open replay
                    </Link>
                  ) : (
                    <span className="disabled-copy">Metadata only</span>
                  )}
                </article>
              ))}
            </div>
          )}
        </section>
      </div>

      <section className="surface-panel board-panel">
        <SectionHeading
          eyebrow="Imported catalog"
          title="Latest replay snapshots"
          meta={
            <Link href="/runs" className="button button-ghost">
              View all runs
            </Link>
          }
        />
        {loading ? (
          <div className="loading-copy">Loading imported runs...</div>
        ) : runs.items.length === 0 ? (
          <EmptyPanel
            title="No imported runs yet"
            copy="Use Import trace or wait for discovery to auto-import a Codex rollout."
          />
        ) : (
          <div className="catalog-grid catalog-grid-ledger">
            {runs.items.map((run) => (
              <Link href={`/runs/${run.id}`} key={run.id} className="catalog-card ledger-card">
                <div className="catalog-topline">
                  <div className="catalog-badges">
                    <StatusBadge label={run.status} tone={toneForStatus(run.status)} />
                    <StatusBadge
                      label={run.review_attention ?? "low"}
                      tone={toneForAttention(run.review_attention)}
                    />
                  </div>
                  <span>{run.repo_name ?? run.provider}</span>
                </div>
                <h2>{runDisplayTitle(run)}</h2>
                <p className="catalog-note">{runDisplayNote(run)}</p>
                <div className="catalog-stats">
                  <span>{formatDate(run.started_at)}</span>
                  <span>{run.total_files_changed} files</span>
                  <span>{run.total_commands} commands</span>
                  <span>{run.total_errors} errors</span>
                </div>
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
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    async function loadRuns() {
      try {
        setLoading(true);
        setError(null);
        const path = deferredQuery
          ? `/runs?limit=100&q=${encodeURIComponent(deferredQuery)}`
          : "/runs?limit=100";
        const data = await apiGet<RunListData>(path);
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
  }, [deferredQuery]);

  return (
    <div className="page-shell page-enter">
      <div className="page-heading">
        <div>
          <div className="eyebrow">Imported runs</div>
          <h1>Replay catalog</h1>
          <p>Browse every persisted replay snapshot, including discovery imports and manual uploads.</p>
        </div>
        <Link href="/" className="button button-ghost">
          Back to landing
        </Link>
      </div>

      <div className="search-row">
        <input
          type="search"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Search by task, repo, or source name"
          aria-label="Search imported runs"
        />
      </div>

      {error ? <div className="error-banner">{error}</div> : null}

      <section className="surface-panel board-panel">
        {loading ? (
          <div className="loading-copy">Loading replay catalog...</div>
        ) : (
          <div className="catalog-grid catalog-grid-ledger">
            {runs.items.map((run) => (
              <Link href={`/runs/${run.id}`} key={run.id} className="catalog-card ledger-card">
                <div className="catalog-topline">
                  <div className="catalog-badges">
                    <StatusBadge label={run.status} tone={toneForStatus(run.status)} />
                    <StatusBadge
                      label={run.review_attention ?? "low"}
                      tone={toneForAttention(run.review_attention)}
                    />
                  </div>
                  <span>{run.provider}</span>
                </div>
                <h2>{runDisplayTitle(run)}</h2>
                <p className="catalog-note">{runDisplayNote(run)}</p>
                <div className="catalog-stats">
                  <span>{run.repo_name ?? "Unknown repo"}</span>
                  <span>{run.total_events} events</span>
                  <span>{run.total_files_changed} files</span>
                  <span>{formatDuration(run.duration_ms)}</span>
                </div>
              </Link>
            ))}
          </div>
        )}
      </section>
    </div>
  );
}

export function ReplayInspector({ runId }: { runId: string }) {
  const [run, setRun] = useState<RunDetail | null>(null);
  const [timeline, setTimeline] = useState<TimelineItem[]>([]);
  const [summary, setSummary] = useState<{ markdown: string; status: string } | null>(null);
  const [insights, setInsights] = useState<Insight[]>([]);
  const [skills, setSkills] = useState<Skill[]>([]);
  const [selectedEventId, setSelectedEventId] = useState<string | null>(null);
  const [selectedDetail, setSelectedDetail] = useState<EventDetailEnvelope | null>(null);
  const [activeFilter, setActiveFilter] = useState<TimelineFilter>("all");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const timelineListRef = useRef<HTMLDivElement | null>(null);

  const filteredTimeline = useMemo(
    () => timeline.filter((item) => matchesTimelineFilter(item, activeFilter)),
    [activeFilter, timeline],
  );

  const selectedIndex = useMemo(
    () => filteredTimeline.findIndex((item) => item.event_id === selectedEventId),
    [filteredTimeline, selectedEventId],
  );

  const selectedTimelineItem = useMemo(
    () => timeline.find((item) => item.event_id === selectedEventId) ?? null,
    [selectedEventId, timeline],
  );

  const displayTitle = run
    ? run.session_title ||
      normalizeDisplayTitle(
        run.task_summary ?? run.prompt ?? run.source_name,
        run.source_name ? compactArtifactName(run.source_name) : run.repo_name ?? run.id,
      )
    : runId;

  const changedFiles = useMemo(
    () =>
      summarizeDelimitedText(run?.changed_files_summary).map((item) =>
        compactPath(item, run?.repo_root),
      ),
    [run],
  );

  const validationTrail = useMemo(
    () => summarizeDelimitedText(run?.validation_summary),
    [run],
  );

  const displayedSkills = useMemo(() => skills.slice(0, 4), [skills]);
  const selectedLead = useMemo(
    () => (selectedDetail?.message_text ? previewMessageText(selectedDetail.message_text) : null),
    [selectedDetail],
  );
  const hasCollapsedLead = Boolean(
    selectedDetail?.message_text && selectedLead && selectedLead !== selectedDetail.message_text,
  );

  useEffect(() => {
    let cancelled = false;
    async function loadRun() {
      try {
        setLoading(true);
        setError(null);
        const [runData, timelineData, summaryData, insightsData, skillsData] = await Promise.all([
          apiGet<RunDetail>(`/runs/${runId}`),
          apiGet<{ items: TimelineItem[] }>(`/runs/${runId}/timeline`),
          apiGet<{ markdown: string; status: string }>(`/runs/${runId}/summary`),
          apiGet<{ items: Insight[] }>(`/runs/${runId}/insights`),
          apiGet<{ items: Skill[] }>(`/runs/${runId}/skills`),
        ]);
        if (cancelled) {
          return;
        }
        setRun(runData);
        setTimeline(timelineData.items);
        setSummary(summaryData);
        setInsights(insightsData.items);
        setSkills(skillsData.items);
        setSelectedEventId(pickInitialTimelineEventId(timelineData.items));
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
          setError(loadError instanceof Error ? loadError.message : "Failed to load event detail.");
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
    if (!selectedEventId || !timelineListRef.current) {
      return;
    }
    const target = timelineListRef.current.querySelector<HTMLElement>(
      `[data-event-id="${selectedEventId}"]`,
    );
    target?.scrollIntoView({ block: "center" });
  }, [filteredTimeline, selectedEventId]);

  function handleTimelineKeyDown(event: React.KeyboardEvent<HTMLElement>) {
    if (!filteredTimeline.length) {
      return;
    }
    if (event.key !== "ArrowDown" && event.key !== "ArrowUp") {
      return;
    }
    event.preventDefault();
    const nextIndex =
      event.key === "ArrowDown"
        ? Math.min(selectedIndex + 1, filteredTimeline.length - 1)
        : Math.max(selectedIndex - 1, 0);
    startTransition(() => {
      setSelectedEventId(filteredTimeline[nextIndex]?.event_id ?? selectedEventId);
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

  function jumpTo(filter: TimelineFilter, predicate: (item: TimelineItem) => boolean) {
    const target = timeline.find(predicate);
    if (!target) {
      return;
    }
    startTransition(() => {
      setActiveFilter(filter);
      setSelectedEventId(target.event_id);
    });
  }

  const filterCounts = useMemo(() => {
    return TIMELINE_FILTERS.reduce<Record<TimelineFilter, number>>((accumulator, filter) => {
      accumulator[filter.key] = timeline.filter((item) =>
        matchesTimelineFilter(item, filter.key),
      ).length;
      return accumulator;
    }, {
      all: 0,
      errors: 0,
      commands: 0,
      diffs: 0,
      tests: 0,
      skills: 0,
    });
  }, [timeline]);

  return (
    <div className="replay-page">
      <div className="page-shell replay-enter">
        <div className="page-heading">
          <div>
            <div className="eyebrow">Replay inspector</div>
            <h1>{displayTitle}</h1>
            <p>
              {run?.repo_name ?? compactPath(run?.repo_root)} •{" "}
              {run?.started_at ? formatDate(run.started_at) : "Unknown start time"} •{" "}
              {run?.source_name ? compactArtifactName(run.source_name) : "Local replay snapshot"}
            </p>
          </div>
          <div className="hero-actions">
            <Link href="/" className="button button-ghost">
              Landing
            </Link>
            <button type="button" className="button button-primary" onClick={() => void exportRun()}>
              Export bundle
            </button>
          </div>
        </div>

        {error ? <div className="error-banner">{error}</div> : null}

        {loading || !run ? (
          <div className="surface-panel loading-copy">Loading replay...</div>
        ) : (
          <div className="replay-layout">
            <aside
              className="surface-panel replay-timeline board-panel"
              tabIndex={0}
              onKeyDown={handleTimelineKeyDown}
            >
              <SectionHeading
                eyebrow="Event ledger"
                title={`${filteredTimeline.length}/${timeline.length} steps`}
                meta={<StatusBadge label={run.status} tone={toneForStatus(run.status)} />}
              />

              <div className="filter-row">
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

              <div className="jump-row">
                <button
                  type="button"
                  className="button button-ghost button-small"
                  onClick={() => jumpTo("errors", (item) => item.has_error || item.event_type === "error")}
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
                  onClick={() => jumpTo("diffs", (item) => item.has_diff || item.event_type === "diff")}
                  disabled={!filterCounts.diffs}
                >
                  First diff
                </button>
              </div>

              <div ref={timelineListRef} className="timeline-list">
                {filteredTimeline.map((item) => (
                  <button
                    type="button"
                    key={item.event_id}
                    data-event-id={item.event_id}
                    className={classNames(
                      "timeline-item",
                      `timeline-tone-${timelineTone(item)}`,
                      selectedEventId === item.event_id && "timeline-item-active",
                    )}
                    onClick={() =>
                      startTransition(() => {
                        setSelectedEventId(item.event_id);
                      })
                    }
                  >
                    <div className="timeline-index">{item.seq}</div>
                    <div className="timeline-copy">
                      <div className="timeline-label">
                        {normalizeTimelineLabel(item.label, item.event_type)}
                      </div>
                      <div className="timeline-meta">
                        {timelineMarkers(item).map((marker) => (
                          <span key={`${item.event_id}-${marker}`} className="timeline-token">
                            {marker}
                          </span>
                        ))}
                      </div>
                    </div>
                  </button>
                ))}
              </div>
            </aside>

            <main className="surface-panel replay-evidence board-panel">
              <SectionHeading
                eyebrow="Evidence stage"
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

              {detailLoading ? (
                <div className="loading-copy">Loading event detail...</div>
              ) : selectedDetail ? (
                <div className="detail-stack">
                  <section className="detail-block detail-overview">
                    <div className="detail-meta-row detail-meta-ledger">
                      <span>Step {selectedDetail.seq}</span>
                      <span>{humanizeEventType(selectedDetail.event_type)}</span>
                      <span>{formatDate(selectedDetail.timestamp)}</span>
                    </div>
                    <div className="detail-tag-row">
                      {selectedTimelineItem ? (
                        timelineMarkers(selectedTimelineItem).map((marker) => (
                          <span key={`selected-${marker}`} className="timeline-token">
                            {marker}
                          </span>
                        ))
                      ) : (
                        <span className="timeline-token">{humanizeEventType(selectedDetail.event_type)}</span>
                      )}
                    </div>
                    {selectedLead ? (
                      <>
                        <p className="detail-lead">{selectedLead}</p>
                        {hasCollapsedLead ? (
                          <details className="detail-block raw-disclosure compact-disclosure">
                            <summary>Open full note</summary>
                            <pre>{selectedDetail.message_text}</pre>
                          </details>
                        ) : null}
                      </>
                    ) : (
                      <p className="muted">
                        Structured evidence for this step is shown below. Raw payload stays collapsed
                        unless you explicitly open it.
                      </p>
                    )}
                  </section>

                  {selectedDetail.detail.command ? (
                    <section className="detail-block">
                      <h3>Command record</h3>
                      <code className="inline-command">
                        {selectedDetail.detail.command.command_text}
                      </code>
                      <div className="detail-metrics">
                        <span>Exit {selectedDetail.detail.command.exit_code ?? "?"}</span>
                        <span>{formatDuration(selectedDetail.detail.command.duration_ms)}</span>
                      </div>
                      <pre>{selectedDetail.detail.command.stdout_preview || "No stdout captured."}</pre>
                    </section>
                  ) : null}

                  {selectedDetail.detail.test ? (
                    <section className="detail-block">
                      <h3>Validation record</h3>
                      <code className="inline-command">
                        {selectedDetail.detail.test.command_text}
                      </code>
                      <div className="detail-metrics">
                        <span>Framework: {selectedDetail.detail.test.framework}</span>
                        <span>Result: {selectedDetail.detail.test.result}</span>
                        <span>
                          Passed: {selectedDetail.detail.test.passed_count ?? 0} / Failed:{" "}
                          {selectedDetail.detail.test.failed_count ?? 0}
                        </span>
                      </div>
                      <pre>{selectedDetail.detail.test.stdout_preview || "No stdout captured."}</pre>
                    </section>
                  ) : null}

                  {selectedDetail.detail.diff ? (
                    <section className="detail-block">
                      <h3>Diff record</h3>
                      <div className="detail-metrics">
                        <span>{compactPath(selectedDetail.detail.diff.file_path, run.repo_root)}</span>
                        <span>{selectedDetail.detail.diff.change_type}</span>
                        <span>
                          +{selectedDetail.detail.diff.lines_added} / -
                          {selectedDetail.detail.diff.lines_removed}
                        </span>
                      </div>
                      <pre className="diff-block">{selectedDetail.detail.diff.diff_text}</pre>
                    </section>
                  ) : null}

                  {selectedDetail.detail.tool ? (
                    <section className="detail-block">
                      <h3>Tool call</h3>
                      <div className="detail-metrics">
                        <span>{selectedDetail.detail.tool.tool_name}</span>
                        <span>{selectedDetail.detail.tool.tool_type}</span>
                      </div>
                      <pre>{selectedDetail.detail.tool.input_preview}</pre>
                    </section>
                  ) : null}

                  {selectedDetail.detail.error ? (
                    <section className="detail-block detail-block-alert">
                      <h3>Error state</h3>
                      <p>{selectedDetail.detail.error.message}</p>
                    </section>
                  ) : null}

                  <details className="detail-block raw-disclosure">
                    <summary>Raw payload JSON</summary>
                    <pre>{JSON.stringify(selectedDetail.raw_payload, null, 2)}</pre>
                  </details>
                </div>
              ) : (
                <EmptyPanel
                  title="Select a ledger row"
                  copy="codex-replay loads step detail lazily so long diffs and command outputs do not block the entire page."
                />
              )}
            </main>

            <aside className="surface-panel replay-rail board-panel">
              <SectionHeading
                eyebrow="Review rail"
                title="Case context"
                meta={
                  <StatusBadge
                    label={run.review_attention}
                    tone={toneForAttention(run.review_attention)}
                  />
                }
              />

              <div className="rail-group">
                <MetricTile label="Files changed" value={run.counts.files_changed} />
                <MetricTile label="Commands" value={run.counts.commands} />
                <MetricTile label="Tests" value={run.counts.tests} />
                <MetricTile label="Duration" value={formatDuration(run.duration_ms)} />
              </div>

              <section className="rail-block">
                <h3>Review focus</h3>
                <p>{run.reviewer_notes ?? "Focus on the first failure, the final patch, and the last validation step."}</p>
              </section>

              <section className="rail-block">
                <h3>Validation trail</h3>
                {validationTrail.length ? (
                  <div className="stack-list tight">
                    {validationTrail.map((item, index) => (
                      <div key={`validation-${index}`} className="rail-inline-row">
                        <span className="summary-bullet" />
                        <span>{item}</span>
                      </div>
                    ))}
                  </div>
                ) : (
                  <p className="muted">No validation summary available.</p>
                )}
              </section>

              <section className="rail-block">
                <h3>Changed files</h3>
                {changedFiles.length ? (
                  <div className="chip-grid">
                    {changedFiles.map((item, index) => (
                      <span key={`file-${index}`} className="path-chip">
                        {item}
                      </span>
                    ))}
                  </div>
                ) : (
                  <p className="muted">No file summary available.</p>
                )}
              </section>

              <section className="rail-block">
                <h3>Failure and recovery</h3>
                <p>{run.failure_summary ?? "No failure summary available."}</p>
              </section>

              <section className="rail-block">
                <h3>Session digest</h3>
                <SummaryDigest markdown={summary?.markdown ?? ""} />
              </section>

              <section className="rail-block">
                <h3>Insights</h3>
                <div className="stack-list tight">
                  {insights.length ? (
                    insights.map((insight) => (
                      <article key={insight.id} className="insight-card">
                        <div className="insight-topline">
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
                          <span>{insight.code}</span>
                        </div>
                        <h4>{insight.title}</h4>
                        <p>{insight.message}</p>
                        {insight.recommendation ? (
                          <p className="muted">{insight.recommendation}</p>
                        ) : null}
                      </article>
                    ))
                  ) : (
                    <p className="muted">No deterministic insights were generated for this run.</p>
                  )}
                </div>
              </section>

              <section className="rail-block">
                <h3>Skill context</h3>
                {displayedSkills.length ? (
                  <div className="stack-list tight">
                    {displayedSkills.map((skill) => (
                      <div key={skill.id} className="skill-chip">
                        <strong>{skill.name}</strong>
                        <span>
                          {skill.mode} • {Math.round(skill.confidence * 100)}%
                        </span>
                      </div>
                    ))}
                  </div>
                ) : (
                  <p className="muted">No skill signals were detected.</p>
                )}
              </section>
            </aside>
          </div>
        )}
      </div>
    </div>
  );
}

function truncateId(value: string) {
  return value.length > 12 ? `${value.slice(0, 8)}…${value.slice(-4)}` : value;
}

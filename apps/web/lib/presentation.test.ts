import { describe, expect, it } from "vitest";

import {
  compactArtifactName,
  compactPath,
  isLowSignalMessage,
  matchesTimelineFilter,
  normalizeDisplayTitle,
  normalizeTimelineLabel,
  pickInitialTimelineEventId,
  previewMessageText,
} from "./presentation";

describe("presentation helpers", () => {
  it("extracts a readable task title from AGENTS-heavy prompts", () => {
    const title = normalizeDisplayTitle(
      `# AGENTS.md instructions for /Users/kai/Desktop/codex-replay

<INSTRUCTIONS>
- [$brainstorming](/Users/kai/.agents/skills/brainstorming/SKILL.md)
</INSTRUCTIONS>
you need to continue the remaining tasks and improve the frontend replay UI`,
      "Fallback title",
    );

    expect(title).toBe("you need to continue the remaining tasks and improve the frontend replay UI");
  });

  it("prefers direct task requests over policy language", () => {
    const title = normalizeDisplayTitle(
      `Safety and fallback: If a skill can't be applied cleanly, state the issue and continue.
PLEASE IMPLEMENT THIS PLAN:
Improve the replay inspector and finish the MVP polish tasks.`,
      "Fallback title",
    );

    expect(title).toBe("Improve the replay inspector and finish the MVP polish tasks.");
  });

  it("ignores environment noise like timezone tokens when deriving titles", () => {
    const title = normalizeDisplayTitle(
      `# AGENTS.md instructions
Europe/Dublin
zsh
Improve responsive dashboard layout`,
      "Fallback title",
    );

    expect(title).toBe("Improve responsive dashboard layout");
  });

  it("normalizes noisy timeline labels", () => {
    const label = normalizeTimelineLabel(
      "<skill> <name>brainstorming</name> <path>/Users/kai/.agents/skills/brainstorming/SKILL.md</path>",
      "message",
    );

    expect(label).toBe("User note");
  });

  it("compacts rollout artifact filenames into readable catalog labels", () => {
    expect(
      compactArtifactName(
        "rollout-2026-03-14T18-12-45-019ced8c-fa45-7e33-bfe1-608953630fc1.jsonl",
      ),
    ).toBe("rollout 2026-03-14 18:12 • 019ced8c");
  });

  it("renders repo-relative paths when possible", () => {
    expect(
      compactPath(
        "/Users/kai/Desktop/codex-replay/apps/web/components/replay-app.tsx",
        "/Users/kai/Desktop/codex-replay",
      ),
    ).toBe("apps/web/components/replay-app.tsx");
  });

  it("matches timeline filters deterministically", () => {
    const item = {
      event_id: "evt_1",
      event_type: "command",
      label: "Run tests",
      has_diff: false,
      has_error: true,
      has_skill: false,
    };

    expect(matchesTimelineFilter(item, "errors")).toBe(true);
    expect(matchesTimelineFilter(item, "commands")).toBe(true);
    expect(matchesTimelineFilter(item, "tests")).toBe(false);
  });

  it("picks the first visible timeline event in chronological order", () => {
    const items = [
      {
        event_id: "evt_1",
        event_type: "run_started",
        label: "Run started",
        has_diff: false,
        has_error: false,
        has_skill: true,
      },
      {
        event_id: "evt_2",
        event_type: "message",
        label: "# AGENTS.md instructions for /Users/kai/Desktop/codex-replay",
        has_diff: false,
        has_error: false,
        has_skill: false,
      },
      {
        event_id: "evt_3",
        event_type: "command",
        label: "pnpm --filter web build",
        has_diff: false,
        has_error: false,
        has_skill: false,
      },
    ];

    expect(pickInitialTimelineEventId(items)).toBe("evt_1");
  });

  it("truncates long message previews without returning raw prompt walls", () => {
    const preview = previewMessageText(
      `# AGENTS.md instructions for /Users/kai/Desktop/codex-replay

<INSTRUCTIONS>
timezone
</INSTRUCTIONS>

Improve the replay inspector and continue the remaining MVP tasks.
${"x".repeat(1200)}`,
      120,
    );

    expect(preview).toContain("Improve the replay inspector");
    expect(preview.length).toBeLessThanOrEqual(120);
  });

  it("suppresses pure setup or policy boilerplate from message previews", () => {
    const preview = previewMessageText(
      `Filesystem sandboxing defines which files can be read or written.
Approval policy is currently never.
Do not provide the sandbox_permissions.
Network access is enabled.`,
      160,
    );

    expect(preview).toBe("");
  });

  it("drops setup noise but preserves the actual task request", () => {
    const preview = previewMessageText(
      `Filesystem sandboxing defines which files can be read or written.
Approval policy is currently never.
Improve the event ledger readability and remove mobile overflow on the review rail.`,
      160,
    );

    expect(preview).toBe(
      "Improve the event ledger readability and remove mobile overflow on the review rail.",
    );
  });

  it("suppresses desktop app context bullets from the spotlight preview", () => {
    const preview = previewMessageText(
      `- You are running inside the Codex (desktop) app, which allows some additional features.
- In the app, use markdown image syntax with absolute filesystem path references.
- This app supports recurring tasks and automations are stored as TOML.`,
      180,
    );

    expect(preview).toBe("");
  });

  it("flags instruction-heavy prompt walls as low-signal message content", () => {
    expect(
      isLowSignalMessage(`- Only use ::automation-update{...} when the user explicitly asks.
- For view directives, id is required.
- RRULE limitations apply.
- Prompting guidance should stay concise.`),
    ).toBe(true);
  });

  it("does not flag direct task bullets as low-signal", () => {
    expect(
      isLowSignalMessage(`- Improve the event ledger readability.
- Fix the mobile overflow on the review rail.
- Re-test the web app at 390px and 1280px.`),
    ).toBe(false);
  });
});

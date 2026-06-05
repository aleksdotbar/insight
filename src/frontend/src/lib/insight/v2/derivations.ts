import type { BulletMetric } from "@/types/insight";

export interface CompositionRow {
  name: string;
  value: number;
}

export interface CollabActivityRow {
  category: string;
  label: string;
  description: string;
  value: number;
  unit: string;
  higher_is_better: boolean;
  /** Raw catalog source tags of the constituent metrics, e.g. ["m365","zoom"]. */
  sources: string[];
}

const AI_TOOL_KEYS: ReadonlyArray<readonly [string, string]> = [
  ["cursor_lines", "Cursor"],
  ["cc_lines", "Claude Code"],
  ["codex_lines", "Codex"],
  ["copilot_lines", "Copilot"],
];

function rawValue(row: BulletMetric | undefined): number {
  if (!row) return 0;
  const n = Number(row.value);
  return Number.isFinite(n) ? n : 0;
}

function sumKeys(rows: BulletMetric[], keys: ReadonlyArray<string>): number {
  let total = 0;
  for (const r of rows) {
    if (keys.includes(r.metric_key)) total += rawValue(r);
  }
  return total;
}

/** Union of catalog source_tags across the rows matching `keys`. */
function collectSources(
  rows: BulletMetric[],
  keys: ReadonlyArray<string>,
): string[] {
  const set = new Set<string>();
  for (const r of rows) {
    if (keys.includes(r.metric_key)) {
      for (const t of r.source_tags ?? []) set.add(t);
    }
  }
  return [...set];
}

export function deriveAiToolComposition(
  aiRows: BulletMetric[],
): CompositionRow[] {
  const byKey = new Map(aiRows.map((r) => [r.metric_key, r]));
  const named: CompositionRow[] = [];
  let knownTotal = 0;
  for (const [key, name] of AI_TOOL_KEYS) {
    const v = rawValue(byKey.get(key));
    if (v > 0) named.push({ name, value: v });
    knownTotal += v;
  }
  const totalRow = byKey.get("team_ai_loc");
  const total = rawValue(totalRow);
  const other = Math.max(0, total - knownTotal);
  if (other > 0) named.push({ name: "Other", value: other });
  return named;
}

const COLLAB_META: Record<
  string,
  Pick<CollabActivityRow, "label" | "description" | "higher_is_better">
> = {
  meetings: {
    label: "Meetings",
    description: "Hours attended in scheduled meetings.",
    higher_is_better: false,
  },
  messages: {
    label: "Messages",
    description: "Chat messages, emails, and direct messages sent.",
    higher_is_better: true,
  },
  files: {
    label: "Files engaged",
    description: "Documents created or edited.",
    higher_is_better: true,
  },
};

const MEETING_KEYS = ["meeting_hours"] as const;
const MESSAGE_KEYS = [
  "slack_messages_sent",
  "m365_emails_sent",
  "m365_teams_chats",
] as const;
const FILE_KEYS = ["m365_files_engaged"] as const;

export function deriveCollabActivities(
  collabRows: BulletMetric[],
): CollabActivityRow[] {
  return [
    {
      category: "meetings",
      value: sumKeys(collabRows, MEETING_KEYS),
      unit: "h",
      sources: collectSources(collabRows, MEETING_KEYS),
      ...COLLAB_META.meetings,
    },
    {
      category: "messages",
      value: sumKeys(collabRows, MESSAGE_KEYS),
      unit: "count",
      sources: collectSources(collabRows, MESSAGE_KEYS),
      ...COLLAB_META.messages,
    },
    {
      category: "files",
      value: sumKeys(collabRows, FILE_KEYS),
      unit: "count",
      sources: collectSources(collabRows, FILE_KEYS),
      ...COLLAB_META.files,
    },
  ];
}

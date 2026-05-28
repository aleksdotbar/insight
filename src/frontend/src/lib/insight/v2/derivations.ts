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

export function deriveCollabActivities(
  collabRows: BulletMetric[],
): CollabActivityRow[] {
  const meetings = sumKeys(collabRows, ["meeting_hours"]);
  const messages = sumKeys(collabRows, [
    "slack_messages_sent",
    "m365_emails_sent",
    "m365_teams_chats",
  ]);
  const files = sumKeys(collabRows, ["m365_files_engaged"]);
  return [
    { category: "meetings", value: meetings, unit: "h", ...COLLAB_META.meetings },
    {
      category: "messages",
      value: messages,
      unit: "count",
      ...COLLAB_META.messages,
    },
    { category: "files", value: files, unit: "count", ...COLLAB_META.files },
  ];
}

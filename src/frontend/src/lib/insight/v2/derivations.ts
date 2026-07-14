import type { BulletMetric } from "@/types/insight";

export interface CompositionRow {
  name: string;
  value: number;
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

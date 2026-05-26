import { teamHealthStatus } from "@/api/metric-semantics";
import { TEAM_KPIS_BY_PERIOD, TEAM_VIEW_CONFIG } from "@/api/view-configs";
import type { PeriodValue, TeamKpi, TeamMember } from "@/types/insight";

function median(values: number[]): number | null {
  const nums = values.filter((v) => Number.isFinite(v));
  if (nums.length === 0) return null;
  const sorted = [...nums].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? (sorted[mid - 1]! + sorted[mid]!) / 2
    : sorted[mid]!;
}

export function deriveTeamKpis(
  members: TeamMember[],
  period: PeriodValue,
): TeamKpi[] {
  if (members.length === 0) return [];

  const total = members.length;
  const focusTrigger =
    TEAM_VIEW_CONFIG.alert_thresholds.find(
      (t) => t.metric_key === "focus_time_pct",
    )?.trigger ?? 60;

  const atRisk = members.filter((m) =>
    TEAM_VIEW_CONFIG.alert_thresholds.some((t) => {
      const v = m[t.metric_key as keyof TeamMember];
      return typeof v === "number" && Number.isFinite(v) && v < t.trigger;
    }),
  ).length;

  const membersWithFocus = members.filter(
    (m): m is TeamMember & { focus_time_pct: number } =>
      m.focus_time_pct !== null,
  );
  const focusCount = membersWithFocus.filter(
    (m) => m.focus_time_pct >= focusTrigger,
  ).length;
  const belowFocus = membersWithFocus.length - focusCount;
  const noAiCount = members.filter((m) => m.ai_tools.length === 0).length;

  const devTimeMedian = median(
    members
      .map((m) => m.dev_time_h)
      .filter((v): v is number => v !== null),
  );

  const atRiskStatus = teamHealthStatus(atRisk, total);
  const focusStatus = teamHealthStatus(belowFocus, total);
  const noAiStatus = teamHealthStatus(noAiCount, total);

  const templates = TEAM_KPIS_BY_PERIOD[period] ?? TEAM_KPIS_BY_PERIOD.month;
  return templates.map((k) => {
    if (k.metric_key === "at_risk_count")
      return { ...k, value: String(atRisk), status: atRiskStatus };
    if (k.metric_key === "focus_gte_60")
      return {
        ...k,
        value: `${focusCount} / ${total}`,
        sublabel: `${belowFocus} member${belowFocus !== 1 ? "s" : ""} below target`,
        status: focusStatus,
      };
    if (k.metric_key === "not_using_ai")
      return { ...k, value: String(noAiCount), status: noAiStatus };
    if (k.metric_key === "team_dev_time") {
      const value =
        devTimeMedian === null ? "—" : `${Math.round(devTimeMedian)}h`;
      return {
        ...k,
        value,
        sublabel: `Team median · ${total} member${total !== 1 ? "s" : ""}`,
        chipLabel: undefined,
      };
    }
    return k;
  });
}

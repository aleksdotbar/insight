import { useMemo } from "react";

import { teamHealthStatus } from "@/api/metric-semantics";
import { useTeamKpisByPeriod, useTeamViewConfig } from "@/api/view-configs";
import type { AlertThreshold, PeriodValue, TeamKpi, TeamMember } from "@/types/insight";

function median(values: number[]): number | null {
  const nums = values.filter((v) => Number.isFinite(v));
  if (nums.length === 0) return null;
  const sorted = [...nums].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? (sorted[mid - 1]! + sorted[mid]!) / 2
    : sorted[mid]!;
}

/**
 * Build the period-scoped overview KPIs ("At Risk", "Focus ≥60%", etc.)
 * from the loaded team members. Pure on inputs — exported so call sites
 * that already hold the resolved alert thresholds (tests, future
 * imperative call sites) can derive KPIs without spinning up a hook.
 */
export function deriveTeamKpis(
  members: TeamMember[],
  templates: TeamKpi[],
  alertThresholds: AlertThreshold[],
): TeamKpi[] {
  if (members.length === 0) return [];

  const total = members.length;
  const focusTrigger =
    alertThresholds.find((t) => t.metric_key === "focus_time_pct")?.trigger ??
    60;

  const atRisk = members.filter((m) =>
    alertThresholds.some((t) => {
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

/**
 * React hook variant — wires the catalog-resolved team view config and
 * the period-scoped templates into `deriveTeamKpis`. This is the entry
 * point screens should call; the pure `deriveTeamKpis` stays exported
 * for unit tests and direct callers.
 */
export function useTeamKpis(
  members: TeamMember[],
  period: PeriodValue,
): TeamKpi[] {
  const { alert_thresholds } = useTeamViewConfig();
  const templates = useTeamKpisByPeriod(period);
  return useMemo(
    () => deriveTeamKpis(members, templates, alert_thresholds),
    [members, templates, alert_thresholds],
  );
}

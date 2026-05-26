import { Card, CardContent } from "@/components/ui/card";
import { cn } from "@/lib/utils";
import type { ExecColumnThreshold, ExecTeamRow, OrgKpis } from "@/types/insight";

import { ComingSoon } from "./coming-soon";
import { MetricInfo } from "./metric-info";

export interface OrgKpiCardsProps {
  teams: ExecTeamRow[];
  orgKpis: OrgKpis | null;
  columnThresholds: ExecColumnThreshold[];
}

type KpiCardDef = {
  label: string;
  value: number | string | null;
  valueSuffix?: string;
  isGood: boolean | null;
  description: string;
};

function thresholdStatus(
  value: number,
  metricKey: string,
  thresholds: ExecColumnThreshold[],
): boolean | null {
  const t = thresholds.find((x) => x.metric_key === metricKey);
  return t ? value >= t.threshold : null;
}

function describeTarget(
  metricKey: string,
  thresholds: ExecColumnThreshold[],
  fallback: string,
): string {
  const t = thresholds.find((x) => x.metric_key === metricKey);
  return t ? `${fallback} Target ≥${t.threshold}%.` : fallback;
}

function KpiCard({ label, value, valueSuffix, isGood, description }: KpiCardDef) {
  const valueColor =
    isGood === true
      ? "text-success"
      : isGood === false
        ? "text-warning"
        : "text-foreground";
  return (
    <Card className="text-center">
      <CardContent className="p-4">
        {value === null ? (
          <div className="flex min-h-[2rem] items-center justify-center">
            <ComingSoon variant="chip" />
          </div>
        ) : (
          <div className={cn("text-2xl font-extrabold", valueColor)}>
            {value}
            {valueSuffix}
          </div>
        )}
        <div className="text-muted-foreground mt-1 flex items-center justify-center text-xs">
          {label}
          <MetricInfo description={description} side="bottom" />
        </div>
      </CardContent>
    </Card>
  );
}

export function OrgKpiCards({
  teams,
  orgKpis,
  columnThresholds,
}: OrgKpiCardsProps) {
  const teamsAtRisk = (teams ?? []).filter(
    (t) => t.status === "warn" || t.status === "bad",
  ).length;

  const build = orgKpis?.avgBuildSuccess ?? null;
  const ai = orgKpis?.avgAiAdoption ?? null;
  const focus = orgKpis?.avgFocus ?? null;

  const cards: KpiCardDef[] = [
    {
      label: "Teams at Risk",
      value: teamsAtRisk,
      isGood: teamsAtRisk === 0,
      description:
        "Teams with warn or bad status across key delivery and quality metrics.",
    },
    {
      label: "Avg Build Success",
      value: build,
      valueSuffix: "%",
      isGood:
        build === null
          ? null
          : thresholdStatus(build, "build_success_pct", columnThresholds),
      description:
        build === null
          ? "CI connector not configured — no build data to aggregate."
          : describeTarget(
              "build_success_pct",
              columnThresholds,
              "Average CI/CD build pass rate across all teams.",
            ),
    },
    {
      label: "Avg AI Adoption",
      value: ai,
      valueSuffix: "%",
      isGood:
        ai === null
          ? null
          : thresholdStatus(ai, "ai_adoption_pct", columnThresholds),
      description:
        ai === null
          ? "No AI-tool data available for any team this period."
          : describeTarget(
              "ai_adoption_pct",
              columnThresholds,
              "Average share of members actively using AI tools this period.",
            ),
    },
    {
      label: "Avg Focus Time",
      value: focus,
      valueSuffix: "%",
      isGood:
        focus === null
          ? null
          : thresholdStatus(focus, "focus_time_pct", columnThresholds),
      description:
        focus === null
          ? "No focus-time data available for any team this period."
          : describeTarget(
              "focus_time_pct",
              columnThresholds,
              "Average share of work time spent in uninterrupted 60-min+ blocks.",
            ),
    },
  ];

  return (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
      {cards.map((card) => (
        <KpiCard key={card.label} {...card} />
      ))}
    </div>
  );
}

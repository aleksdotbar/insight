import {
  PolarAngleAxis,
  PolarGrid,
  Radar,
  RadarChart,
  ResponsiveContainer,
  Tooltip,
} from "recharts";

import type { OrgKpis } from "@/types/insight";

import { CHART_BLUE, CHART_FONT_TICK } from "./chart-colors";
import { ComingSoon } from "./coming-soon";

export interface OrgHealthRadarProps {
  orgKpis: OrgKpis;
}

export function OrgHealthRadar({ orgKpis }: OrgHealthRadarProps) {
  const data = [
    { metric: "Build Success", value: orgKpis.avgBuildSuccess },
    { metric: "AI Adoption", value: orgKpis.avgAiAdoption },
    { metric: "Focus Time", value: orgKpis.avgFocus },
  ];
  const hasAnyValue = data.some((d) => d.value !== null && d.value !== undefined);

  return (
    <div>
      <div className="text-foreground mb-4 text-sm font-semibold">
        Team Health Overview
      </div>
      {hasAnyValue ? (
        <ResponsiveContainer width="100%" height={220}>
          <RadarChart data={data}>
            <PolarGrid />
            <PolarAngleAxis dataKey="metric" tick={{ fontSize: CHART_FONT_TICK }} />
            <Radar
              dataKey="value"
              stroke={CHART_BLUE}
              fill={CHART_BLUE}
              fillOpacity={0.15}
              strokeWidth={2}
            />
            <Tooltip />
          </RadarChart>
        </ResponsiveContainer>
      ) : (
        <ComingSoon variant="card" />
      )}
    </div>
  );
}

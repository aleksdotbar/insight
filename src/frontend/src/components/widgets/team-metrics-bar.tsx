import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import type { ExecTeamRow } from "@/types/insight";

import {
  CHART_BLUE,
  CHART_FONT_TICK,
  CHART_GRAY,
  CHART_GREEN,
  CHART_PURPLE,
  CHART_TRACK_BG,
} from "./chart-colors";
import { ComingSoon } from "./coming-soon";

export interface TeamMetricsBarProps {
  teams: ExecTeamRow[];
}

const LEGEND_ITEMS: Array<{ colorClass: string; label: string }> = [
  { colorClass: "bg-[color:var(--chart-1)]", label: "Build Success %" },
  { colorClass: "bg-[color:var(--chart-4)]", label: "AI Adoption %" },
  { colorClass: "bg-[color:var(--chart-2)]", label: "Focus Time %" },
];

export function TeamMetricsBar({ teams }: TeamMetricsBarProps) {
  const barData = (teams ?? []).map((t) => ({
    team: t.team_name,
    "Build Success %": t.build_success_pct,
    "AI Adoption %": t.ai_adoption_pct,
    "Focus Time %": t.focus_time_pct,
  }));

  return (
    <div>
      <div className="mb-3 flex items-center justify-between">
        <div className="text-foreground text-sm font-semibold">
          Key Metrics by Team
        </div>
        {barData.length > 0 ? (
          <div className="flex items-center gap-3">
            {LEGEND_ITEMS.map(({ colorClass, label }) => (
              <span key={label} className="flex items-center gap-1">
                <span
                  className={`inline-block h-2 w-2 flex-shrink-0 rounded-full ${colorClass}`}
                />
                <span className="text-muted-foreground text-xs">{label}</span>
              </span>
            ))}
          </div>
        ) : null}
      </div>
      {barData.length === 0 ? (
        <ComingSoon variant="card" />
      ) : (
        <ResponsiveContainer width="100%" height={220}>
          <BarChart
            data={barData}
            margin={{ top: 4, right: 4, left: -20, bottom: 0 }}
            barCategoryGap="30%"
          >
            <CartesianGrid
              strokeDasharray="3 3"
              vertical={false}
              stroke={CHART_TRACK_BG}
            />
            <XAxis
              dataKey="team"
              tick={{ fontSize: CHART_FONT_TICK, fill: CHART_GRAY }}
              axisLine={false}
              tickLine={false}
            />
            <YAxis
              domain={[0, 100]}
              tick={{ fontSize: CHART_FONT_TICK, fill: CHART_GRAY }}
              axisLine={false}
              tickLine={false}
            />
            <Tooltip />
            <Bar
              dataKey="Build Success %"
              fill={CHART_BLUE}
              radius={[3, 3, 0, 0]}
              maxBarSize={20}
            />
            <Bar
              dataKey="AI Adoption %"
              fill={CHART_PURPLE}
              radius={[3, 3, 0, 0]}
              maxBarSize={20}
            />
            <Bar
              dataKey="Focus Time %"
              fill={CHART_GREEN}
              radius={[3, 3, 0, 0]}
              maxBarSize={20}
            />
          </BarChart>
        </ResponsiveContainer>
      )}
    </div>
  );
}

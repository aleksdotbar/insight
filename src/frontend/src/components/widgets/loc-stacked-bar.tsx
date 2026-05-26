import { memo } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import {
  CHART_AI_LOC,
  CHART_BLUE,
  CHART_FONT_TICK,
  CHART_GRAY,
  CHART_SPEC_LINES,
  CHART_TRACK_BG,
} from "./chart-colors";
import { ComingSoon } from "./coming-soon";

export interface LocStackedBarProps {
  data: Array<{
    label: string;
    aiLoc: number;
    codeLoc: number;
    specLines: number | null;
  }>;
}

type ChartRow = {
  label: string;
  "AI LOC": number;
  "Code LOC": number;
  "Spec Lines": number;
};

function LocStackedBarImpl({ data }: LocStackedBarProps) {
  if (data.length === 0) return <ComingSoon variant="card" />;

  const chartData: ChartRow[] = data.map((r) => ({
    label: r.label,
    "AI LOC": r.aiLoc,
    "Code LOC": r.codeLoc,
    "Spec Lines": r.specLines ?? 0,
  }));

  return (
    <ResponsiveContainer width="100%" height={200}>
      <BarChart
        data={chartData}
        margin={{ top: 4, right: 4, left: -20, bottom: 0 }}
        barSize={chartData.length <= 3 ? 40 : undefined}
        maxBarSize={60}
      >
        <CartesianGrid strokeDasharray="3 3" vertical={false} stroke={CHART_TRACK_BG} />
        <XAxis
          dataKey="label"
          tick={{ fontSize: CHART_FONT_TICK, fill: CHART_GRAY }}
          axisLine={false}
          tickLine={false}
        />
        <YAxis
          tick={{ fontSize: CHART_FONT_TICK, fill: CHART_GRAY }}
          axisLine={false}
          tickLine={false}
        />
        <Tooltip />
        <Legend wrapperStyle={{ fontSize: CHART_FONT_TICK, paddingTop: 8 }} />
        <Bar dataKey="AI LOC" stackId="loc" fill={CHART_AI_LOC} />
        <Bar dataKey="Code LOC" stackId="loc" fill={CHART_BLUE} />
        <Bar dataKey="Spec Lines" stackId="loc" fill={CHART_SPEC_LINES} radius={[3, 3, 0, 0]} />
      </BarChart>
    </ResponsiveContainer>
  );
}

export const LocStackedBar = memo(LocStackedBarImpl);

import { memo } from "react";
import {
  BarChart,
  CartesianGrid,
  ChartBar,
  ChartLegend,
  ChartTooltip,
  ResponsiveContainer,
  XAxis,
  YAxis,
} from "@/components/ui/chart";

import {
  CHART_BLUE,
  CHART_FONT_TICK,
  CHART_GRAY,
  CHART_SPEC_LINES,
  CHART_TRACK_BG,
} from "./chart-colors";
import { ComingSoon } from "./coming-soon";
import type { LocDataPoint } from "@/types/insight";

export interface LocStackedBarProps {
  data: LocDataPoint[];
}

type ChartRow = {
  label: string;
  "Code LOC": number;
  "Spec Lines": number;
  Config: number;
};

function LocStackedBarImpl({ data }: LocStackedBarProps) {
  if (data.length === 0) return <ComingSoon variant="card" />;

  const chartData: ChartRow[] = data.map((r) => ({
    label: r.label,
    "Code LOC": r.codeLoc,
    "Spec Lines": r.specLines,
    Config: r.configLoc,
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
        <ChartTooltip />
        <ChartLegend wrapperStyle={{ fontSize: CHART_FONT_TICK, paddingTop: 8 }} />
        <ChartBar dataKey="Code LOC" stackId="loc" fill={CHART_BLUE} />
        <ChartBar dataKey="Spec Lines" stackId="loc" fill={CHART_SPEC_LINES} />
        <ChartBar dataKey="Config" stackId="loc" fill={CHART_GRAY} radius={[3, 3, 0, 0]} />
      </BarChart>
    </ResponsiveContainer>
  );
}

export const LocStackedBar = memo(LocStackedBarImpl);

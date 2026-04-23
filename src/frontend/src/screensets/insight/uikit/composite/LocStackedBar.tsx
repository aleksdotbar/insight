/**
 * LocStackedBar — stacked bar chart for LOC breakdown.
 * Series: AI LOC (light blue), Code LOC (blue), Spec Lines (purple).
 * Aggregates raw weekly rows by period (week→daily, month→4w, quarter→monthly, year→quarterly).
 * Chart components from @hai3/uikit; Legend from recharts directly.
 * No state imports.
 */

import React from 'react';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  ResponsiveContainer,
  ChartTooltipContent,
  ChartLegendContent,
} from '@hai3/uikit';
import { Tooltip, Legend } from 'recharts';
import { CHART_GRAY, CHART_TRACK_BG, CHART_BLUE, CHART_AI_LOC, CHART_SPEC_LINES, CHART_FONT_TICK } from '../base/chartColors';
import ComingSoon from './ComingSoon';

export interface LocStackedBarProps {
  // specLines is nullable — spec extractor not wired. Recharts doesn't stack
  // `null` values, so we coerce to 0 for rendering; the axis legend /
  // tooltip can distinguish "0 spec lines" (present, none this period) from
  // "spec source not ingested" via column sum or an explicit note in the UI.
  data: Array<{ label: string; aiLoc: number; codeLoc: number; specLines: number | null }>;
}

type ChartRow = { label: string; 'AI LOC': number; 'Code LOC': number; 'Spec Lines': number };

const LocStackedBar: React.FC<LocStackedBarProps> = ({ data }) => {
  if (data.length === 0) {
    return <ComingSoon variant="card" />;
  }

  const chartData: ChartRow[] = data.map((r) => ({
    label: r.label,
    'AI LOC': r.aiLoc,
    'Code LOC': r.codeLoc,
    'Spec Lines': r.specLines ?? 0,
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
        <XAxis dataKey="label" tick={{ fontSize: CHART_FONT_TICK, fill: CHART_GRAY }} axisLine={false} tickLine={false} />
        <YAxis tick={{ fontSize: CHART_FONT_TICK, fill: CHART_GRAY }} axisLine={false} tickLine={false} />
        <Tooltip content={<ChartTooltipContent />} />
        <Legend content={<ChartLegendContent />} wrapperStyle={{ fontSize: CHART_FONT_TICK, paddingTop: 8 }} />
        <Bar dataKey="AI LOC" stackId="loc" fill={CHART_AI_LOC} />
        <Bar dataKey="Code LOC" stackId="loc" fill={CHART_BLUE} />
        <Bar dataKey="Spec Lines" stackId="loc" fill={CHART_SPEC_LINES} radius={[3, 3, 0, 0]} />
      </BarChart>
    </ResponsiveContainer>
  );
};

export default React.memo(LocStackedBar);

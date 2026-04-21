/**
 * DeliveryTrends — multi-line chart for delivery metrics over time.
 * Series: Commits (blue), PRs Merged (purple), Tasks Done (green).
 * Aggregates raw weekly rows by period (week→daily, month→4w, quarter→monthly, year→quarterly).
 * Chart components from @hai3/uikit; Legend from recharts directly.
 * No state imports.
 */

import React from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  ResponsiveContainer,
  ChartTooltipContent,
  ChartLegendContent,
} from '@hai3/uikit';
import { Tooltip, Legend } from 'recharts';
import { CHART_GRAY, CHART_TRACK_BG, CHART_BLUE, CHART_PURPLE, CHART_GREEN, CHART_FONT_TICK } from '../base/chartColors';
import ComingSoon from './ComingSoon';

export interface DeliveryTrendsProps {
  data: Array<{ label: string; commits: number; prsMerged: number; tasksDone: number }>;
}

type ChartRow = { label: string; Commits: number; 'PRs Merged': number; 'Tasks Done': number };

const DeliveryTrends: React.FC<DeliveryTrendsProps> = ({ data }) => {
  if (data.length === 0) {
    return <ComingSoon variant="card" />;
  }

  const chartData: ChartRow[] = data.map((r) => ({
    label: r.label,
    Commits: r.commits,
    'PRs Merged': r.prsMerged,
    'Tasks Done': r.tasksDone,
  }));

  return (
    <ResponsiveContainer width="100%" height={180}>
      <LineChart data={chartData} margin={{ top: 4, right: 4, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" vertical={false} stroke={CHART_TRACK_BG} />
        <XAxis dataKey="label" tick={{ fontSize: CHART_FONT_TICK, fill: CHART_GRAY }} axisLine={false} tickLine={false} />
        <YAxis width={28} tick={{ fontSize: CHART_FONT_TICK, fill: CHART_GRAY }} axisLine={false} tickLine={false} allowDecimals={false} />
        <Tooltip content={<ChartTooltipContent />} />
        <Legend content={<ChartLegendContent />} wrapperStyle={{ fontSize: CHART_FONT_TICK, paddingTop: 8 }} />
        <Line type="monotone" dataKey="Commits" stroke={CHART_BLUE} strokeWidth={2} dot={{ r: 3 }} activeDot={{ r: 5 }} />
        <Line type="monotone" dataKey="PRs Merged" stroke={CHART_PURPLE} strokeWidth={2} dot={{ r: 3 }} activeDot={{ r: 5 }} />
        <Line type="monotone" dataKey="Tasks Done" stroke={CHART_GREEN} strokeWidth={2} dot={{ r: 3 }} activeDot={{ r: 5 }} />
      </LineChart>
    </ResponsiveContainer>
  );
};

export default React.memo(DeliveryTrends);

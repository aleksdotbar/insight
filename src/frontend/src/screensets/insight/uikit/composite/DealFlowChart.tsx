/**
 * DealFlowChart — multi-line chart for sales deal flow over time.
 * Series: Opened (blue), Closed (purple), Won (green).
 *
 * Mirrors `DeliveryTrends` (commits/PRs/tasks) but for deal-flow metrics —
 * kept as a separate component so the eng chart's keys/labels don't bleed
 * into sales semantics. Same color palette so palette consistency carries
 * across personas.
 *
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

export interface DealFlowChartProps {
  data: Array<{ label: string; opened: number; closed: number; won: number }>;
}

type ChartRow = { label: string; Opened: number; Closed: number; Won: number };

const DealFlowChart: React.FC<DealFlowChartProps> = ({ data }) => {
  if (data.length === 0) {
    return <ComingSoon variant="card" />;
  }

  const chartData: ChartRow[] = data.map((r) => ({
    label: r.label,
    Opened: r.opened,
    Closed: r.closed,
    Won:    r.won,
  }));

  return (
    <ResponsiveContainer width="100%" height={180}>
      <LineChart data={chartData} margin={{ top: 4, right: 4, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" vertical={false} stroke={CHART_TRACK_BG} />
        <XAxis dataKey="label" tick={{ fontSize: CHART_FONT_TICK, fill: CHART_GRAY }} axisLine={false} tickLine={false} />
        <YAxis width={28} tick={{ fontSize: CHART_FONT_TICK, fill: CHART_GRAY }} axisLine={false} tickLine={false} allowDecimals={false} />
        <Tooltip content={<ChartTooltipContent />} />
        <Legend content={<ChartLegendContent />} wrapperStyle={{ fontSize: CHART_FONT_TICK, paddingTop: 8 }} />
        <Line type="monotone" dataKey="Opened" stroke={CHART_BLUE}   strokeWidth={2} dot={{ r: 3 }} activeDot={{ r: 5 }} />
        <Line type="monotone" dataKey="Closed" stroke={CHART_PURPLE} strokeWidth={2} dot={{ r: 3 }} activeDot={{ r: 5 }} />
        <Line type="monotone" dataKey="Won"    stroke={CHART_GREEN}  strokeWidth={2} dot={{ r: 3 }} activeDot={{ r: 5 }} />
      </LineChart>
    </ResponsiveContainer>
  );
};

export default React.memo(DealFlowChart);

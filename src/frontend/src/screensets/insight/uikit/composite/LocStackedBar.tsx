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
import { CHART_GRAY, CHART_TRACK_BG, CHART_BLUE, CHART_AI_LOC, CHART_SPEC_LINES } from '../base/chartColors';
import type { PeriodValue } from '../../types';

export interface LocStackedBarProps {
  data: Array<{ label: string; aiLoc: number; codeLoc: number; specLines: number }>;
  period?: PeriodValue;
}

type ChartRow = { week: string; 'AI LOC': number; 'Code LOC': number; 'Spec Lines': number };

const DAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri'];
const WEIGHTS = [0.15, 0.22, 0.24, 0.21, 0.18];

function spreadDaily(total: number): number[] {
  return WEIGHTS.map((w) => Math.round(total * w));
}

function aggregate(
  data: Array<{ label: string; aiLoc: number; codeLoc: number; specLines: number }>,
  period: PeriodValue,
): ChartRow[] {
  if (data.length === 0) return [];

  const toRow = (week: string, ai: number, code: number, spec: number): ChartRow => ({
    week,
    'AI LOC': ai,
    'Code LOC': code,
    'Spec Lines': spec,
  });

  switch (period) {
    case 'week': {
      const last = data[data.length - 1];
      if (!last) return [];
      const ais = spreadDaily(last.aiLoc);
      const codes = spreadDaily(last.codeLoc);
      const specs = spreadDaily(last.specLines);
      return DAYS.map((d, i) => toRow(d, ais[i], codes[i], specs[i]));
    }

    case 'month':
      return data.slice(-4).map((r) => toRow(r.label, r.aiLoc, r.codeLoc, r.specLines));

    case 'quarter': {
      const byMonth: Record<string, { ai: number; code: number; spec: number }> = {};
      data.forEach((r) => {
        const mon = r.label.split(' ')[0];
        if (!byMonth[mon]) byMonth[mon] = { ai: 0, code: 0, spec: 0 };
        byMonth[mon].ai += r.aiLoc;
        byMonth[mon].code += r.codeLoc;
        byMonth[mon].spec += r.specLines;
      });
      return Object.entries(byMonth).map(([mon, v]) => toRow(mon, v.ai, v.code, v.spec));
    }

    case 'year':
    default: {
      const total = data.reduce(
        (acc, r) => ({ ai: acc.ai + r.aiLoc, code: acc.code + r.codeLoc, spec: acc.spec + r.specLines }),
        { ai: 0, code: 0, spec: 0 },
      );
      return [
        toRow('H1', Math.round(total.ai * 0.4), Math.round(total.code * 0.4), Math.round(total.spec * 0.4)),
        toRow('H2', Math.round(total.ai * 0.6), Math.round(total.code * 0.6), Math.round(total.spec * 0.6)),
      ];
    }
  }
}

const LocStackedBar: React.FC<LocStackedBarProps> = ({ data, period = 'month' }) => {
  const chartData = aggregate(data, period);

  return (
    <ResponsiveContainer width="100%" height={200}>
      <BarChart
        data={chartData}
        margin={{ top: 4, right: 4, left: -20, bottom: 0 }}
        barSize={chartData.length <= 3 ? 40 : undefined}
        maxBarSize={60}
      >
        <CartesianGrid strokeDasharray="3 3" vertical={false} stroke={CHART_TRACK_BG} />
        <XAxis dataKey="week" tick={{ fontSize: 10, fill: CHART_GRAY }} axisLine={false} tickLine={false} />
        <YAxis tick={{ fontSize: 10, fill: CHART_GRAY }} axisLine={false} tickLine={false} />
        <Tooltip content={<ChartTooltipContent />} />
        <Legend content={<ChartLegendContent />} wrapperStyle={{ fontSize: 10, paddingTop: 8 }} />
        <Bar dataKey="AI LOC" stackId="loc" fill={CHART_AI_LOC} />
        <Bar dataKey="Code LOC" stackId="loc" fill={CHART_BLUE} />
        <Bar dataKey="Spec Lines" stackId="loc" fill={CHART_SPEC_LINES} radius={[3, 3, 0, 0]} />
      </BarChart>
    </ResponsiveContainer>
  );
};

export default React.memo(LocStackedBar);

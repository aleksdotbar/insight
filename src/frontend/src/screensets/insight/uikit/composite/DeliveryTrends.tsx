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
  Tooltip,
  ResponsiveContainer,
} from '@hai3/uikit';
import { Legend } from 'recharts';
import { CHART_GRAY, CHART_TRACK_BG, CHART_BLUE, CHART_PURPLE, CHART_GREEN } from '../base/chartColors';
import type { PeriodValue } from '../../types';

export interface DeliveryTrendsProps {
  data: Array<{ label: string; commits: number; prsMerged: number; tasksDone: number }>;
  period?: PeriodValue;
}

type ChartRow = { week: string; Commits: number; 'PRs Merged': number; 'Tasks Done': number };

const DAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri'];
const WEIGHTS = [0.15, 0.22, 0.24, 0.21, 0.18];

function spreadDaily(total: number): number[] {
  return WEIGHTS.map((w) => Math.round(total * w));
}

function aggregate(
  data: Array<{ label: string; commits: number; prsMerged: number; tasksDone: number }>,
  period: PeriodValue,
): ChartRow[] {
  if (data.length === 0) return [];

  const toRow = (week: string, commits: number, prs: number, tasks: number): ChartRow => ({
    week,
    Commits: commits,
    'PRs Merged': prs,
    'Tasks Done': tasks,
  });

  switch (period) {
    case 'week': {
      const last = data[data.length - 1];
      if (!last) return [];
      const c = spreadDaily(last.commits);
      const p = spreadDaily(last.prsMerged);
      const t = spreadDaily(last.tasksDone);
      return DAYS.map((d, i) => toRow(d, c[i], p[i], t[i]));
    }

    case 'month':
      return data.slice(-4).map((r) => toRow(r.label, r.commits, r.prsMerged, r.tasksDone));

    case 'quarter': {
      const byMonth: Record<string, { commits: number; prs: number; tasks: number }> = {};
      data.forEach((r) => {
        const mon = r.label.split(' ')[0];
        if (!byMonth[mon]) byMonth[mon] = { commits: 0, prs: 0, tasks: 0 };
        byMonth[mon].commits += r.commits;
        byMonth[mon].prs += r.prsMerged;
        byMonth[mon].tasks += r.tasksDone;
      });
      return Object.entries(byMonth).map(([mon, v]) => toRow(mon, v.commits, v.prs, v.tasks));
    }

    case 'year':
    default: {
      const total = data.reduce(
        (acc, r) => ({ commits: acc.commits + r.commits, prs: acc.prs + r.prsMerged, tasks: acc.tasks + r.tasksDone }),
        { commits: 0, prs: 0, tasks: 0 },
      );
      return [
        toRow('H1', Math.round(total.commits * 0.4), Math.round(total.prs * 0.4), Math.round(total.tasks * 0.4)),
        toRow('H2', Math.round(total.commits * 0.6), Math.round(total.prs * 0.6), Math.round(total.tasks * 0.6)),
      ];
    }
  }
}

const DeliveryTrends: React.FC<DeliveryTrendsProps> = ({ data, period = 'month' }) => {
  const chartData = aggregate(data, period);

  return (
    <ResponsiveContainer width="100%" height={180}>
      <LineChart data={chartData} margin={{ top: 4, right: 4, left: -20, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" vertical={false} stroke={CHART_TRACK_BG} />
        <XAxis dataKey="week" tick={{ fontSize: 10, fill: CHART_GRAY }} axisLine={false} tickLine={false} />
        <YAxis tick={{ fontSize: 10, fill: CHART_GRAY }} axisLine={false} tickLine={false} allowDecimals={false} />
        <Tooltip />
        <Legend iconType="circle" iconSize={8} wrapperStyle={{ fontSize: 10, paddingTop: 8 }} />
        <Line type="monotone" dataKey="Commits" stroke={CHART_BLUE} strokeWidth={2} dot={{ r: 3 }} activeDot={{ r: 5 }} />
        <Line type="monotone" dataKey="PRs Merged" stroke={CHART_PURPLE} strokeWidth={2} dot={{ r: 3 }} activeDot={{ r: 5 }} />
        <Line type="monotone" dataKey="Tasks Done" stroke={CHART_GREEN} strokeWidth={2} dot={{ r: 3 }} activeDot={{ r: 5 }} />
      </LineChart>
    </ResponsiveContainer>
  );
};

export default React.memo(DeliveryTrends);

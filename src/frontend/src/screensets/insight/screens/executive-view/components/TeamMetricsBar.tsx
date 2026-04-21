/**
 * TeamMetricsBar — grouped bar chart of build/AI/focus metrics per team.
 * No state imports.
 */

import React from 'react';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from '@hai3/uikit';
import type { ExecTeamRow } from '../../../types';
import { CHART_BLUE, CHART_PURPLE, CHART_GREEN, CHART_GRAY, CHART_TRACK_BG, CHART_FONT_TICK } from '../../../uikit/base/chartColors';
import ComingSoon from '../../../uikit/composite/ComingSoon';

export interface TeamMetricsBarProps {
  teams: ExecTeamRow[];
}

const LEGEND_ITEMS = [
  { color: CHART_BLUE,   colorClass: 'bg-blue-600',   label: 'Build Success %' },
  { color: CHART_PURPLE, colorClass: 'bg-violet-600', label: 'AI Adoption %' },
  { color: CHART_GREEN,  colorClass: 'bg-green-600',  label: 'Focus Time %' },
];

export const TeamMetricsBar: React.FC<TeamMetricsBarProps> = ({ teams }) => {
  const barData = (teams ?? []).map((t) => ({
    team: t.team_name,
    'Build Success %': t.build_success_pct,
    'AI Adoption %': t.ai_adoption_pct,
    'Focus Time %': t.focus_time_pct,
  }));

  return (
    <div>
      <div className="flex items-center justify-between mb-3">
        <div className="text-sm font-semibold">Key Metrics by Team</div>
        <div className="flex items-center gap-3">
          {LEGEND_ITEMS.map(({ colorClass, label }) => (
            <span key={label} className="flex items-center gap-1">
              <span className={`inline-block w-2 h-2 rounded-full flex-shrink-0 ${colorClass}`} />
              <span className="text-xs text-gray-500">{label}</span>
            </span>
          ))}
        </div>
      </div>
      {barData.length === 0 ? (
        <ComingSoon variant="card" />
      ) : (
        <ResponsiveContainer width="100%" height={220}>
          <BarChart data={barData} margin={{ top: 4, right: 4, left: -20, bottom: 0 }} barCategoryGap="30%">
            <CartesianGrid strokeDasharray="3 3" vertical={false} stroke={CHART_TRACK_BG} />
            <XAxis dataKey="team" tick={{ fontSize: CHART_FONT_TICK, fill: CHART_GRAY }} axisLine={false} tickLine={false} />
            <YAxis domain={[0, 100]} tick={{ fontSize: CHART_FONT_TICK, fill: CHART_GRAY }} axisLine={false} tickLine={false} />
            <Tooltip />
            <Bar dataKey="Build Success %" fill={CHART_BLUE} radius={[3, 3, 0, 0]} maxBarSize={20} />
            <Bar dataKey="AI Adoption %" fill={CHART_PURPLE} radius={[3, 3, 0, 0]} maxBarSize={20} />
            <Bar dataKey="Focus Time %" fill={CHART_GREEN} radius={[3, 3, 0, 0]} maxBarSize={20} />
          </BarChart>
        </ResponsiveContainer>
      )}
    </div>
  );
};

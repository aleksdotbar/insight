/**
 * OrgHealthRadar — radar chart showing the org-level KPI dimensions that
 * are natively 0..100 at the team level (Build Success, AI Adoption, Focus
 * Time). Bug Resolution / PR Cycle axes were removed — they required
 * invented FE normalization formulas; re-add only when the backend
 * supplies real normalized scores.
 * No state imports.
 */

import React from 'react';
import {
  RadarChart,
  PolarGrid,
  PolarAngleAxis,
  Radar,
  ResponsiveContainer,
  ChartTooltipContent,
} from '@hai3/uikit';
import { Tooltip } from 'recharts';
import type { OrgKpis } from '../../../types';
import { CHART_BLUE, CHART_FONT_TICK } from '../../../uikit/base/chartColors';
import ComingSoon from '../../../uikit/composite/ComingSoon';

export interface OrgHealthRadarProps {
  orgKpis: OrgKpis;
}

export const OrgHealthRadar: React.FC<OrgHealthRadarProps> = ({ orgKpis }) => {
  // Only metrics that are already 0..100 at the team level and aggregate
  // meaningfully on the org level. Bug Resolution / PR Cycle were previously
  // synthesized here with invented formulas — left out until the backend
  // supplies real org-level normalized scores.
  const data = [
    { metric: 'Build Success', value: orgKpis.avgBuildSuccess },
    { metric: 'AI Adoption',   value: orgKpis.avgAiAdoption },
    { metric: 'Focus Time',    value: orgKpis.avgFocus },
  ];

  const hasAnyValue = data.some((d) => d.value !== null && d.value !== undefined);

  return (
    <div>
      <div className="text-sm font-semibold mb-4">
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
            <Tooltip content={<ChartTooltipContent />} />
          </RadarChart>
        </ResponsiveContainer>
      ) : (
        <ComingSoon variant="card" />
      )}
    </div>
  );
};

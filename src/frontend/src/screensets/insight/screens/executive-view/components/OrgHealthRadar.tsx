/**
 * OrgHealthRadar — radar chart showing 5 org-level KPI dimensions.
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

export interface OrgHealthRadarProps {
  orgKpis: OrgKpis;
}

export const OrgHealthRadar: React.FC<OrgHealthRadarProps> = ({ orgKpis }) => {
  const data = [
    { metric: 'Build Success', value: orgKpis.avgBuildSuccess },
    { metric: 'AI Adoption', value: orgKpis.avgAiAdoption },
    { metric: 'Focus Time', value: orgKpis.avgFocus },
    { metric: 'Bug Resolution', value: orgKpis.bugResolutionScore },
    { metric: 'PR Cycle', value: orgKpis.prCycleScore },
  ];

  return (
    <div>
      <div className="text-sm font-semibold mb-4">
        Team Health Overview
      </div>
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
    </div>
  );
};

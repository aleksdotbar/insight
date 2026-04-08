/**
 * OrgKpiCards — 4 summary KPI cards for the executive view header row.
 * No state imports.
 */

import React from 'react';
import { Card, CardContent } from '@hai3/uikit';
import type { ExecTeamRow, OrgKpis, ExecColumnThreshold } from '../../../types';
import MetricInfo from '../../../uikit/base/MetricInfo';

export interface OrgKpiCardsProps {
  teams: ExecTeamRow[];
  orgKpis: OrgKpis | null;
  columnThresholds: ExecColumnThreshold[];
}

interface KpiCardDef {
  label: string;
  value: number | string;
  isGood: boolean;
  description: string;
}

const KpiCard: React.FC<{ label: string; value: number | string; isGood: boolean; description: string }> = ({
  label,
  value,
  isGood,
  description,
}) => (
  <Card className="text-center">
    <CardContent className="p-4">
      <div className={`text-2xl font-extrabold ${isGood ? 'text-insight-green' : 'text-insight-amber'}`}>
        {value}
      </div>
      <div className="flex items-center justify-center text-xs text-gray-500 mt-1">
        {label}
        <MetricInfo description={description} side="bottom" />
      </div>
    </CardContent>
  </Card>
);

function isGoodByThreshold(value: number, metricKey: string, thresholds: ExecColumnThreshold[]): boolean {
  const t = thresholds.find((x) => x.metric_key === metricKey);
  return t ? value >= t.threshold : true;
}

export const OrgKpiCards: React.FC<OrgKpiCardsProps> = ({ teams, orgKpis, columnThresholds }) => {
  const teamsAtRisk = (teams ?? []).filter((t) => t.status === 'warn' || t.status === 'bad').length;
  const avgBuildSuccess = orgKpis?.avgBuildSuccess ?? 0;
  const avgAiAdoption = orgKpis?.avgAiAdoption ?? 0;
  const avgFocus = orgKpis?.avgFocus ?? 0;

  const buildT = columnThresholds.find((t) => t.metric_key === 'build_success_pct')?.threshold ?? 90;
  const aiT    = columnThresholds.find((t) => t.metric_key === 'ai_adoption_pct')?.threshold ?? 60;
  const focusT = columnThresholds.find((t) => t.metric_key === 'focus_time_pct')?.threshold ?? 60;

  const cards: KpiCardDef[] = [
    { label: 'Teams at Risk', value: teamsAtRisk, isGood: teamsAtRisk === 0,
      description: 'Teams with warn or bad status across key delivery and quality metrics.' },
    { label: 'Avg Build Success', value: `${avgBuildSuccess}%`, isGood: isGoodByThreshold(avgBuildSuccess, 'build_success_pct', columnThresholds),
      description: `Average CI/CD build pass rate across all teams. Target ≥${buildT}%.` },
    { label: 'Avg AI Adoption', value: `${avgAiAdoption}%`, isGood: isGoodByThreshold(avgAiAdoption, 'ai_adoption_pct', columnThresholds),
      description: `Average share of members actively using AI tools this period. Target ≥${aiT}%.` },
    { label: 'Avg Focus Time', value: `${avgFocus}%`, isGood: isGoodByThreshold(avgFocus, 'focus_time_pct', columnThresholds),
      description: `Average share of work time spent in uninterrupted 60-min+ blocks. Target ≥${focusT}%.` },
  ];

  return (
    <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
      {cards.map((card) => (
        <KpiCard key={card.label} label={card.label} value={card.value} isGood={card.isGood} description={card.description} />
      ))}
    </div>
  );
};

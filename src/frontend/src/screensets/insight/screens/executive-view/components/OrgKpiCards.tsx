/**
 * OrgKpiCards — 4 summary KPI cards for the executive view header row.
 * No state imports.
 */

import React from 'react';
import { Card, CardContent } from '@hai3/uikit';
import type { ExecTeamRow, OrgKpis } from '../../../types';
import MetricInfo from '../../../uikit/base/MetricInfo';

export interface OrgKpiCardsProps {
  teams: ExecTeamRow[];
  orgKpis: OrgKpis | null;
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
      <div className={`text-2xl font-extrabold ${isGood ? 'text-green-600' : 'text-amber-600'}`}>
        {value}
      </div>
      <div className="flex items-center justify-center text-xs text-gray-500 mt-1">
        {label}
        <MetricInfo description={description} side="bottom" />
      </div>
    </CardContent>
  </Card>
);

export const OrgKpiCards: React.FC<OrgKpiCardsProps> = ({ teams, orgKpis }) => {
  const teamsAtRisk = (teams ?? []).filter((t) => t.status === 'warn' || t.status === 'bad').length;
  const avgBuildSuccess = orgKpis?.avgBuildSuccess ?? 0;
  const avgAiAdoption = orgKpis?.avgAiAdoption ?? 0;
  const avgFocus = orgKpis?.avgFocus ?? 0;

  const cards: KpiCardDef[] = [
    { label: 'Teams at Risk', value: teamsAtRisk, isGood: teamsAtRisk === 0,
      description: 'Teams with warn or bad status across key delivery and quality metrics.' },
    { label: 'Avg Build Success', value: `${avgBuildSuccess}%`, isGood: avgBuildSuccess >= 90,
      description: 'Average CI/CD build pass rate across all teams. Target ≥90%.' },
    { label: 'Avg AI Adoption', value: `${avgAiAdoption}%`, isGood: avgAiAdoption >= 60,
      description: 'Average share of members actively using AI tools (Cursor, Claude Code, Codex) this period.' },
    { label: 'Avg Focus Time', value: `${avgFocus}%`, isGood: avgFocus >= 60,
      description: 'Average share of work time spent in uninterrupted 60-min+ blocks. Target ≥60%.' },
  ];

  return (
    <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
      {cards.map((card) => (
        <KpiCard key={card.label} label={card.label} value={card.value} isGood={card.isGood} description={card.description} />
      ))}
    </div>
  );
};

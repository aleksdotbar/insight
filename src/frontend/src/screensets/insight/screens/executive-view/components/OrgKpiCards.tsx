/**
 * OrgKpiCards — 4 summary KPI cards for the executive view header row.
 * No state imports.
 */

import React from 'react';
import type { ExecTeamRow, OrgKpis } from '../../../types';

export interface OrgKpiCardsProps {
  teams: ExecTeamRow[];
  orgKpis: OrgKpis | null;
}

interface KpiCardDef {
  label: string;
  value: number | string;
  isGood: boolean;
}

const KpiCard: React.FC<{ label: string; value: number | string; isGood: boolean }> = ({
  label,
  value,
  isGood,
}) => (
  <div className="bg-white border border-gray-200 rounded-lg shadow-sm p-4 text-center">
    <div className={`text-2xl font-extrabold ${isGood ? 'text-green-600' : 'text-amber-600'}`}>
      {value}
    </div>
    <div className="text-xs text-gray-500 mt-1">{label}</div>
  </div>
);

export const OrgKpiCards: React.FC<OrgKpiCardsProps> = ({ teams, orgKpis }) => {
  const teamsAtRisk = (teams ?? []).filter((t) => t.status === 'warn' || t.status === 'bad').length;
  const avgBuildSuccess = orgKpis?.avgBuildSuccess ?? 0;
  const avgAiAdoption = orgKpis?.avgAiAdoption ?? 0;
  const avgFocus = orgKpis?.avgFocus ?? 0;

  const cards: KpiCardDef[] = [
    { label: 'Teams at Risk', value: teamsAtRisk, isGood: teamsAtRisk === 0 },
    { label: 'Avg Build Success', value: `${avgBuildSuccess}%`, isGood: avgBuildSuccess >= 90 },
    { label: 'Avg AI Adoption', value: `${avgAiAdoption}%`, isGood: avgAiAdoption >= 60 },
    { label: 'Avg Focus Time', value: `${avgFocus}%`, isGood: avgFocus >= 60 },
  ];

  return (
    <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
      {cards.map((card) => (
        <KpiCard key={card.label} label={card.label} value={card.value} isGood={card.isGood} />
      ))}
    </div>
  );
};

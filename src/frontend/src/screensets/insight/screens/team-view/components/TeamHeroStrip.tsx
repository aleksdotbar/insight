/**
 * TeamHeroStrip — 4 KPI hero cards for the team view header.
 * No state imports.
 */

import React from 'react';
import { Badge, Card } from '@hai3/uikit';
import type { TeamKpi } from '../../../types';

export interface TeamHeroStripProps {
  teamKpis: TeamKpi[];
}

const CHIP_CLASS: Record<'good' | 'warn' | 'bad', string> = {
  good: 'bg-insight-green-bg text-insight-green',
  warn: 'bg-insight-amber-bg text-insight-amber',
  bad: 'bg-insight-red-bg text-insight-red',
};

// Border classes for each card position on mobile (2-col) and desktop (4-col)
const CARD_BORDER: Record<number, string> = {
  0: '',
  1: 'border-l border-gray-200',
  2: 'border-t sm:border-t-0 sm:border-l border-gray-200',
  3: 'border-t sm:border-t-0 border-l border-gray-200',
};

const KpiCard: React.FC<{ kpi: TeamKpi; idx: number }> = ({ kpi, idx }) => (
  <div className={`flex flex-col gap-0.5 p-3 bg-white ${CARD_BORDER[idx] ?? 'border-l border-gray-200'}`}>
    <div className="text-[20px] font-extrabold text-gray-900 leading-tight tracking-tight">
      {kpi.value}
      {kpi.unit && <span className="text-[11px] font-semibold text-gray-400 ml-0.5">{kpi.unit}</span>}
    </div>
    <div className="text-[11px] font-semibold text-gray-900">{kpi.label}</div>
    {kpi.sublabel && <div className="text-[10px] text-gray-400">{kpi.sublabel}</div>}
    <Badge className={`mt-1 text-[10px] font-bold ${CHIP_CLASS[kpi.status]}`}>
      {kpi.chipLabel ?? kpi.status}
    </Badge>
  </div>
);

export const TeamHeroStrip: React.FC<TeamHeroStripProps> = ({ teamKpis }) => (
  <Card className="overflow-hidden">
    <div className="grid grid-cols-2 sm:grid-cols-4">
      {teamKpis.slice(0, 4).map((kpi, i) => (
        <KpiCard key={kpi.metric_key} kpi={kpi} idx={i} />
      ))}
    </div>
  </Card>
);

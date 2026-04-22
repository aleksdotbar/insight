/**
 * TeamHeroStrip — 4 KPI hero cards for the team view header.
 * No state imports.
 */

import React from 'react';
import { Badge, Card } from '@hai3/uikit';
import type { TeamKpi } from '../../../types';
import MetricInfo from '../../../uikit/base/MetricInfo';
import ComingSoon from '../../../uikit/composite/ComingSoon';

export interface TeamHeroStripProps {
  teamKpis: TeamKpi[];
}

const CHIP_CLASS: Record<'good' | 'warn' | 'bad', string> = {
  good: 'bg-insight-green-bg text-insight-green',
  warn: 'bg-insight-amber-bg text-insight-amber',
  bad: 'bg-insight-red-bg text-insight-red',
};

// Border classes per card position. Mobile stays 2-column regardless of
// count (GRID_COLS always sets `grid-cols-2` on mobile); desktop scales
// from 1 to 6 columns. Entries cover up to 6 cards so teams with a custom
// deriveTeamKpis output (5 or 6 chips) still get correct borders.
const CARD_BORDER: Record<number, string> = {
  0: '',
  1: 'border-l border-gray-200',
  2: 'border-t sm:border-t-0 sm:border-l border-gray-200',
  3: 'border-t sm:border-t-0 border-l border-gray-200',
  // Row 3 on mobile (only relevant when 5+ cards); desktop flattens to a
  // single row so the mobile `border-t` gets neutralized by sm:border-t-0.
  4: 'border-t sm:border-t-0 sm:border-l border-gray-200',
  5: 'border-t sm:border-t-0 border-l border-gray-200',
};

const KpiCard: React.FC<{ kpi: TeamKpi; idx: number }> = ({ kpi, idx }) => (
  <div className={`flex flex-col gap-0.5 p-3 bg-white ${CARD_BORDER[idx] ?? 'border-l border-gray-200'}`}>
    <div className="text-xl font-extrabold text-gray-900 leading-tight tracking-tight">
      {kpi.value}
      {kpi.unit && <span className="text-xs font-semibold text-gray-400 ml-0.5">{kpi.unit}</span>}
    </div>
    <div className="flex items-center gap-0.5">
      <span className="text-sm font-semibold text-gray-900">{kpi.label}</span>
      {kpi.description && <MetricInfo description={kpi.description} />}
    </div>
    {kpi.sublabel && <div className="text-2xs text-gray-400">{kpi.sublabel}</div>}
    <Badge className={`mt-1 text-xs font-bold ${CHIP_CLASS[kpi.status]}`}>
      {kpi.chipLabel ?? kpi.status}
    </Badge>
  </div>
);

// Tailwind needs full class names in the source, so enumerate the common
// column counts we expect from deriveTeamKpis.
const GRID_COLS: Record<number, string> = {
  1: 'grid-cols-1 sm:grid-cols-1',
  2: 'grid-cols-2 sm:grid-cols-2',
  3: 'grid-cols-2 sm:grid-cols-3',
  4: 'grid-cols-2 sm:grid-cols-4',
  5: 'grid-cols-2 sm:grid-cols-5',
  6: 'grid-cols-2 sm:grid-cols-6',
};

export const TeamHeroStrip: React.FC<TeamHeroStripProps> = ({ teamKpis }) => {
  // Previously hardcoded `.slice(0, 4)` silently dropped extra KPIs from the
  // header strip. Now we render all the cards deriveTeamKpis returned and
  // pick a grid sized to the actual count.
  const cols = teamKpis.length;
  const gridClass = GRID_COLS[cols] ?? GRID_COLS[4]!;

  return (
    <Card className="overflow-hidden">
      {cols === 0 ? (
        <div className="p-3">
          <ComingSoon variant="card" />
        </div>
      ) : (
        <div className={`grid ${gridClass}`}>
          {teamKpis.map((kpi, i) => (
            <KpiCard key={kpi.metric_key} kpi={kpi} idx={i} />
          ))}
        </div>
      )}
    </Card>
  );
};

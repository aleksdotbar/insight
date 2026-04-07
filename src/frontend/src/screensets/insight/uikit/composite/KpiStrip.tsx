/**
 * KpiStrip — horizontal KPI summary strip.
 * Renders a flex row of KPI cells with value, label, sublabel, delta badge.
 * plain=true: bare grid; plain=false: Card wrapper with shadow.
 * No state imports.
 */

import React from 'react';
import { Card, CardContent } from '@hai3/uikit';

// Units that suppress period suffix
const SUPPRESS_SUFFIX_UNITS = ['%', '×', 'h', 'avg replies', 'avg', '/mo'];

function getPeriodSuffix(unit: string | undefined, period?: string): string {
  if (!period || !unit) return '';
  const u = unit.toLowerCase();
  if (SUPPRESS_SUFFIX_UNITS.some((s) => u.includes(s))) return '';
  const map: Record<string, string> = {
    week: '/ wk',
    month: '/ mo',
    quarter: '/ qtr',
    year: '/ yr',
  };
  return map[period] ?? '';
}

const DELTA_CLASS: Record<string, string> = {
  good: 'bg-green-100 text-green-600',
  warn: 'bg-amber-100 text-amber-600',
  neutral: 'bg-amber-100 text-amber-600',
  bad: 'bg-red-100 text-red-600',
};

export interface KpiStripKpi {
  metric_key: string;
  label: string;
  value: string;
  unit?: string;
  sublabel?: string;
  delta?: string;
  delta_type?: 'good' | 'warn' | 'bad' | 'neutral';
  period?: string;
}

export interface KpiStripProps {
  kpis: KpiStripKpi[];
  plain?: boolean;
}

const KpiCell: React.FC<{ kpi: KpiStripKpi; index: number; total: number }> = ({
  kpi,
  index,
}) => {
  const suffix = getPeriodSuffix(kpi.unit, kpi.period);
  const deltaClass = kpi.delta_type ? DELTA_CLASS[kpi.delta_type] : null;

  return (
    <div className="relative flex-1 px-3 py-2">
      {/* Vertical separator (not at start) */}
      {index > 0 && (
        <div className="absolute left-0 top-[15%] h-[70%] w-px bg-gray-200" />
      )}

      {/* Value row */}
      <div className="flex items-baseline gap-px">
        <span className="text-xl font-extrabold text-gray-900 leading-tight">{kpi.value}</span>
        {kpi.unit && (
          <sup className="text-[11px] font-semibold text-gray-400">{kpi.unit}</sup>
        )}
        {suffix && (
          <span className="text-[9px] text-gray-400 ml-0.5">{suffix}</span>
        )}
      </div>

      {/* Label */}
      <div className="text-[11px] font-semibold text-gray-900 mt-0.5">{kpi.label}</div>

      {/* Sublabel */}
      {kpi.sublabel && (
        <div className="text-[10px] text-gray-400">{kpi.sublabel}</div>
      )}

      {/* Delta badge */}
      {kpi.delta && deltaClass && (
        <div className={`inline-block mt-1 rounded-full px-1.5 py-px text-[10px] font-bold ${deltaClass}`}>
          {kpi.delta}
        </div>
      )}
    </div>
  );
};

const KpiStripGrid: React.FC<{ kpis: KpiStripKpi[] }> = ({ kpis }) => (
  <div className="flex">
    {kpis.map((kpi, i) => (
      <KpiCell key={kpi.metric_key} kpi={kpi} index={i} total={kpis.length} />
    ))}
  </div>
);

const KpiStrip: React.FC<KpiStripProps> = ({ kpis, plain = false }) => {
  if (plain) {
    return <KpiStripGrid kpis={kpis} />;
  }
  return (
    <Card className="shadow-sm">
      <CardContent className="p-0">
        <KpiStripGrid kpis={kpis} />
      </CardContent>
    </Card>
  );
};

export default React.memo(KpiStrip);

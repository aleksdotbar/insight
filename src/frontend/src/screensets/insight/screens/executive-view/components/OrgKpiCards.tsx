/**
 * OrgKpiCards — summary KPI cards for the executive view header row.
 *
 * A KPI with `value === null` renders ComingSoon in place of the value —
 * it means the connector for that metric isn't ingested yet (build_success
 * → CI, ai_adoption / focus → Cursor + M365). We don't substitute a fake
 * zero that reads as "they have 0% adoption".
 *
 * Thresholds come solely from `columnThresholds` (derived from
 * METRIC_SEMANTICS). No hardcoded 90/60/60 fallbacks — missing thresholds
 * disable the color (card renders neutral) instead of silently inventing
 * a policy number.
 *
 * No state imports.
 */

import React from 'react';
import { Card, CardContent } from '@hai3/uikit';
import type { ExecTeamRow, OrgKpis, ExecColumnThreshold } from '../../../types';
import MetricInfo from '../../../uikit/base/MetricInfo';
import ComingSoon from '../../../uikit/composite/ComingSoon';

export interface OrgKpiCardsProps {
  teams: ExecTeamRow[];
  orgKpis: OrgKpis | null;
  columnThresholds: ExecColumnThreshold[];
}

type KpiCardDef = {
  label: string;
  /** null → render ComingSoon chip in the value slot. */
  value: number | string | null;
  valueSuffix?: string;
  /** null → no threshold set; card renders neutral (not good, not bad). */
  isGood: boolean | null;
  description: string;
};

const KpiCard: React.FC<KpiCardDef> = ({ label, value, valueSuffix, isGood, description }) => {
  const valueColor =
    isGood === true ? 'text-insight-green' :
    isGood === false ? 'text-insight-amber' :
    'text-gray-900';

  return (
    <Card className="text-center">
      <CardContent className="p-4">
        {value === null ? (
          <div className="min-h-[2rem] flex items-center justify-center">
            <ComingSoon variant="chip" />
          </div>
        ) : (
          <div className={`text-2xl font-extrabold ${valueColor}`}>
            {value}{valueSuffix}
          </div>
        )}
        <div className="flex items-center justify-center text-xs text-gray-500 mt-1">
          {label}
          <MetricInfo description={description} side="bottom" />
        </div>
      </CardContent>
    </Card>
  );
};

/** Given a value and a known threshold def, returns true/false for good.
 *  Returns null when no threshold is configured — caller renders neutral. */
function thresholdStatus(
  value: number,
  metricKey: string,
  thresholds: ExecColumnThreshold[],
): boolean | null {
  const t = thresholds.find((x) => x.metric_key === metricKey);
  return t ? value >= t.threshold : null;
}

function describeTarget(metricKey: string, thresholds: ExecColumnThreshold[], fallback: string): string {
  const t = thresholds.find((x) => x.metric_key === metricKey);
  return t ? `${fallback} Target \u2265${t.threshold}%.` : fallback;
}

export const OrgKpiCards: React.FC<OrgKpiCardsProps> = ({ teams, orgKpis, columnThresholds }) => {
  const teamsAtRisk = (teams ?? []).filter((t) => t.status === 'warn' || t.status === 'bad').length;

  const build = orgKpis?.avgBuildSuccess ?? null;
  const ai    = orgKpis?.avgAiAdoption   ?? null;
  const focus = orgKpis?.avgFocus        ?? null;

  const cards: KpiCardDef[] = [
    {
      label: 'Teams at Risk',
      value: teamsAtRisk,
      isGood: teamsAtRisk === 0,
      description: 'Teams with warn or bad status across key delivery and quality metrics.',
    },
    {
      label: 'Avg Build Success',
      value: build,
      valueSuffix: '%',
      isGood: build === null ? null : thresholdStatus(build, 'build_success_pct', columnThresholds),
      description: build === null
        ? 'CI connector not configured — no build data to aggregate.'
        : describeTarget('build_success_pct', columnThresholds, 'Average CI/CD build pass rate across all teams.'),
    },
    {
      label: 'Avg AI Adoption',
      value: ai,
      valueSuffix: '%',
      isGood: ai === null ? null : thresholdStatus(ai, 'ai_adoption_pct', columnThresholds),
      description: ai === null
        ? 'No AI-tool data available for any team this period.'
        : describeTarget('ai_adoption_pct', columnThresholds, 'Average share of members actively using AI tools this period.'),
    },
    {
      label: 'Avg Focus Time',
      value: focus,
      valueSuffix: '%',
      isGood: focus === null ? null : thresholdStatus(focus, 'focus_time_pct', columnThresholds),
      description: focus === null
        ? 'No focus-time data available for any team this period.'
        : describeTarget('focus_time_pct', columnThresholds, 'Average share of work time spent in uninterrupted 60-min+ blocks.'),
    },
  ];

  return (
    <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
      {cards.map((card) => (
        <KpiCard key={card.label} {...card} />
      ))}
    </div>
  );
};

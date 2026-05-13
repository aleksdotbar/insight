/**
 * SalesDashboard — IC dashboard body for sales personas.
 *
 * Sections (top → bottom):
 *   1. Sticky person/period header
 *   2. Hero KPI strip — 5 cards: Deals Opened · Closed · Closed Value · Win Rate · Pipeline Now
 *   3. Pacing band — Closed $ vs prior-year same period · period progress
 *   4. Velocity & Quality bullet card — rep vs team distribution
 *   5. Outreach Activity bullet card — rep vs team distribution
 *   6. Deal Flow Trend chart — weekly opened/closed/won
 *
 * Data layer: TanStack React Query, queryOptions factories in
 * `../../../queries/{crm,identity}.ts`. Each section reads its own
 * query state directly (`isPending / isFetching / isError / data`);
 * no slice / actions / events / effects involved. Period and custom
 * range still come from Redux — that's UI state, not server cache.
 */

import React from 'react';
import { useAppSelector } from '@hai3/react';
import { useQuery, useQueries } from '@tanstack/react-query';
import { changePeriod, setDateRange } from '../../../actions/periodActions';
import { selectCustomRange } from '../../../slices/periodSlice';
import { PeriodSelectorBar } from '../../../uikit/composite/PeriodSelectorBar';
import type { CustomRange } from '../../../types';
import { usePeriod } from '../../../hooks/usePeriod';
import { resolveDateRange } from '../../../utils/periodToDateRange';
import { crmQueries } from '../../../queries/crm';
import { identityQueries } from '../../../queries/identity';
import KpiStrip, { type KpiStripKpi } from '../../../uikit/composite/KpiStrip';
import MetricCard from '../../../uikit/composite/MetricCard';
import DealFlowChart from '../../../uikit/composite/DealFlowChart';
import CollapsibleSection from '../../../uikit/composite/CollapsibleSection';
import ComingSoon from '../../../uikit/composite/ComingSoon';
import PersonHeader from './PersonHeader';

interface SalesDashboardProps {
  /** Already-resolved active person id (email). Page guards `null`. */
  personId: string;
}

// ---------------------------------------------------------------------------
// Format helpers (sales-flavored)
// ---------------------------------------------------------------------------

/** Compact integer formatter (e.g. 12_345 → "12,345"). */
const formatInt = (n: number): string => new Intl.NumberFormat('en-US').format(Math.round(n));

/** Compact currency, USD assumed for HubSpot Constructor pipeline. Drops cents on values >= 1000. */
const formatCurrency = (n: number): string => {
  const abs = Math.abs(n);
  if (abs >= 1_000_000) return `$${(n / 1_000_000).toFixed(2)}M`;
  if (abs >= 1_000)     return `$${(n / 1_000).toFixed(0)}k`;
  return `$${n.toFixed(0)}`;
};

/** "62% (192/194)" — win rate with deal-count caveat baked in. */
const formatWinRate = (won: number, closed: number): string => {
  if (closed <= 0) return '—';
  const pct = (won / closed) * 100;
  return `${pct.toFixed(0)}% (${formatInt(won)}/${formatInt(closed)})`;
};

/** Signed % delta, used by the pacing band. `null` = no comparison available. */
const formatDeltaPct = (cur: number, prev: number): string | null => {
  if (prev <= 0) return null;
  const diff = ((cur - prev) / prev) * 100;
  const sign = diff >= 0 ? '+' : '';
  return `${sign}${diff.toFixed(0)}%`;
};

/** "Day 14 of 30" string for the pacing band. ISO date strings, inclusive. */
const formatPeriodProgress = (from: string, to: string): string => {
  const start = new Date(from);
  const end   = new Date(to);
  const today = new Date();
  const totalDays   = Math.max(1, Math.round((end.getTime() - start.getTime()) / 86_400_000) + 1);
  const elapsedDays = Math.max(0, Math.min(totalDays, Math.round((today.getTime() - start.getTime()) / 86_400_000) + 1));
  return `Day ${elapsedDays} of ${totalDays}`;
};

/**
 * Subtle "I'm refreshing" cue for sections that already have data and
 * are re-fetching in the background. Replaces the old slice's
 * `revalidating` status with RQ's `isFetching && !isPending`.
 */
const fetchingClass = (isFetching: boolean, isPending: boolean): string =>
  isFetching && !isPending
    ? 'opacity-70 transition-opacity duration-300'
    : 'opacity-100 transition-opacity duration-300';

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

const SalesDashboard: React.FC<SalesDashboardProps> = ({ personId }) => {
  const period = usePeriod();
  const customRange = useAppSelector(selectCustomRange);
  const range = resolveDateRange(period, customRange);

  const handleRangeChange = (range: CustomRange | null): void => {
    if (range) setDateRange(range);
  };

  // Identity is its own query — decouples name/avatar load from metric loads
  // so a slow analytics-api 5xx doesn't hide the person header.
  const personQ = useQuery(identityQueries.byEmail(personId));

  const [kpisQ, prevKpisQ, flowQ, vqQ, actQ] = useQueries({
    queries: [
      crmQueries.kpis(personId, range),
      crmQueries.prevKpis(personId, range),
      crmQueries.flow(personId, range),
      crmQueries.bullet(personId, range, 'quality',  period),
      crmQueries.bullet(personId, range, 'activity', period),
    ],
  });

  const person   = personQ.data ?? null;
  const kpis     = kpisQ.data ?? null;
  const prevKpis = prevKpisQ.data ?? null;
  const flow     = flowQ.data ?? [];
  const vq       = vqQ.data ?? [];
  const act      = actQ.data ?? [];

  // Hero strip cards. Empty array while data loads — KpiStrip falls back to
  // ComingSoon. Pipeline Now is labeled "as of today" so users don't expect
  // it to react to the period selector (it doesn't — server-side max() agg).
  const kpiRows: KpiStripKpi[] = kpis
    ? [
        { metric_key: 'deals_opened',       label: 'Deals Opened',   value: formatInt(kpis.dealsOpened),
          sublabel: 'Created in period',
          description: 'Deals created (createdAt date) within the selected period.' },
        { metric_key: 'deals_closed',       label: 'Deals Closed',   value: formatInt(kpis.dealsClosed),
          sublabel: 'Won + lost in period',
          description: 'Deals whose closedate falls within the selected period.' },
        { metric_key: 'deals_value_closed', label: 'Closed Value',   value: formatCurrency(kpis.dealsValueClosed),
          sublabel: 'Won deals · sum',
          description: 'Sum of properties_amount for deals won in the selected period.' },
        { metric_key: 'win_rate',           label: 'Win Rate',       value: formatWinRate(kpis.dealsWon, kpis.dealsClosed),
          sublabel: 'Won ÷ Closed',
          description: 'Won deals divided by closed deals in the selected period. Deal count shown so 1-of-1 doesn\'t look like a 100% trend.' },
        { metric_key: 'pipeline_value',     label: 'Pipeline Now',   value: formatCurrency(kpis.pipelineValue),
          sublabel: `${formatInt(kpis.pipelineCount)} open · as of today`,
          description: 'Currently-open deals owned by this rep — snapshot, not period-filtered. Sum of properties_amount.' },
      ]
    : [];

  const closedDelta = (kpis && prevKpis)
    ? formatDeltaPct(kpis.dealsValueClosed, prevKpis.dealsValueClosed)
    : null;

  // Identity is critical for rendering a meaningful person header — surface
  // a whole-screen error if it fails. Per-section errors stay local.
  if (personQ.isError) {
    return (
      <div className="flex items-center justify-center p-6">
        <div className="bg-white border border-red-200 rounded-xl px-12 py-8 text-center">
          <div className="text-4xl mb-3">⚠️</div>
          <div className="text-base font-bold text-gray-900 mb-1.5">Unable to load dashboard</div>
          <div className="text-sm text-gray-500 mb-3">Identity service unavailable</div>
          <button
            type="button"
            onClick={() => { void personQ.refetch(); }}
            className="rounded-md border border-red-300 bg-white px-3 py-1 text-sm font-semibold text-red-600 hover:bg-red-50"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-4 p-4">
      {/* Sticky header + period selector — matches EngineeringDashboard layout. */}
      <div className="sticky top-0 z-20 -mx-4 -mt-4 px-4 pt-4 pb-3 bg-background/95 backdrop-blur-sm border-b border-border/60">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <PersonHeader person={person} inline />
          <div className="flex items-center gap-2 flex-shrink-0">
            <PeriodSelectorBar
              period={period}
              customRange={customRange}
              onPeriodChange={changePeriod}
              onRangeChange={handleRangeChange}
            />
          </div>
        </div>
      </div>

      {/* 1. Hero KPI strip — 5 cards. */}
      <div className="bg-white border border-gray-200 rounded-xl overflow-hidden">
        {kpisQ.isPending ? (
          <div className="p-4">
            <ComingSoon variant="row" state="loading" />
          </div>
        ) : kpisQ.isError ? (
          <div className="p-4">
            <ComingSoon variant="row" state="error" onRetry={() => { void kpisQ.refetch(); }} />
          </div>
        ) : (
          <div className={fetchingClass(kpisQ.isFetching, kpisQ.isPending)}>
            <KpiStrip kpis={kpiRows} plain={true} />
          </div>
        )}
      </div>

      {/* 2. Pacing band — closed-$ YoY + period progress. Compact horizontal
          panel; no period chip on the YoY pill (it's intrinsic to the comparison). */}
      {kpis && (
        <div className="bg-white border border-gray-200 rounded-xl px-4 py-3 flex flex-wrap items-center gap-x-6 gap-y-2 text-sm">
          <div className="flex items-center gap-2">
            <span className="text-gray-500">Closed this period</span>
            <span className="font-semibold text-gray-900">{formatCurrency(kpis.dealsValueClosed)}</span>
          </div>
          {prevKpis && (
            <div className="flex items-center gap-2">
              <span className="text-gray-500">vs prior-year same period</span>
              <span className="font-semibold text-gray-900">{formatCurrency(prevKpis.dealsValueClosed)}</span>
              {closedDelta !== null && (
                <span className={`px-1.5 py-0.5 rounded text-xs font-semibold ${
                  closedDelta.startsWith('+') ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'
                }`}>{closedDelta}</span>
              )}
            </div>
          )}
          <div className="flex items-center gap-2 ml-auto">
            <span className="text-gray-500">{formatPeriodProgress(range.from, range.to)}</span>
          </div>
        </div>
      )}

      {/* 3. Velocity & Quality bullet — rep vs team (outcome-side metrics). */}
      <MetricCard
        title="Velocity & Quality"
        metrics={vq}
        columns={2}
        loading={vqQ.isPending}
        revalidating={vqQ.isFetching && !vqQ.isPending}
        errored={vqQ.isError}
        onRetry={() => { void vqQ.refetch(); }}
        personName={person?.name}
      />

      {/* 4. Outreach Activity bullet — rep vs team (effort-side metrics).
          Manager-axis card: how much outreach is this rep doing relative to
          peers, and is the comms-per-won ratio reasonable? */}
      <MetricCard
        title="Outreach Activity"
        metrics={act}
        columns={2}
        loading={actQ.isPending}
        revalidating={actQ.isFetching && !actQ.isPending}
        errored={actQ.isError}
        onRetry={() => { void actQ.refetch(); }}
        personName={person?.name}
      />

      {/* 5. Deal Flow Trend — weekly opened/closed/won. */}
      <CollapsibleSection
        title="Deal Flow"
        subtitle="HubSpot · weekly · opened / closed / won counts"
        storageKey="insight:sales-dashboard:deal-flow"
      >
        <div className="p-4">
          {flowQ.isPending && flow.length === 0 ? (
            <ComingSoon variant="card" state="loading" />
          ) : flowQ.isError ? (
            <ComingSoon variant="card" state="error" onRetry={() => { void flowQ.refetch(); }} />
          ) : (
            <div className={fetchingClass(flowQ.isFetching, flowQ.isPending)}>
              <DealFlowChart data={flow} />
            </div>
          )}
        </div>
      </CollapsibleSection>

    </div>
  );
};

export default SalesDashboard;

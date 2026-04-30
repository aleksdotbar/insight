/**
 * IC Dashboard Actions
 *
 * Queries the Analytics API for IC KPIs, bullet metrics, chart trends, and
 * time-off notice using per-metric OData queries. Person profile is fetched
 * from IdentityApiService; data_availability from ConnectorManagerService.
 *
 * Each section runs its own query pipeline and emits its own
 * IcDashboardSectionLoading / IcDashboardSectionLoaded / IcDashboardSectionFailed
 * events. A slow section never blocks the rendering of the others.
 *
 * Spec: analytics-views-api.md §4.3
 */

import { eventBus, apiRegistry } from '@hai3/react';
import { IcDashboardEvents, type IcDashboardSectionData } from '../events/icDashboardEvents';
import { InsightApiService } from '../api/insightApiService';
import { ConnectorManagerService } from '../api/connectorManagerService';
import { IdentityApiService } from '@/app/api/IdentityApiService';
import { METRIC_REGISTRY } from '../api/metricRegistry';
import { odataDateFilter, odataEscapeValue, type DateRange } from '../utils/periodToDateRange';
import {
  transformIcKpis,
  transformBulletMetrics,
  transformLocTrend,
  transformDeliveryTrend,
  transformTimeOff,
  transformDrill,
} from '../api/transforms';
import type {
  PeriodValue,
} from '../types';
import type {
  RawIcAggregateRow,
  RawBulletAggregateRow,
  RawLocTrendRow,
  RawDeliveryTrendRow,
  RawTimeOffRow,
  RawDrillRow,
} from '../api/rawTypes';

// ---------------------------------------------------------------------------
// Actions
// ---------------------------------------------------------------------------

/**
 * Select a person for the IC Dashboard (stores personId in Redux)
 */
export const selectIcPerson = (personId: string): void => {
  eventBus.emit(IcDashboardEvents.PersonSelected, personId);
};

/**
 * Compute the previous-period range by shifting the given range back by one
 * full period length (in local time). Used for period-over-period deltas.
 *
 * Month/quarter shifts clamp to the last day of the target month so
 * end-of-month boundaries don't roll forward (2026-03-31 minus one month →
 * 2026-02-28, not 2026-03-03).
 */
function previousPeriodRange(range: DateRange, period: PeriodValue): DateRange {
  const shift = (iso: string): string => {
    const [y, m, d] = iso.split('-').map(Number);
    // Construct in local time so DST / timezone edges behave predictably.
    const date = new Date(y, (m ?? 1) - 1, d ?? 1);
    const originalDay = date.getDate();

    const shiftMonths = (months: number): void => {
      date.setDate(1);
      date.setMonth(date.getMonth() - months);
      const lastDay = new Date(date.getFullYear(), date.getMonth() + 1, 0).getDate();
      date.setDate(Math.min(originalDay, lastDay));
    };

    switch (period) {
      case 'week':    date.setDate(date.getDate() - 7);          break;
      case 'month':   shiftMonths(1);                            break;
      case 'quarter': shiftMonths(3);                            break;
      case 'year':    date.setFullYear(date.getFullYear() - 1); break;
    }
    const yy = date.getFullYear();
    const mm = String(date.getMonth() + 1).padStart(2, '0');
    const dd = String(date.getDate()).padStart(2, '0');
    return `${yy}-${mm}-${dd}`;
  };
  return { from: shift(range.from), to: shift(range.to) };
}

// ---------------------------------------------------------------------------
// Per-section runner
// ---------------------------------------------------------------------------

/**
 * Run a single section pipeline: emit Loading, then Loaded on success or
 * Failed on rejection. Sections are completely independent — one failing or
 * hanging never affects the others.
 */
function runSection(
  sectionId: string,
  pipeline: () => Promise<IcDashboardSectionData>,
): void {
  eventBus.emit(IcDashboardEvents.IcDashboardSectionLoading, { sectionId });
  void pipeline()
    .then((data) => {
      eventBus.emit(IcDashboardEvents.IcDashboardSectionLoaded, { sectionId, data });
    })
    .catch((err: unknown) => {
      eventBus.emit(IcDashboardEvents.IcDashboardSectionFailed, {
        sectionId,
        error: err instanceof Error ? err.message : String(err),
      });
    });
}

/**
 * Load IC dashboard data for a person and period.
 *
 * Identity is critical: without it the screen cannot render a name/avatar, so
 * its failure raises a whole-dashboard error. Data sections kick off in
 * parallel but each renders independently — if one hangs, only that section's
 * placeholder shows "loading".
 */
export const loadIcDashboard = (
  personId: string,
  period: PeriodValue,
  range: DateRange,
): void => {
  eventBus.emit(IcDashboardEvents.IcDashboardLoadStarted);

  const api        = apiRegistry.getService(InsightApiService);
  const connectors = apiRegistry.getService(ConnectorManagerService);
  const identity   = apiRegistry.getService(IdentityApiService);

  // ClickHouse gold views store person_id in lowercase (BambooHR emails come
  // in mixed case; the ingestion pipeline normalizes). Force lowercase in
  // OData filters so mixed-case identity emails don't produce empty results.
  const personIdLc       = personId.toLowerCase();
  const personFilter     = `person_id eq '${odataEscapeValue(personIdLc)}' and ${odataDateFilter(range)}`;
  const prevRange        = previousPeriodRange(range, period);
  const prevPersonFilter = `person_id eq '${odataEscapeValue(personIdLc)}' and ${odataDateFilter(prevRange)}`;

  // ---- Identity (critical) -------------------------------------------------
  // Without identity we cannot show name/avatar/role; surface an explicit
  // dashboard-level failure rather than silently substituting the email.
  void identity.getPersonByEmail(personId)
    .then((person) => {
      eventBus.emit(IcDashboardEvents.IcPersonLoaded, person);
    })
    .catch(() => {
      eventBus.emit(IcDashboardEvents.IcDashboardLoadFailed, 'IDENTITY_UNAVAILABLE');
    });

  // ---- Availability (best-effort) ------------------------------------------
  // Failure is silent — banners that depend on availability simply don't
  // render. Matches today's `settled(...)` fallback behaviour.
  void connectors.getDataAvailability()
    .then((availability) => {
      eventBus.emit(IcDashboardEvents.IcDashboardAvailabilityLoaded, availability);
    })
    .catch(() => {
      // swallow — availability is informational
    });

  // ---- Section: kpis (current + previous period combined) ------------------
  runSection('kpis', () =>
    Promise.all([
      api.queryMetric<RawIcAggregateRow>(METRIC_REGISTRY.IC_KPIS, { $filter: personFilter }),
      api.queryMetric<RawIcAggregateRow>(METRIC_REGISTRY.IC_KPIS, { $filter: prevPersonFilter }),
    ]).then(([cur, prev]) => ({
      kind: 'kpis',
      kpis: transformIcKpis(cur.items[0] ?? null, prev.items[0] ?? null, period),
    })),
  );

  // ---- Section: task_delivery ---------------------------------------------
  runSection('task_delivery', () =>
    api.queryMetric<RawBulletAggregateRow>(METRIC_REGISTRY.IC_BULLET_DELIVERY, { $filter: personFilter })
      .then((resp) => ({ kind: 'bullet', sectionId: 'task_delivery', metrics: transformBulletMetrics(resp.items, 'task_delivery', period) })),
  );

  // ---- Section: collaboration ---------------------------------------------
  runSection('collaboration', () =>
    api.queryMetric<RawBulletAggregateRow>(METRIC_REGISTRY.IC_BULLET_COLLAB, { $filter: personFilter })
      .then((resp) => ({ kind: 'bullet', sectionId: 'collaboration', metrics: transformBulletMetrics(resp.items, 'collaboration', period) })),
  );

  // ---- Section: ai_adoption -----------------------------------------------
  runSection('ai_adoption', () =>
    api.queryMetric<RawBulletAggregateRow>(METRIC_REGISTRY.IC_BULLET_AI, { $filter: personFilter })
      .then((resp) => ({ kind: 'bullet', sectionId: 'ai_adoption', metrics: transformBulletMetrics(resp.items, 'ai_adoption', period) })),
  );

  // ---- Section: git_output ------------------------------------------------
  runSection('git_output', () =>
    api.queryMetric<RawBulletAggregateRow>(METRIC_REGISTRY.IC_BULLET_GIT, { $filter: personFilter })
      .then((resp) => ({ kind: 'bullet', sectionId: 'git_output', metrics: transformBulletMetrics(resp.items, 'git_output', period) })),
  );

  // ---- Section: loc_trend -------------------------------------------------
  runSection('loc_trend', () =>
    api.queryMetric<RawLocTrendRow>(METRIC_REGISTRY.IC_CHART_LOC, { $filter: personFilter })
      .then((resp) => ({ kind: 'locTrend', trend: transformLocTrend(resp.items, period) })),
  );

  // ---- Section: delivery_trend --------------------------------------------
  runSection('delivery_trend', () =>
    api.queryMetric<RawDeliveryTrendRow>(METRIC_REGISTRY.IC_CHART_DELIVERY, { $filter: personFilter })
      .then((resp) => ({ kind: 'deliveryTrend', trend: transformDeliveryTrend(resp.items, period) })),
  );

  // ---- Section: time_off --------------------------------------------------
  runSection('time_off', () =>
    api.queryMetric<RawTimeOffRow>(METRIC_REGISTRY.IC_TIMEOFF, { $filter: personFilter })
      .then((resp) => ({ kind: 'timeOff', notice: resp.items[0] ? transformTimeOff(resp.items[0]) : null })),
  );
};

/**
 * Open drill modal — fetches drill detail for a specific metric on demand.
 */
export const openDrill = (personId: string, drillId: string): void => {
  void apiRegistry.getService(InsightApiService)
    .queryMetric<RawDrillRow>(METRIC_REGISTRY.IC_DRILL, {
      $filter: `person_id eq '${odataEscapeValue(personId.toLowerCase())}' and drill_id eq '${odataEscapeValue(drillId)}'`,
    })
    .then((resp) => {
      const drillData = resp.items[0];
      if (drillData) {
        eventBus.emit(IcDashboardEvents.DrillOpened, { drillId, drillData: transformDrill(drillData) });
      }
    })
    .catch(() => {
      // Drill fetch failed — keep the modal closed. UI shows ComingSoon for
      // empty/error content; explicit user-facing toast is future work.
    });
};

/**
 * Close drill modal
 */
export const closeDrill = (): void => {
  eventBus.emit(IcDashboardEvents.DrillClosed);
};

/**
 * IC Dashboard Actions
 *
 * Queries the Analytics API for IC KPIs, bullet metrics, chart trends, and
 * time-off notice using per-metric OData queries. Person profile is fetched
 * from IdentityApiService; data_availability from ConnectorManagerService.
 * All requests are made in parallel.
 *
 * Spec: analytics-views-api.md §4.3
 */

import { eventBus, apiRegistry } from '@hai3/react';
import { IcDashboardEvents } from '../events/icDashboardEvents';
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
  IcDashboardData,
} from '../types';
import type {
  RawIcAggregateRow,
  RawBulletAggregateRow,
  RawLocTrendRow,
  RawDeliveryTrendRow,
  RawTimeOffRow,
  RawDrillRow,
} from '../api/rawTypes';

import { settled, emptyOdata } from '../utils/settledResult';

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

/**
 * Load IC dashboard data for a person and period.
 * Fires 10 parallel requests: 8 metric queries + identity + availability.
 * The KPI query is made twice (current + previous period) so the transform
 * layer can compute period-over-period deltas.
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

  const personFilter     = `person_id eq '${odataEscapeValue(personId)}' and ${odataDateFilter(range)}`;
  const prevRange        = previousPeriodRange(range, period);
  const prevPersonFilter = `person_id eq '${odataEscapeValue(personId)}' and ${odataDateFilter(prevRange)}`;

  void Promise.allSettled([
    api.queryMetric<RawIcAggregateRow>(METRIC_REGISTRY.IC_KPIS,         { $filter: personFilter }),
    api.queryMetric<RawIcAggregateRow>(METRIC_REGISTRY.IC_KPIS,         { $filter: prevPersonFilter }),
    api.queryMetric<RawBulletAggregateRow>(METRIC_REGISTRY.IC_BULLET_DELIVERY, { $filter: personFilter }),
    api.queryMetric<RawBulletAggregateRow>(METRIC_REGISTRY.IC_BULLET_COLLAB,   { $filter: personFilter }),
    api.queryMetric<RawBulletAggregateRow>(METRIC_REGISTRY.IC_BULLET_AI,       { $filter: personFilter }),
    api.queryMetric<RawLocTrendRow>(METRIC_REGISTRY.IC_CHART_LOC,              { $filter: personFilter }),
    api.queryMetric<RawDeliveryTrendRow>(METRIC_REGISTRY.IC_CHART_DELIVERY,    { $filter: personFilter }),
    api.queryMetric<RawTimeOffRow>(METRIC_REGISTRY.IC_TIMEOFF,                 { $filter: personFilter }),
    identity.getPersonByEmail(personId),
    connectors.getDataAvailability(),
  ])
    .then(([r0, r1, r2, r3, r4, r5, r6, r7, r8, r9]) => {
      const curKpisResp      = settled(r0, emptyOdata<RawIcAggregateRow>(), 'IC_KPIS');
      const prevKpisResp     = settled(r1, emptyOdata<RawIcAggregateRow>(), 'IC_KPIS_PREV');
      const deliveryResp     = settled(r2, emptyOdata<RawBulletAggregateRow>(), 'IC_BULLET_DELIVERY');
      const collabResp       = settled(r3, emptyOdata<RawBulletAggregateRow>(), 'IC_BULLET_COLLAB');
      const aiResp           = settled(r4, emptyOdata<RawBulletAggregateRow>(), 'IC_BULLET_AI');
      const locResp          = settled(r5, emptyOdata<RawLocTrendRow>(), 'IC_CHART_LOC');
      const deliveryTrendResp = settled(r6, emptyOdata<RawDeliveryTrendRow>(), 'IC_CHART_DELIVERY');
      const timeOffResp      = settled(r7, emptyOdata<RawTimeOffRow>(), 'IC_TIMEOFF');
      const availability     = settled(r9, { git: 'no-connector', tasks: 'no-connector', ci: 'no-connector', comms: 'no-connector', hr: 'no-connector', ai: 'no-connector' } as unknown as Awaited<ReturnType<typeof connectors.getDataAvailability>>, 'CONNECTORS');

      // Track which per-section queries actually rejected (vs returned empty).
      // Section IDs match the string keys used by the screen when filtering
      // bulletMetrics and charts, so the UI can render an error-state
      // placeholder with retry for failed sections only.
      const erroredSections: string[] = [];
      if (r0.status !== 'fulfilled') erroredSections.push('kpis');
      if (r2.status !== 'fulfilled') erroredSections.push('task_delivery');
      if (r3.status !== 'fulfilled') erroredSections.push('collaboration');
      if (r4.status !== 'fulfilled') erroredSections.push('ai_adoption');
      if (r5.status !== 'fulfilled') erroredSections.push('loc_trend');
      if (r6.status !== 'fulfilled') erroredSections.push('delivery_trend');
      if (r7.status !== 'fulfilled') erroredSections.push('time_off');

      // Identity is non-negotiable for this screen: without it we cannot show
      // a name / avatar / correct role. Surface the failure explicitly rather
      // than silently substituting the email as display name.
      if (r8.status !== 'fulfilled') {
        eventBus.emit(IcDashboardEvents.IcDashboardLoadFailed, 'IDENTITY_UNAVAILABLE');
        return;
      }
      const person = r8.value;

      const data: IcDashboardData = {
        kpis: transformIcKpis(curKpisResp.items[0] ?? null, prevKpisResp.items[0] ?? null, period),
        bulletMetrics: [
          ...transformBulletMetrics(deliveryResp.items, 'task_delivery', period),
          ...transformBulletMetrics(collabResp.items,   'collaboration', period),
          ...transformBulletMetrics(aiResp.items,       'ai_adoption',   period),
        ],
        charts: {
          locTrend:      transformLocTrend(locResp.items, period),
          deliveryTrend: transformDeliveryTrend(deliveryTrendResp.items, period),
        },
        timeOffNotice: timeOffResp.items[0] ? transformTimeOff(timeOffResp.items[0]) : null,
        drills:        {},
      };

      eventBus.emit(IcDashboardEvents.IcDashboardLoaded, data);
      eventBus.emit(IcDashboardEvents.IcPersonLoaded, person);
      eventBus.emit(IcDashboardEvents.IcDashboardAvailabilityLoaded, availability);
      eventBus.emit(IcDashboardEvents.IcDashboardSectionsErrored, erroredSections);
    })
    .catch((err: unknown) => {
      // Promise.allSettled never rejects, but guard against unexpected
      // errors in the transform/emit logic above.
      eventBus.emit(IcDashboardEvents.IcDashboardLoadFailed, String(err));
    });
};

/**
 * Open drill modal — fetches drill detail for a specific metric on demand.
 */
export const openDrill = (personId: string, drillId: string): void => {
  void apiRegistry.getService(InsightApiService)
    .queryMetric<RawDrillRow>(METRIC_REGISTRY.IC_DRILL, {
      $filter: `person_id eq '${odataEscapeValue(personId)}' and drill_id eq '${odataEscapeValue(drillId)}'`,
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

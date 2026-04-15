/**
 * IC Dashboard Actions
 *
 * Queries the Analytics API for IC KPIs, bullet metrics, chart trends, and
 * time-off notice using per-metric OData queries. Person profile is fetched
 * from IdentityResolutionService; data_availability from ConnectorManagerService.
 * All requests are made in parallel.
 *
 * Spec: analytics-views-api.md §4.3
 */

import { eventBus, apiRegistry } from '@hai3/react';
import { IcDashboardEvents } from '../events/icDashboardEvents';
import { InsightApiService } from '../api/insightApiService';
import { ConnectorManagerService } from '../api/connectorManagerService';
import { IdentityResolutionService } from '../api/identityResolutionService';
import { METRIC_REGISTRY } from '../api/metricRegistry';
import { odataDateFilter, odataEscapeValue, periodToDateRange } from '../utils/periodToDateRange';
import {
  transformIcKpis,
  transformBulletMetrics,
  transformLocTrend,
  transformDeliveryTrend,
} from '../api/transforms';
import type {
  PeriodValue,
  TimeOffNotice,
  DrillData,
  IcDashboardData,
} from '../types';
import type {
  RawIcAggregateRow,
  RawBulletAggregateRow,
  RawLocTrendRow,
  RawDeliveryTrendRow,
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
 * Compute an OData date filter for the previous period (shifted back by one
 * full period length relative to the current filter range).
 */
function previousPeriodFilter(personId: string, period: PeriodValue): string {
  const { from, to } = periodToDateRange(period);

  const shiftBack = (isoDate: string): string => {
    const d = new Date(isoDate);
    switch (period) {
      case 'week':    d.setUTCDate(d.getUTCDate() - 7);       break;
      case 'month':   d.setUTCMonth(d.getUTCMonth() - 1);     break;
      case 'quarter': d.setUTCMonth(d.getUTCMonth() - 3);     break;
      case 'year':    d.setUTCFullYear(d.getUTCFullYear() - 1); break;
    }
    return d.toISOString().slice(0, 10);
  };

  return `person_id eq '${personId}' and metric_date ge '${shiftBack(from)}' and metric_date lt '${shiftBack(to)}'`;
}

/**
 * Load IC dashboard data for a person and period.
 * Fires 10 parallel requests: 8 metric queries + identity + availability.
 * The KPI query is made twice (current + previous period) so the transform
 * layer can compute period-over-period deltas.
 */
export const loadIcDashboard = (personId: string, period: PeriodValue): void => {
  eventBus.emit(IcDashboardEvents.IcDashboardLoadStarted);

  const api        = apiRegistry.getService(InsightApiService);
  const connectors = apiRegistry.getService(ConnectorManagerService);
  const identity   = apiRegistry.getService(IdentityResolutionService);

  const personFilter     = `person_id eq '${odataEscapeValue(personId)}' and ${odataDateFilter(period)}`;
  const prevPersonFilter = previousPeriodFilter(odataEscapeValue(personId), period);

  void Promise.allSettled([
    api.queryMetric<RawIcAggregateRow>(METRIC_REGISTRY.IC_KPIS,         { $filter: personFilter }),
    api.queryMetric<RawIcAggregateRow>(METRIC_REGISTRY.IC_KPIS,         { $filter: prevPersonFilter }),
    api.queryMetric<RawBulletAggregateRow>(METRIC_REGISTRY.IC_BULLET_DELIVERY, { $filter: personFilter }),
    api.queryMetric<RawBulletAggregateRow>(METRIC_REGISTRY.IC_BULLET_COLLAB,   { $filter: personFilter }),
    api.queryMetric<RawBulletAggregateRow>(METRIC_REGISTRY.IC_BULLET_AI,       { $filter: personFilter }),
    api.queryMetric<RawLocTrendRow>(METRIC_REGISTRY.IC_CHART_LOC,              { $filter: personFilter }),
    api.queryMetric<RawDeliveryTrendRow>(METRIC_REGISTRY.IC_CHART_DELIVERY,    { $filter: personFilter }),
    api.queryMetric<TimeOffNotice>(METRIC_REGISTRY.IC_TIMEOFF,                 { $filter: personFilter }),
    identity.getPerson(personId),
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
      const timeOffResp      = settled(r7, emptyOdata<TimeOffNotice>(), 'IC_TIMEOFF');
      const person           = settled(r8, { person_id: personId, display_name: personId, name: personId, role: '', seniority: '' } as unknown as Awaited<ReturnType<typeof identity.getPerson>>, 'IDENTITY');
      const availability     = settled(r9, { git: 'no-connector', tasks: 'no-connector', ci: 'no-connector', comms: 'no-connector', hr: 'no-connector', ai: 'no-connector' } as unknown as Awaited<ReturnType<typeof connectors.getDataAvailability>>, 'CONNECTORS');

      const data: IcDashboardData = {
        kpis: transformIcKpis(curKpisResp.items[0] ?? null, prevKpisResp.items[0] ?? null, period),
        bulletMetrics: [
          ...transformBulletMetrics(deliveryResp.items, 'task_delivery', period),
          ...transformBulletMetrics(collabResp.items,   'collab',        period),
          ...transformBulletMetrics(aiResp.items,       'ai_tools',      period),
        ],
        charts: {
          locTrend:      transformLocTrend(locResp.items, period),
          deliveryTrend: transformDeliveryTrend(deliveryTrendResp.items, period),
        },
        timeOffNotice: timeOffResp.items[0] ?? null,
        drills:        {},
      };

      eventBus.emit(IcDashboardEvents.IcDashboardLoaded, data);
      eventBus.emit(IcDashboardEvents.IcPersonLoaded, person);
      eventBus.emit(IcDashboardEvents.IcDashboardAvailabilityLoaded, availability);
    });
};

/**
 * Open drill modal — fetches drill detail for a specific metric on demand.
 */
export const openDrill = (personId: string, drillId: string): void => {
  void apiRegistry.getService(InsightApiService)
    .queryMetric<DrillData>(METRIC_REGISTRY.IC_DRILL, {
      $filter: `person_id eq '${odataEscapeValue(personId)}' and drill_id eq '${odataEscapeValue(drillId)}'`,
    })
    .then((resp) => {
      const drillData = resp.items[0];
      if (drillData) {
        eventBus.emit(IcDashboardEvents.DrillOpened, { drillId, drillData });
      }
    })
    .catch((err: unknown) => {
      console.error('Failed to load drill data:', err);
    });
};

/**
 * Close drill modal
 */
export const closeDrill = (): void => {
  eventBus.emit(IcDashboardEvents.DrillClosed);
};

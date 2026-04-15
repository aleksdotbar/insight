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
  const fromDate = new Date(from);
  const toDate   = new Date(to);
  const spanMs   = toDate.getTime() - fromDate.getTime();

  const prevTo   = new Date(fromDate);
  const prevFrom = new Date(prevTo.getTime() - spanMs);

  const iso = (d: Date): string => d.toISOString().slice(0, 10);
  return `person_id eq '${personId}' and metric_date ge '${iso(prevFrom)}' and metric_date lt '${iso(prevTo)}'`;
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

  void Promise.all([
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
    .then(([curKpisResp, prevKpisResp, deliveryResp, collabResp, aiResp, locResp, deliveryTrendResp, timeOffResp, person, availability]) => {
      const currentRow  = curKpisResp.items[0] ?? null;
      const previousRow = prevKpisResp.items[0] ?? null;

      const data: IcDashboardData = {
        kpis: transformIcKpis(currentRow, previousRow, period),
        bulletMetrics: [
          ...transformBulletMetrics(deliveryResp.items, 'task_delivery', period),
          ...transformBulletMetrics(collabResp.items,   'collaboration', period),
          ...transformBulletMetrics(aiResp.items,       'ai_adoption',   period),
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
    })
    .catch((err: unknown) => {
      eventBus.emit(IcDashboardEvents.IcDashboardLoadFailed, String(err));
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

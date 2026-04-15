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
import { odataDateFilter, odataEscapeValue } from '../utils/periodToDateRange';
import type {
  PeriodValue,
  IcKpi,
  BulletMetric,
  LocDataPoint,
  DeliveryDataPoint,
  TimeOffNotice,
  DrillData,
  IcDashboardData,
} from '../types';

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
 * Load IC dashboard data for a person and period.
 * Fires 9 parallel requests: 7 metric queries + identity + availability.
 */
export const loadIcDashboard = (personId: string, period: PeriodValue): void => {
  eventBus.emit(IcDashboardEvents.IcDashboardLoadStarted);

  const api        = apiRegistry.getService(InsightApiService);
  const connectors = apiRegistry.getService(ConnectorManagerService);
  const identity   = apiRegistry.getService(IdentityResolutionService);

  const personFilter = `person_id eq '${odataEscapeValue(personId)}' and ${odataDateFilter(period)}`;

  void Promise.all([
    api.queryMetric<IcKpi>(METRIC_REGISTRY.IC_KPIS,              { $filter: personFilter }),
    api.queryMetric<BulletMetric>(METRIC_REGISTRY.IC_BULLET_DELIVERY, { $filter: personFilter }),
    api.queryMetric<BulletMetric>(METRIC_REGISTRY.IC_BULLET_COLLAB,   { $filter: personFilter }),
    api.queryMetric<BulletMetric>(METRIC_REGISTRY.IC_BULLET_AI,       { $filter: personFilter }),
    api.queryMetric<LocDataPoint>(METRIC_REGISTRY.IC_CHART_LOC,       { $filter: personFilter }),
    api.queryMetric<DeliveryDataPoint>(METRIC_REGISTRY.IC_CHART_DELIVERY, { $filter: personFilter }),
    api.queryMetric<TimeOffNotice>(METRIC_REGISTRY.IC_TIMEOFF,        { $filter: personFilter }),
    identity.getPerson(personId),
    connectors.getDataAvailability(),
  ])
    .then(([kpisResp, deliveryResp, collabResp, aiResp, locResp, deliveryTrendResp, timeOffResp, person, availability]) => {
      const data: IcDashboardData = {
        kpis:          kpisResp.items,
        bulletMetrics: [
          ...deliveryResp.items,
          ...collabResp.items,
          ...aiResp.items,
        ],
        charts: {
          locTrend:      locResp.items,
          deliveryTrend: deliveryTrendResp.items,
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

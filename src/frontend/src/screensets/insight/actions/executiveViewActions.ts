/**
 * Executive View Actions
 *
 * Queries the Analytics API for executive summary data using per-metric OData
 * queries, then assembles ExecViewData from the responses.
 * Also fetches data_availability separately via ConnectorManagerService.
 *
 * Spec: analytics-views-api.md §4.1
 */

import { eventBus, apiRegistry } from '@hai3/react';
import { ExecutiveViewEvents } from '../events/executiveViewEvents';
import { InsightApiService } from '../api/insightApiService';
import { ConnectorManagerService } from '../api/connectorManagerService';
import { METRIC_REGISTRY } from '../api/metricRegistry';
import { odataDateFilter } from '../utils/periodToDateRange';
import { transformExecRows } from '../api/transforms';
import type { PeriodValue, ExecViewData } from '../types';
import type { RawExecSummaryRow } from '../api/rawTypes';
import { EXEC_VIEW_CONFIG } from '../api/mocks/fixtures/executive-view';

export const loadExecutiveView = (period: PeriodValue): void => {
  eventBus.emit(ExecutiveViewEvents.ExecutiveViewLoadStarted);

  const api        = apiRegistry.getService(InsightApiService);
  const connectors = apiRegistry.getService(ConnectorManagerService);
  const filter     = odataDateFilter(period);

  // Critical path — metric data only
  void api.queryMetric<RawExecSummaryRow>(METRIC_REGISTRY.EXEC_SUMMARY, {
    $filter:  filter,
    $orderby: 'org_unit_name asc',
    $top:     200,
  })
    .then((summaryResp) => {
      const teams = transformExecRows(summaryResp.items, EXEC_VIEW_CONFIG.column_thresholds);

      const withValue = <T>(vals: (T | null)[]): T[] =>
        vals.filter((v): v is T => v !== null);

      const buildVals = withValue(teams.map((t) => t.build_success_pct));
      const bugVals   = withValue(teams.map((t) => t.tasks_closed));
      const avg       = (arr: number[]): number | null =>
        arr.length ? Math.round(arr.reduce((a, b) => a + b, 0) / arr.length) : null;

      const data: ExecViewData = {
        teams,
        orgKpis: {
          avgBuildSuccess:    avg(buildVals),
          avgAiAdoption:      avg(withValue(teams.map((t) => t.ai_adoption_pct))),
          avgFocus:           avg(withValue(teams.map((t) => t.focus_time_pct))),
          bugResolutionScore: bugVals.length ? Math.min(100, avg(bugVals) ?? 0) : null,
          prCycleScore:       teams.length
            ? Math.max(0, 100 - (avg(withValue(teams.map((t) => t.pr_cycle_time_h))) ?? 0))
            : null,
        },
        config: EXEC_VIEW_CONFIG,
      };

      eventBus.emit(ExecutiveViewEvents.ExecutiveViewLoaded, data);
    })
    .catch((err: unknown) => {
      eventBus.emit(ExecutiveViewEvents.ExecutiveViewLoadFailed, String(err));
    });

  // Best-effort — availability does not block the view
  void connectors.getDataAvailability()
    .then((availability) => {
      eventBus.emit(ExecutiveViewEvents.ExecutiveViewAvailabilityLoaded, availability);
    })
    .catch(() => { /* availability is optional */ });
};

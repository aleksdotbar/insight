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
import type { PeriodValue, ExecTeamRow, ExecViewData } from '../types';
import { EXEC_VIEW_CONFIG } from '../api/mocks/fixtures/executive-view';

export const loadExecutiveView = (period: PeriodValue): void => {
  eventBus.emit(ExecutiveViewEvents.ExecutiveViewLoadStarted);

  const api        = apiRegistry.getService(InsightApiService);
  const connectors = apiRegistry.getService(ConnectorManagerService);
  const filter     = odataDateFilter(period);

  void Promise.all([
    api.queryMetric<ExecTeamRow>(METRIC_REGISTRY.EXEC_SUMMARY, {
      $filter:  filter,
      $orderby: 'org_unit_name asc',
      $top:     200,
    }),
    connectors.getDataAvailability(),
  ])
    .then(([summaryResp, availability]) => {
      const teams = summaryResp.items;

      // Org KPIs are derived from team rows
      const withValue = <T>(vals: (T | null)[]): T[] =>
        vals.filter((v): v is T => v !== null);

      const buildVals  = withValue(teams.map((t) => t.build_success_pct));
      const bugVals    = withValue(teams.map((t) => t.tasks_closed));
      const avg        = (arr: number[]) =>
        arr.length ? Math.round(arr.reduce((a, b) => a + b, 0) / arr.length) : null;

      const data: ExecViewData = {
        teams,
        orgKpis: {
          avgBuildSuccess:    avg(buildVals),
          avgAiAdoption:      avg(teams.map((t) => t.ai_adoption_pct)) ?? 0,
          avgFocus:           avg(teams.map((t) => t.focus_time_pct))  ?? 0,
          bugResolutionScore: bugVals.length ? Math.min(100, Math.round(avg(bugVals) ?? 0)) : null,
          prCycleScore:       Math.max(0, 100 - Math.round((avg(teams.map((t) => t.pr_cycle_time_h)) ?? 24))),
        },
        config: EXEC_VIEW_CONFIG,
      };

      eventBus.emit(ExecutiveViewEvents.ExecutiveViewLoaded, data);
      eventBus.emit(ExecutiveViewEvents.ExecutiveViewAvailabilityLoaded, availability);
    })
    .catch((err: unknown) => {
      eventBus.emit(ExecutiveViewEvents.ExecutiveViewLoadFailed, String(err));
    });
};

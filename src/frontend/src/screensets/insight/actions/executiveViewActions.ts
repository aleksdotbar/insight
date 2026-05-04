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
import { type DateRange } from '../utils/periodToDateRange';
import { transformExecRows } from '../api/transforms';
import type { ExecViewData } from '../types';
import type { RawExecSummaryRow } from '../api/rawTypes';
import { EXEC_VIEW_CONFIG } from '../api/viewConfigs';
import { settled, emptyOdata } from '../utils/settledResult';

export const loadExecutiveView = (range: DateRange): void => {
  eventBus.emit(ExecutiveViewEvents.ExecutiveViewLoadStarted);

  const api        = apiRegistry.getService(InsightApiService);
  const connectors = apiRegistry.getService(ConnectorManagerService);

  void Promise.allSettled([
    api.queryMetric<RawExecSummaryRow>(METRIC_REGISTRY.EXEC_SUMMARY, range, {
      $orderby: 'org_unit_name asc',
      $top:     200,
    }),
    connectors.getDataAvailability(),
  ])
    .then(([summaryResult, availResult]) => {
      const summaryResp = settled(summaryResult, emptyOdata<RawExecSummaryRow>(), 'EXEC_SUMMARY');
      const teams = transformExecRows(summaryResp.items, EXEC_VIEW_CONFIG.column_thresholds);

      const withValue = <T>(vals: (T | null)[]): T[] =>
        vals.filter((v): v is T => v !== null);

      const avg = (arr: number[]): number | null =>
        arr.length ? Math.round(arr.reduce((a, b) => a + b, 0) / arr.length) : null;

      const data: ExecViewData = {
        teams,
        orgKpis: {
          avgBuildSuccess: avg(withValue(teams.map((t) => t.build_success_pct))),
          avgAiAdoption:   avg(withValue(teams.map((t) => t.ai_adoption_pct))),
          avgFocus:        avg(withValue(teams.map((t) => t.focus_time_pct))),
        },
        config: EXEC_VIEW_CONFIG,
      };

      eventBus.emit(ExecutiveViewEvents.ExecutiveViewLoaded, data);

      const availability = settled(
        availResult,
        { git: 'no-connector', tasks: 'no-connector', ci: 'no-connector', comms: 'no-connector', hr: 'no-connector', ai: 'no-connector' } as const,
        'CONNECTORS',
      );
      eventBus.emit(ExecutiveViewEvents.ExecutiveViewAvailabilityLoaded, availability);
    })
    .catch((err: unknown) => {
      // Promise.allSettled never rejects, but guard against unexpected
      // errors in the transform/emit logic above.
      eventBus.emit(ExecutiveViewEvents.ExecutiveViewLoadFailed, String(err));
    });
};

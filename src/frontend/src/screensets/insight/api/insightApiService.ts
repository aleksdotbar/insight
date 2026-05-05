/**
 * InsightApiService
 *
 * Thin wrapper around the Analytics API.
 * All data queries use POST /api/analytics/v1/metrics/{id}/query with OData params.
 *
 * Spec: docs/components/backend/specs/analytics-views-api.md
 *
 * Mocks are OFF by default and only loaded when VITE_ENABLE_MOCKS=true in
 * a dev build. The import is dynamic so the mock module tree is tree-shaken
 * out of prod bundles entirely — there is no way for fake analytics data
 * to leak into a deployed build.
 */

import { BaseApiService, RestProtocol, RestMockPlugin, apiRegistry } from '@hai3/react';
import { AuthPlugin } from '@/app/plugins/AuthPlugin';
import { mocksEnabled } from '@/app/config/mocksEnabled';
import { insightMockMap } from './mocks';
import type { ODataParams, ODataResponse } from '../types';
import type { DashboardData, SpeedData } from '../types';
import { odataDateFilter, type DateRange } from '../utils/periodToDateRange';

export class InsightApiService extends BaseApiService {
  constructor() {
    const restProtocol = new RestProtocol();

    super({ baseURL: '/api/analytics/v1' }, restProtocol);

    // Mock plugin attached directly to restProtocol.plugins (was: registered
    // via this.registerPlugin and activated by toggleMockMode → syncMockPlugins).
    // The framework path races: syncMockPlugins iterates apiRegistry.getAll()
    // before InsightApiService is fully instantiated on first dispatch, so
    // the mock plugin never makes it into the active chain. Direct add
    // avoids that ordering hazard. Mock chain runs BEFORE AuthPlugin so a
    // matched route short-circuits without needing a token.
    // Tree-shaken from prod via the mocksEnabled() === false constant fold.
    if (mocksEnabled()) {
      restProtocol.plugins.add(new RestMockPlugin({ mockMap: insightMockMap, delay: 100 }));
    }
    restProtocol.plugins.add(new AuthPlugin());
  }

  /**
   * Execute a metric query bound to a period.
   *
   * Every dashboard-facing metric is period-aware: the gold bullet/aggregate
   * views are aggregations over a time window, so a query without a
   * `metric_date` filter implicitly returns "all-time" — almost never what
   * a screen wants. This method makes the period mandatory and prepends it
   * to any caller-supplied `$filter` so it cannot be forgotten.
   *
   * Use this for **all** dashboard data fetches. The raw
   * {@link queryMetricRaw} is reserved for the rare case where a metric is
   * genuinely period-independent (e.g. catalog enumeration).
   *
   * @param metricId  UUID from METRIC_REGISTRY
   * @param range     Resolved {from, to} date range (use
   *                  `resolveDateRange(period, customRange)`).
   * @param params    Extra OData params. Any `$filter` here is ANDed AFTER
   *                  the date filter — pass scopes like
   *                  `person_id eq '…'` or `org_unit_id eq '…'` here.
   */
  async queryMetric<T>(
    metricId: string,
    range: DateRange,
    params?: ODataParams,
  ): Promise<ODataResponse<T>> {
    const dateFilter = odataDateFilter(range);
    const combined = params?.$filter
      ? `${dateFilter} and ${params.$filter}`
      : dateFilter;
    return this.queryMetricRaw<T>(metricId, { ...params, $filter: combined });
  }

  /**
   * Escape hatch for period-independent metric queries. Prefer
   * {@link queryMetric} for every dashboard call — forgetting the period
   * silently returns all-time data and breaks any "this week / month"
   * expectation on the UI.
   */
  async queryMetricRaw<T>(metricId: string, params: ODataParams): Promise<ODataResponse<T>> {
    return this.protocol(RestProtocol).post<ODataResponse<T>>(
      `/metrics/${metricId}/query`,
      params,
    );
  }

  // ---------------------------------------------------------------------------
  // Legacy endpoints kept for Dashboard and Speed screens — not yet migrated
  // ---------------------------------------------------------------------------

  async getDashboard(): Promise<DashboardData> {
    return this.protocol(RestProtocol).get<DashboardData>('/dashboard');
  }

  async getSpeed(): Promise<SpeedData> {
    return this.protocol(RestProtocol).get<SpeedData>('/speed');
  }
}

apiRegistry.register(InsightApiService);

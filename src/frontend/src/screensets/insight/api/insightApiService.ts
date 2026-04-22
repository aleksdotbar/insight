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
import type { ODataParams, ODataResponse } from '../types';
import type { DashboardData, SpeedData } from '../types';

export class InsightApiService extends BaseApiService {
  constructor() {
    const restProtocol = new RestProtocol();

    super({ baseURL: '/api/analytics/v1' }, restProtocol);

    if (mocksEnabled()) {
      void import('./mocks').then(({ insightMockMap }) => {
        this.registerPlugin(
          restProtocol,
          new RestMockPlugin({ mockMap: insightMockMap, delay: 100 }),
        );
      });
    }
    restProtocol.plugins.add(new AuthPlugin());
  }

  /**
   * Execute an OData query against a seeded metric.
   *
   * @param metricId  UUID from METRIC_REGISTRY
   * @param params    OData params ($filter, $orderby, $top, $select, $skip)
   */
  async queryMetric<T>(metricId: string, params: ODataParams): Promise<ODataResponse<T>> {
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

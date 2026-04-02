/**
 * insight API Service
 * Domain-specific API service for this screenset
 */

import { BaseApiService, RestProtocol, RestMockPlugin, apiRegistry } from '@hai3/react';
import { insightMockMap } from './mocks';
import type { DashboardData, SpeedData } from '../types';

/**
 * insight API Service
 * Extends BaseApiService with domain-specific methods
 */
export class InsightApiService extends BaseApiService {
  constructor() {
    const restProtocol = new RestProtocol();

    super({ baseURL: '/api/insight' }, restProtocol);

    // Register mock plugin (framework controls when it's active based on mock mode toggle)
    this.registerPlugin(
      restProtocol,
      new RestMockPlugin({
        mockMap: insightMockMap,
        delay: 100,
      })
    );
  }

  /**
   * Get dashboard analytics data
   */
  async getDashboard(): Promise<DashboardData> {
    return this.protocol(RestProtocol).get<DashboardData>('/dashboard');
  }


  /**
   * Get speed gauge data
   */
  async getSpeed(): Promise<SpeedData> {
    return this.protocol(RestProtocol).get<SpeedData>('/speed');
  }
}

// Register API service using class-based registration
apiRegistry.register(InsightApiService);

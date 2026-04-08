/**
 * insight API Service
 * Domain-specific API service for this screenset
 */

import { BaseApiService, RestProtocol, RestMockPlugin, apiRegistry } from '@hai3/react';
import { insightMockMap } from './mocks';
import type { DashboardData, SpeedData, ExecViewData, TeamViewData, IcDashboardData, DrillData, PeriodValue } from '../types';

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

  /**
   * Get executive view data for a given period
   */
  async getExecutiveViewData(period: PeriodValue): Promise<ExecViewData> {
    return this.protocol(RestProtocol).get<ExecViewData>(`/executive-view/${period}`);
  }

  /**
   * Get team view data for a given period
   */
  async getTeamViewData(period: PeriodValue): Promise<TeamViewData> {
    return this.protocol(RestProtocol).get<TeamViewData>(`/team-view/${period}`);
  }

  /**
   * Get IC dashboard data for a given person and period
   */
  async getIcDashboardData(personId: string, period: PeriodValue): Promise<IcDashboardData | null> {
    return this.protocol(RestProtocol).get<IcDashboardData | null>(`/ic-dashboard/${personId}/${period}`);
  }

  /**
   * Get drill data for a given person and drill ID
   */
  async getIcDrillData(personId: string, drillId: string): Promise<DrillData> {
    return this.protocol(RestProtocol).get<DrillData>(`/ic-dashboard/${personId}/drill/${drillId}`);
  }

  /**
   * Get team-level drill data for a given drill ID and period
   */
  async getTeamDrillData(drillId: string, period: PeriodValue): Promise<DrillData> {
    return this.protocol(RestProtocol).get<DrillData>(`/team-view/drill/${drillId}/${period}`);
  }
}

// Register API service using class-based registration
apiRegistry.register(InsightApiService);

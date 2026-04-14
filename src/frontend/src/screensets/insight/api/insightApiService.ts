/**
 * insight API Service
 * Domain-specific API service for this screenset
 */

import { BaseApiService, RestProtocol, RestMockPlugin, apiRegistry } from '@hai3/react';
import { insightMockMap } from './mocks';
import { AuthPlugin } from '@/app/plugins/AuthPlugin';
import type { DashboardData, SpeedData, ExecViewData, TeamViewData, IcDashboardData, DrillData, PeriodValue } from '../types';

/**
 * insight API Service
 * Extends BaseApiService with domain-specific methods
 */
export class InsightApiService extends BaseApiService {
  constructor() {
    const restProtocol = new RestProtocol();

    super({ baseURL: '/api/v1/analytics' }, restProtocol);

    // Register mock plugin (framework controls when it's active based on mock mode toggle)
    this.registerPlugin(
      restProtocol,
      new RestMockPlugin({
        mockMap: insightMockMap,
        delay: 100,
      })
    );

    // Inject auth headers on every request (RestPlugin — added to protocol, not service)
    restProtocol.plugins.add(new AuthPlugin());
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
    return this.protocol(RestProtocol).get<ExecViewData>(`/views/executive?period=${period}`);
  }

  /**
   * Get team view data for a given period
   */
  async getTeamViewData(teamId: string, period: PeriodValue): Promise<TeamViewData> {
    return this.protocol(RestProtocol).get<TeamViewData>(`/views/team/${teamId}?period=${period}`);
  }

  /**
   * Get IC dashboard data for a given person and period
   */
  async getIcDashboardData(personId: string, period: PeriodValue): Promise<IcDashboardData | null> {
    return this.protocol(RestProtocol).get<IcDashboardData | null>(`/views/ic/${personId}?period=${period}`);
  }

  /**
   * Get drill data for a given person and drill ID
   */
  async getIcDrillData(personId: string, drillId: string): Promise<DrillData> {
    return this.protocol(RestProtocol).get<DrillData>(`/views/ic/${personId}/drill/${drillId}`);
  }

  /**
   * Get team-level drill data for a given drill ID and period
   */
  async getTeamDrillData(drillId: string, period: PeriodValue): Promise<DrillData> {
    return this.protocol(RestProtocol).get<DrillData>(`/views/team/drill/${drillId}?period=${period}`);
  }
}

// Register API service using class-based registration
apiRegistry.register(InsightApiService);

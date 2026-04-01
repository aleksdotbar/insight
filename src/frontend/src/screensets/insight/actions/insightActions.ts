/**
 * insight Actions
 * Emit events AND interact with APIs (Flux pattern)
 * Following Flux: Actions emit events for effects to update Redux, and call APIs
 */

import { eventBus, apiRegistry } from '@hai3/react';
import { InsightEvents } from '../events/insightEvents';
import { InsightApiService } from '../api/insightApiService';

/**
 * Fetch dashboard analytics data
 */
export const fetchDashboard = (): void => {
  void apiRegistry.getService(InsightApiService).getDashboard().then((data) => {
    eventBus.emit(InsightEvents.DashboardLoaded, data);
  });
};


/**
 * Fetch speed gauge data
 */
export const fetchSpeed = (): void => {
  void apiRegistry.getService(InsightApiService).getSpeed().then((data) => {
    eventBus.emit(InsightEvents.SpeedLoaded, data);
  });
};

/**
 * IC Dashboard Actions
 * Emit events AND interact with APIs (Flux pattern)
 * Following Flux: Actions emit events for effects to update Redux, and call APIs
 */

import { eventBus, apiRegistry } from '@hai3/react';
import { IcDashboardEvents } from '../events/icDashboardEvents';
import { InsightApiService } from '../api/insightApiService';
import type { PeriodValue } from '../types';

/**
 * Select a person for the IC Dashboard (stores personId in Redux)
 */
export const selectIcPerson = (personId: string): void => {
  eventBus.emit(IcDashboardEvents.PersonSelected, personId);
};

/**
 * Load IC dashboard data for a person and period
 */
export const loadIcDashboard = (personId: string, period: PeriodValue): void => {
  eventBus.emit(IcDashboardEvents.IcDashboardLoadStarted);
  void apiRegistry.getService(InsightApiService).getIcDashboardData(personId, period)
    .then((data) => {
      if (data) {
        eventBus.emit(IcDashboardEvents.IcDashboardLoaded, data);
      } else {
        eventBus.emit(IcDashboardEvents.IcDashboardLoadFailed, 'Person not found');
      }
    })
    .catch((err: unknown) => {
      eventBus.emit(IcDashboardEvents.IcDashboardLoadFailed, String(err));
    });
};

/**
 * Open drill modal for a specific metric
 */
export const openDrill = (personId: string, drillId: string): void => {
  void apiRegistry.getService(InsightApiService).getIcDrillData(personId, drillId)
    .then((drillData) => {
      eventBus.emit(IcDashboardEvents.DrillOpened, { drillId, drillData });
    });
};

/**
 * Close drill modal
 */
export const closeDrill = (): void => {
  eventBus.emit(IcDashboardEvents.DrillClosed);
};

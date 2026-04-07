/**
 * Executive View Actions
 * Emit events AND interact with APIs (Flux pattern)
 * Following Flux: Actions emit events for effects to update Redux, and call APIs
 */

import { eventBus, apiRegistry } from '@hai3/react';
import { ExecutiveViewEvents } from '../events/executiveViewEvents';
import { InsightApiService } from '../api/insightApiService';
import type { PeriodValue } from '../types';

/**
 * Load executive view data for the given period
 */
export const loadExecutiveView = (period: PeriodValue): void => {
  eventBus.emit(ExecutiveViewEvents.ExecutiveViewLoadStarted);
  void apiRegistry.getService(InsightApiService).getExecutiveViewData(period).then((data) => {
    eventBus.emit(ExecutiveViewEvents.ExecutiveViewLoaded, data);
  }).catch((err: unknown) => {
    eventBus.emit(ExecutiveViewEvents.ExecutiveViewLoadFailed, String(err));
  });
};

/**
 * insight Effects
 * Listen to events and update slice
 * Following Flux: Effects subscribe to events and update their own slice only
 */

import { type AppDispatch, eventBus } from '@hai3/react';
import { InsightEvents } from '../events/insightEvents';
import { setDashboard, setSpeed } from '../slices/insightSlice';

/**
 * Initialize effects
 * Called once during slice registration
 */
export const initializeInsightEffects = (appDispatch: AppDispatch): void => {
  const dispatch = appDispatch;

  eventBus.on(InsightEvents.DashboardLoaded, (data) => {
    dispatch(setDashboard(data));
  });


  eventBus.on(InsightEvents.SpeedLoaded, (data) => {
    dispatch(setSpeed(data));
  });
};

/**
 * Executive View Effects
 * Listen to events and update slice
 * Following Flux: Effects subscribe to events and update their own slice only
 */

import { type AppDispatch, eventBus } from '@hai3/react';
import { ExecutiveViewEvents } from '../events/executiveViewEvents';
import { setLoading, setExecutiveViewData, setAvailability, setError } from '../slices/executiveViewSlice';

/**
 * Initialize effects
 * Called once during slice registration
 */
export const initializeExecutiveViewEffects = (appDispatch: AppDispatch): void => {
  const dispatch = appDispatch;

  eventBus.on(ExecutiveViewEvents.ExecutiveViewLoadStarted, () => {
    dispatch(setLoading(true));
  });

  eventBus.on(ExecutiveViewEvents.ExecutiveViewLoaded, (data) => {
    dispatch(setExecutiveViewData(data));
  });

  eventBus.on(ExecutiveViewEvents.ExecutiveViewAvailabilityLoaded, (availability) => {
    dispatch(setAvailability(availability));
  });

  eventBus.on(ExecutiveViewEvents.ExecutiveViewLoadFailed, (msg) => {
    dispatch(setError(msg));
    dispatch(setLoading(false));
  });
};

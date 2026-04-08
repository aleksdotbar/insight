/**
 * Period Effects
 * Listen to events and update slice
 * Following Flux: Effects subscribe to events and update their own slice only
 */

import { type AppDispatch, eventBus } from '@hai3/react';
import { PeriodEvents } from '../events/periodEvents';
import { setPeriod, setCustomRange, setScale } from '../slices/periodSlice';

/**
 * Initialize effects
 * Called once during slice registration
 */
export const initializePeriodEffects = (appDispatch: AppDispatch): void => {
  const dispatch = appDispatch;

  eventBus.on(PeriodEvents.PeriodChanged, (data) => {
    dispatch(setPeriod(data.period));
    dispatch(setScale(data.scale));
    dispatch(setCustomRange(null)); // switching to a preset period clears any custom range
  });

  eventBus.on(PeriodEvents.DateRangeSet, (data) => {
    dispatch(setCustomRange(data));
  });
};

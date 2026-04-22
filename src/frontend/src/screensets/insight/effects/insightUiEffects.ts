/**
 * Insight UI Effects
 * Listen for UI preference events and update slice.
 */

import { type AppDispatch, eventBus } from '@hai3/react';
import { InsightUiEvents } from '../events/insightUiEvents';
import { setInsightViewMode } from '../slices/insightUiSlice';

export const initializeInsightUiEffects = (dispatch: AppDispatch): void => {
  eventBus.on(InsightUiEvents.ViewModeChanged, (mode) => {
    dispatch(setInsightViewMode(mode));
  });
};

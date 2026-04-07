/**
 * Team View Effects
 * Listen to events and update slice
 * Following Flux: Effects subscribe to events and update their own slice only
 */

import { type AppDispatch, eventBus } from '@hai3/react';
import { TeamViewEvents } from '../events/teamViewEvents';
import { setLoading, setTeamViewData, setError } from '../slices/teamViewSlice';

/**
 * Initialize effects
 * Called once during slice registration
 */
export const initializeTeamViewEffects = (appDispatch: AppDispatch): void => {
  const dispatch = appDispatch;

  eventBus.on(TeamViewEvents.TeamViewLoadStarted, () => {
    dispatch(setLoading(true));
  });

  eventBus.on(TeamViewEvents.TeamViewLoaded, (data) => {
    dispatch(setTeamViewData(data));
  });

  eventBus.on(TeamViewEvents.TeamViewLoadFailed, (msg) => {
    dispatch(setError(msg));
    dispatch(setLoading(false));
  });
};

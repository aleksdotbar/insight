/**
 * Team View Effects
 * Listen to events and update slice
 * Following Flux: Effects subscribe to events and update their own slice only
 */

import { type AppDispatch, eventBus } from '@hai3/react';
import { TeamViewEvents } from '../events/teamViewEvents';
import {
  resetForLoad,
  setAvailability,
  setError,
  setSectionLoading,
  setSectionLoaded,
  setSectionFailed,
  setDrillState,
  clearDrill,
} from '../slices/teamViewSlice';

export const initializeTeamViewEffects = (appDispatch: AppDispatch): void => {
  const dispatch = appDispatch;

  eventBus.on(TeamViewEvents.TeamViewLoadStarted, () => {
    dispatch(resetForLoad());
  });

  eventBus.on(TeamViewEvents.TeamViewSectionLoading, (payload) => {
    dispatch(setSectionLoading(payload));
  });

  eventBus.on(TeamViewEvents.TeamViewSectionLoaded, (payload) => {
    dispatch(setSectionLoaded(payload));
  });

  eventBus.on(TeamViewEvents.TeamViewSectionFailed, (payload) => {
    dispatch(setSectionFailed(payload));
  });

  // Backwards-compat: legacy bulk `TeamViewLoaded` is no longer emitted by
  // the action layer; keep listener absent so emit-from-tests is a no-op.

  eventBus.on(TeamViewEvents.TeamViewAvailabilityLoaded, (availability) => {
    dispatch(setAvailability(availability));
  });

  eventBus.on(TeamViewEvents.TeamViewLoadFailed, (msg) => {
    dispatch(setError(msg));
  });

  eventBus.on(TeamViewEvents.DrillOpened, (payload) => {
    dispatch(setDrillState(payload));
  });

  eventBus.on(TeamViewEvents.DrillClosed, () => {
    dispatch(clearDrill());
  });
};

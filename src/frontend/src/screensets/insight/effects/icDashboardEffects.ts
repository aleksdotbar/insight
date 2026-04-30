/**
 * IC Dashboard Effects
 * Listen to events and update slice
 * Following Flux: Effects subscribe to events and update their own slice only
 */

import { type AppDispatch, eventBus } from '@hai3/react';
import { IcDashboardEvents } from '../events/icDashboardEvents';
import {
  resetForLoad,
  setPerson,
  setAvailability,
  setError,
  setSectionLoading,
  setSectionLoaded,
  setSectionFailed,
  setDrillState,
  clearDrill,
} from '../slices/icDashboardSlice';
import { setSelectedPerson } from '../slices/userContextSlice';

/**
 * Initialize effects
 * Called once during slice registration
 */
export const initializeIcDashboardEffects = (appDispatch: AppDispatch): void => {
  const dispatch = appDispatch;

  eventBus.on(IcDashboardEvents.PersonSelected, (personId) => {
    dispatch(setSelectedPerson(personId));
  });

  eventBus.on(IcDashboardEvents.IcDashboardLoadStarted, () => {
    // Wipe out previous-load section status / aggregate data so per-section
    // skeletons render immediately when the user changes person/period.
    dispatch(resetForLoad());
  });

  eventBus.on(IcDashboardEvents.IcDashboardSectionLoading, (payload) => {
    dispatch(setSectionLoading(payload));
  });

  eventBus.on(IcDashboardEvents.IcDashboardSectionLoaded, (payload) => {
    dispatch(setSectionLoaded(payload));
  });

  eventBus.on(IcDashboardEvents.IcDashboardSectionFailed, (payload) => {
    dispatch(setSectionFailed(payload));
  });

  // Backwards-compat: legacy bulk `IcDashboardLoaded` is no longer emitted by
  // the action layer, but keep the listener as a no-op so any external test
  // harness emitting it doesn't crash with an unhandled event.

  eventBus.on(IcDashboardEvents.IcPersonLoaded, (person) => {
    dispatch(setPerson(person));
  });

  eventBus.on(IcDashboardEvents.IcDashboardAvailabilityLoaded, (availability) => {
    dispatch(setAvailability(availability));
  });

  eventBus.on(IcDashboardEvents.IcDashboardLoadFailed, (msg) => {
    dispatch(setError(msg));
  });

  eventBus.on(IcDashboardEvents.DrillOpened, ({ drillId, drillData }) => {
    dispatch(setDrillState({ drillId, drillData }));
  });

  eventBus.on(IcDashboardEvents.DrillClosed, () => {
    dispatch(clearDrill());
  });
};

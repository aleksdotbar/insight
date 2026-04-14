/**
 * IC Dashboard Effects
 * Listen to events and update slice
 * Following Flux: Effects subscribe to events and update their own slice only
 */

import { type AppDispatch, eventBus } from '@hai3/react';
import { IcDashboardEvents } from '../events/icDashboardEvents';
import {
  setSelectedPersonId,
  setLoading,
  setIcDashboardData,
  setPerson,
  setAvailability,
  setError,
  setDrillState,
  clearDrill,
} from '../slices/icDashboardSlice';

/**
 * Initialize effects
 * Called once during slice registration
 */
export const initializeIcDashboardEffects = (appDispatch: AppDispatch): void => {
  const dispatch = appDispatch;

  eventBus.on(IcDashboardEvents.PersonSelected, (personId) => {
    dispatch(setSelectedPersonId(personId));
  });

  eventBus.on(IcDashboardEvents.IcDashboardLoadStarted, () => {
    dispatch(setLoading(true));
  });

  eventBus.on(IcDashboardEvents.IcDashboardLoaded, (data) => {
    dispatch(setIcDashboardData(data));
  });

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

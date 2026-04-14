/**
 * auth Effects
 * Listen to events and update slice
 * Following Flux: Effects subscribe to events and update their own slice only
 */

import { type AppDispatch } from '@hai3/react';
// import { eventBus } from '@hai3/react';
// import { AuthEvents } from '../events/authEvents';
// import { } from '../slices/authSlice';

/**
 * Initialize effects
 * Called once during slice registration
 */
export const initializeAuthEffects = (_appDispatch: AppDispatch): void => {
  // Store dispatch for use in event listeners
  // const dispatch = _appDispatch;

  // Add your event listeners here
  // Example:
  // eventBus.on(AuthEvents.Selected, ({ id }) => {
  //   dispatch(setSelectedId(id));
  // });
};

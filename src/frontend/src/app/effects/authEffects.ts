/**
 * Auth Effects
 * Listen to OIDC lifecycle events and update authSlice
 * Following Flux: Effects subscribe to events and dispatch to their own slice only
 */

import { eventBus, type AppDispatch } from '@hai3/react';
import { AuthEvent } from '@/app/events/authEvents';
import { setToken, setConfig, setStatus } from '@/app/slices/authSlice';

export function initAuthEffects(dispatch: AppDispatch): void {
  eventBus.on(AuthEvent.ConfigLoaded, ({ config }) => {
    dispatch(setConfig(config));
  });

  eventBus.on(AuthEvent.TokenStored, ({ token }) => {
    dispatch(setToken(token));
    dispatch(setStatus('authenticated'));
  });

  eventBus.on(AuthEvent.SessionExpired, () => {
    dispatch(setToken(null));
    dispatch(setStatus('expired'));
  });
}

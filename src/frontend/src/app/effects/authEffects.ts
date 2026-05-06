/**
 * Auth Effects
 * Listen to OIDC lifecycle events and update authSlice
 * Following Flux: Effects subscribe to events and dispatch to their own slice only
 *
 * @cpt-component:cpt-auth-component-oidc-manager (event wiring)
 */

import { eventBus, type AppDispatch } from '@hai3/react';
import { AuthEvent } from '@/app/events/authEvents';
import { setToken, setConfig, setStatus, clearAuth } from '@/app/slices/authSlice';

export function initAuthEffects(dispatch: AppDispatch): void {
  eventBus.on(AuthEvent.ConfigLoaded, ({ config }) => {
    dispatch(setConfig(config));
    dispatch(setStatus('loading'));
  });

  eventBus.on(AuthEvent.TokenStored, ({ token }) => {
    dispatch(setToken(token));
    dispatch(setStatus('authenticated'));
  });

  eventBus.on(AuthEvent.CallbackCompleted, ({ returnUrl }) => {
    window.location.replace(returnUrl);
  });

  eventBus.on(AuthEvent.SessionExpired, () => {
    dispatch(setToken(null));
    dispatch(setStatus('expired'));
  });

  eventBus.on(AuthEvent.Unauthorized, () => {
    dispatch(setToken(null));
    dispatch(setStatus('unauthorized'));
  });

  eventBus.on(AuthEvent.LogoutRequested, () => {
    dispatch(clearAuth());
  });
}

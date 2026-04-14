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

  eventBus.on(AuthEvent.TokenStored, async ({ token }) => {
    dispatch(setToken(token));
    dispatch(setStatus('authenticated'));

    // Fetch current user from backend (real request, not mocked)
    try {
      const response = await fetch('/api/v1/user', {
        headers: { 'Authorization': `Bearer ${token}` },
      });
      console.log('[authEffects] GET /api/v1/user →', response.status);
    } catch (err) {
      console.warn('[authEffects] /api/v1/user failed:', err);
    }
  });

  eventBus.on(AuthEvent.SessionExpired, () => {
    dispatch(setToken(null));
    dispatch(setStatus('expired'));
  });

  eventBus.on(AuthEvent.LogoutRequested, () => {
    dispatch(clearAuth());
  });
}

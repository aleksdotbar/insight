/**
 * Auth Actions (app-level)
 * OIDC session management following HAI3 Flux pattern.
 */

import { eventBus } from '@hai3/react';
import { AuthEvent } from '@/app/events/authEvents';
import { OidcManager } from '@/app/auth/OidcManager';

/**
 * Initialize auth — check for existing session.
 * Called from Layout on mount.
 * Resolves OidcManager.authReady with user or null.
 * NEVER redirects — that is the responsibility of protected screens.
 */
export function initAuth(): void {
  void OidcManager.init()
    .then(() => OidcManager.getUser())
    .then((user) => {
      if (user && !user.expired) {
        OidcManager.resolveAuth(user);
      } else {
        OidcManager.resolveAuth(null);
      }
    })
    .catch(() => {
      OidcManager.resolveAuth(null);
    });
}

/** Logout: clear local state and redirect to OIDC provider logout */
export function logout(): void {
  eventBus.emit(AuthEvent.LogoutRequested);
  void OidcManager.signOut();
}

/**
 * Auth Actions (screenset-level)
 * OIDC callback handling.
 */

import { eventBus } from '@hai3/react';
import { AuthEvent } from '@/app/events/authEvents';
import { OidcManager } from '@/app/auth/OidcManager';
import { getStartUrl } from '@/app/auth/startUrl';

/**
 * Handle OIDC callback — exchange authorization code for tokens.
 * Called from CallbackScreen on mount.
 *
 * Uses startUrl captured at app startup (before AppRouter strips query params).
 */
export function handleOidcCallback(): void {
  const callbackUrl = getStartUrl();

  if (!callbackUrl || !callbackUrl.includes('code=')) {
    console.error('[handleOidcCallback] no callback URL with code');
    eventBus.emit(AuthEvent.SessionExpired);
    return;
  }

  void OidcManager.handleCallback(callbackUrl)
    .then((returnUrl) => {
      eventBus.emit(AuthEvent.CallbackCompleted, { returnUrl });
    })
    .catch((err) => {
      console.error('[handleOidcCallback] failed:', err);
      eventBus.emit(AuthEvent.SessionExpired);
    });
}

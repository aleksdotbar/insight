/**
 * Auth Events — OIDC lifecycle events
 */

import '@hai3/react';
import type { OidcConfig } from '@/app/types/auth';

const APP_ID = 'app';
const DOMAIN_ID = 'auth';

export const AuthEvent = {
  ConfigLoaded:    `${APP_ID}/${DOMAIN_ID}/configLoaded`,
  TokenStored:     `${APP_ID}/${DOMAIN_ID}/tokenStored`,
  SessionExpired:  `${APP_ID}/${DOMAIN_ID}/sessionExpired`,
  LogoutRequested: `${APP_ID}/${DOMAIN_ID}/logoutRequested`,
} as const;

declare module '@hai3/react' {
  interface EventPayloadMap {
    /** OIDC config loaded from /auth/config */
    'app/auth/configLoaded': { config: OidcConfig };
    /** Access token stored after OIDC callback */
    'app/auth/tokenStored': { token: string };
    /** 401 received — session expired, re-initiate OIDC flow */
    'app/auth/sessionExpired': void;
    /** User requested logout */
    'app/auth/logoutRequested': void;
  }
}

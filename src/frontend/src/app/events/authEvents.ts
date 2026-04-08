/**
 * Auth Events — OIDC lifecycle events
 */

import '@hai3/react';
import type { OidcConfig } from '@/app/types/auth';

declare module '@hai3/react' {
  interface EventPayloadMap {
    /** OIDC config loaded from /auth/config */
    'auth:configLoaded': { config: OidcConfig };
    /** Access token stored after OIDC callback */
    'auth:tokenStored': { token: string };
    /** 401 received — session expired, re-initiate OIDC flow */
    'auth:sessionExpired': void;
  }
}

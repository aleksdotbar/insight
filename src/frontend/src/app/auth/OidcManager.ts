/**
 * OidcManager
 * Wraps oidc-client-ts UserManager and bridges OIDC events to HAI3 eventBus.
 * All OIDC operations go through this service — components never touch UserManager directly.
 *
 * authReady — global promise that resolves when auth state is determined.
 * Protected screens await it before rendering.
 */

import { UserManager, WebStorageStateStore, type User } from 'oidc-client-ts';
import { eventBus, apiRegistry } from '@hai3/react';
import { AuthEvent } from '@/app/events/authEvents';
import { AuthApiService } from '@/app/api/AuthApiService';
import type { OidcConfig } from '@/app/types/auth';

let userManager: UserManager | null = null;
let initPromise: Promise<void> | null = null;

/** Resolves when auth state is determined (user found or not) */
let authReadyResolve: (user: User | null) => void;
const authReady: Promise<User | null> = new Promise((resolve) => {
  authReadyResolve = resolve;
});

function buildSettings(config: OidcConfig) {
  const store = new WebStorageStateStore({ store: sessionStorage });
  return {
    authority: config.issuer_url,
    client_id: config.client_id,
    redirect_uri: config.redirect_uri,
    post_logout_redirect_uri: window.location.origin,
    scope: config.scopes.join(' '),
    response_type: config.response_type,
    automaticSilentRenew: true,
    userStore: store,
    stateStore: store,
  };
}

function wireEvents(um: UserManager): void {
  um.events.addUserLoaded((user: User) => {
    eventBus.emit(AuthEvent.TokenStored, { token: user.access_token });
  });

  um.events.addUserUnloaded(() => {
    eventBus.emit(AuthEvent.SessionExpired);
  });

  um.events.addAccessTokenExpired(() => {
    eventBus.emit(AuthEvent.SessionExpired);
  });

  um.events.addSilentRenewError(() => {
    eventBus.emit(AuthEvent.SessionExpired);
  });
}

async function doInit(): Promise<void> {
  const authService = apiRegistry.getService(AuthApiService);
  const config = await authService.getConfig();

  eventBus.emit(AuthEvent.ConfigLoaded, { config });

  userManager = new UserManager(buildSettings(config));
  wireEvents(userManager);

  // Restore session from sessionStorage if available
  const existingUser = await userManager.getUser();
  if (existingUser && !existingUser.expired) {
    eventBus.emit(AuthEvent.TokenStored, { token: existingUser.access_token });
  }
}

export const OidcManager = {
  /** Promise that resolves when auth state is determined */
  authReady,

  /** Resolve the authReady promise (called by actions) */
  resolveAuth(user: User | null): void {
    authReadyResolve(user);
  },

  /**
   * Initialize UserManager with config from backend.
   * Safe to call multiple times — returns the same promise.
   */
  async init(): Promise<void> {
    if (!initPromise) {
      initPromise = doInit();
    }
    return initPromise;
  },

  /** Check for existing session in sessionStorage */
  async getUser(): Promise<User | null> {
    if (!userManager) return null;
    return userManager.getUser();
  },

  /** Redirect to OIDC provider login page (Authorization Code + PKCE) */
  async signIn(): Promise<void> {
    await OidcManager.init();
    if (!userManager) return;

    sessionStorage.setItem('__debug_signIn_called', Date.now().toString());
    await userManager.signinRedirect({
      state: { returnUrl: window.location.pathname + window.location.search },
    });
    // Won't reach here — browser redirects
  },

  /** Complete the callback — exchange code for tokens */
  async handleCallback(callbackUrl: string): Promise<string> {
    await OidcManager.init();
    if (!userManager) throw new Error('OidcManager not initialized');


    const user = await userManager.signinRedirectCallback(callbackUrl);
    authReadyResolve(user);
    const returnUrl = (user.state as { returnUrl?: string })?.returnUrl ?? '/';
    return returnUrl;
  },

  /** RP-initiated logout at OIDC provider */
  async signOut(): Promise<void> {
    if (!userManager) return;
    await userManager.signoutRedirect();
  },
};

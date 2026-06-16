import { UserManager, WebStorageStateStore, type User } from "oidc-client-ts";

import { authStore } from "./auth-store";
import { readOidcConfig } from "./config";
import { readDevUserEmail } from "./dev-config";
import type { AuthUser, OidcConfig, OidcSigninState } from "./types";

let userManager: UserManager | null = null;
let initPromise: Promise<void> | null = null;
let refreshPromise: Promise<string | null> | null = null;

const { promise: authReady, resolve: authReadyResolve } =
  Promise.withResolvers<User | null>();

function buildSettings(config: OidcConfig) {
  const store = new WebStorageStateStore({ store: sessionStorage });
  return {
    authority: config.issuer_url,
    client_id: config.client_id,
    redirect_uri: config.redirect_uri,
    post_logout_redirect_uri: window.location.origin,
    scope: config.scopes.join(" "),
    response_type: config.response_type,
    automaticSilentRenew: true,
    userStore: store,
    stateStore: store,
  };
}

/**
 * Same-origin path validation for the post-callback redirect target.
 *
 * `window.location.replace(raw)` honors absolute URLs, protocol-relative
 * `//host/path`, and `javascript:` / `data:` schemes — any of which would
 * pivot a freshly minted session to an attacker origin if `state.returnUrl`
 * is tampered with (XSS, malicious extension, shared device, library bug).
 * Restrict to same-origin paths.
 */
function safeReturnUrl(raw: unknown): string {
  if (typeof raw !== "string") return "/";
  if (!raw.startsWith("/")) return "/";
  if (raw.startsWith("//")) return "/";
  return raw;
}

function toAuthUser(user: User): AuthUser {
  const profile = user.profile ?? {};
  return {
    sub: profile.sub,
    email: typeof profile.email === "string" ? profile.email : undefined,
    name: typeof profile.name === "string" ? profile.name : undefined,
  };
}

function wireEvents(um: UserManager): void {
  um.events.addUserLoaded((user) => {
    authStore.setToken(user.access_token);
    authStore.setUser(toAuthUser(user));
    authStore.setStatus("authenticated");
  });

  um.events.addUserUnloaded(() => {
    authStore.setToken(null);
    authStore.setStatus("expired");
  });

  um.events.addAccessTokenExpired(() => {
    authStore.setToken(null);
    authStore.setStatus("expired");
  });

  um.events.addSilentRenewError(() => {
    authStore.setStatus("expired", "silent_renew_failed");
  });
}

async function doInit(): Promise<void> {
  authStore.setStatus("loading");

  const config = readOidcConfig();

  if (!config) {
    // Bypass auth when running with no OIDC AND either:
    //   - we're in Vite dev (developer convenience), or
    //   - the runtime explicitly injected a dev user email via
    //     window.__DEV_CONFIG__ (compose dev stack with the published
    //     ghcr image; entrypoint refuses to emit __DEV_CONFIG__ when
    //     real OIDC is also configured, so prod fails closed).
    const runtimeDevEmail = readDevUserEmail();
    if (import.meta.env.DEV || runtimeDevEmail) {
      console.warn(
        "[OidcManager] No window.__OIDC_CONFIG__ — auth bypassed (dev impersonation).",
      );
      authStore.setStatus("authenticated");
      authReadyResolve(null);
      return;
    }
    authStore.setStatus("unauthorized", "missing_oidc_config");
    authReadyResolve(null);
    return;
  }

  userManager = new UserManager(buildSettings(config));
  wireEvents(userManager);

  const existingUser = await userManager.getUser();
  if (existingUser && !existingUser.expired) {
    authStore.setToken(existingUser.access_token);
    authStore.setUser(toAuthUser(existingUser));
    authStore.setStatus("authenticated");
    authReadyResolve(existingUser);
  } else {
    authStore.setStatus("idle");
    authReadyResolve(null);
  }
}

export const OidcManager = {
  authReady,

  async init(): Promise<void> {
    if (!initPromise) initPromise = doInit();
    return initPromise;
  },

  async ensureReady(): Promise<User | null> {
    await OidcManager.init();
    return authReady;
  },

  async getUser(): Promise<User | null> {
    if (!userManager) return null;
    return userManager.getUser();
  },

  async refresh(): Promise<string | null> {
    if (refreshPromise) return refreshPromise;
    refreshPromise = (async () => {
      if (!userManager) return null;
      try {
        // signinSilent replaces the stored user atomically on success and
        // leaves the previous one untouched on failure — do NOT removeUser()
        // first, that would synchronously fire addUserUnloaded and flip the
        // store to `expired` mid-renew, blanking the token for any
        // concurrent fetchWithAuth caller.
        const newUser = await userManager.signinSilent();
        const token = newUser?.access_token ?? null;
        if (token) {
          authStore.setToken(token);
          authStore.setUser(toAuthUser(newUser!));
          authStore.setStatus("authenticated");
        }
        return token;
      } catch {
        return null;
      }
    })().finally(() => {
      refreshPromise = null;
    });
    return refreshPromise;
  },

  async signIn(): Promise<void> {
    await OidcManager.init();
    if (!userManager) return;
    const state: OidcSigninState = {
      returnUrl: window.location.pathname + window.location.search,
    };
    await userManager.signinRedirect({ state });
  },

  async handleCallback(callbackUrl: string): Promise<string> {
    await OidcManager.init();
    if (!userManager) throw new Error("oidc_not_initialized");
    const user = await userManager.signinRedirectCallback(callbackUrl);
    authStore.setToken(user.access_token);
    authStore.setUser(toAuthUser(user));
    authStore.setStatus("authenticated");
    authReadyResolve(user);
    const state = user.state as OidcSigninState | undefined;
    return safeReturnUrl(state?.returnUrl);
  },

  async signOut(): Promise<void> {
    const um = userManager;
    if (!um) {
      authStore.reset();
      return;
    }
    // Capture id_token before clearing so the end_session call still has
    // id_token_hint — some IdPs require it.
    let idTokenHint: string | undefined;
    try {
      const user = await um.getUser();
      idTokenHint = user?.id_token;
    } catch {
      idTokenHint = undefined;
    }
    // Targeted cleanup via oidc-client-ts APIs — `removeUser()` drops the
    // active user entry; `clearStaleState()` purges stale code/state entries
    // from prior incomplete redirects. Both stay within the OIDC namespace,
    // so unrelated sessionStorage entries (current or future) aren't lost.
    try {
      await um.removeUser();
      await um.clearStaleState();
    } catch {
      // storage unavailable or already cleared — proceed to redirect.
    }
    authStore.reset();
    await um.signoutRedirect({ id_token_hint: idTokenHint });
  },
};

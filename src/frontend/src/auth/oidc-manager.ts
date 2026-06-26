import { UserManager, WebStorageStateStore, type User } from "oidc-client-ts";

import { authStore } from "./auth-store";
import { readOidcConfig } from "./config";
import { readDevUserEmail } from "./dev-config";
import type { AuthReason, AuthUser, OidcConfig, OidcSigninState } from "./types";

let userManager: UserManager | null = null;
let initPromise: Promise<void> | null = null;
let refreshPromise: Promise<string | null> | null = null;
let redirectPromise: Promise<void> | null = null;

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
 * Same-origin validation for the post-callback redirect target.
 *
 * `window.location.replace(raw)` honors absolute URLs, protocol-relative
 * `//host/path`, backslash variants (`/\host` — browsers fold `\`→`/`), and
 * `javascript:` / `data:` schemes — any of which would pivot a freshly minted
 * session to an attacker origin if `state.returnUrl` is tampered with (XSS,
 * malicious extension, shared device, library bug). Resolve against our own
 * origin and only echo back a same-origin path + query + hash.
 */
function safeReturnUrl(raw: unknown): string {
  if (typeof raw !== "string") return "/";
  try {
    const url = new URL(raw, window.location.origin);
    if (url.origin !== window.location.origin) return "/";
    return url.pathname + url.search + url.hash;
  } catch {
    return "/";
  }
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
    authStore.setStatus("renewing");
  });

  um.events.addAccessTokenExpired(() => {
    authStore.setToken(null);
    authStore.setStatus("renewing");
  });

  // Background silent renew has given up — the session can't recover on its
  // own, so escalate to an interactive redirect without waiting for the next
  // request to 401.
  um.events.addSilentRenewError(() => {
    void OidcManager.requireReauth("silent_renew_failed");
  });
}

async function doInit(): Promise<void> {
  authStore.setStatus("initializing");

  const config = readOidcConfig();

  if (!config) {
    // No OIDC config: auth is not active. In Vite dev or when the runtime
    // injected a dev user email (compose dev stack with the published ghcr
    // image) this is an intentional bypass; otherwise it's an unconfigured
    // deploy that fails closed (no token is minted, so requests 401 and
    // surface nothing — `requireReauth` has no IdP to redirect to and no-ops).
    const runtimeDevEmail = readDevUserEmail();
    if (import.meta.env.DEV || runtimeDevEmail) {
      console.warn(
        "[OidcManager] No window.__OIDC_CONFIG__ — auth bypassed (dev impersonation).",
      );
      authStore.setStatus("disabled", "dev_bypass");
    } else {
      authStore.setStatus("disabled", "missing_oidc_config");
    }
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
    // Configured but no live session — an interactive sign-in is needed.
    // `beforeLoad` performs the redirect for this first-load case.
    authStore.setStatus("reauth_required");
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
        // store to `renewing` mid-renew, blanking the token for any
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
    // Share a single in-flight redirect (same idiom as `refresh`/`init`):
    // concurrent callers — multiple 401s, the silent-renew failure, the
    // first-load guard — await the very same promise and observe the same
    // outcome, so a redirect that rejects rejects for all of them (letting
    // every `requireReauth` caller fall through to `reauth_failed`). On
    // success the page unloads; on rejection the slot is cleared for a retry.
    if (redirectPromise) return redirectPromise;
    // Never return the user to /callback — it has no `code` on a fresh visit
    // and would loop straight back into the failure screen.
    const path = window.location.pathname;
    const returnUrl =
      path === "/callback"
        ? "/"
        : path + window.location.search + window.location.hash;
    const state: OidcSigninState = { returnUrl };
    redirectPromise = userManager.signinRedirect({ state }).finally(() => {
      redirectPromise = null;
    });
    return redirectPromise;
  },

  /**
   * Single owner of the "session is dead, recover it" decision. Both the
   * 401 path (`fetchWithAuth`) and the background silent-renew failure funnel
   * here so the redirect lives in one place with one outcome model:
   *   - no IdP to redirect to (dev / unconfigured) → no-op, leave the app as-is
   *   - redirect starts → status `reauth_required`, the overlay covers the UI
   *   - redirect can't start → status `reauth_failed`, the gate offers a retry
   *
   * Never rejects — callers may `void` it; failures surface through state.
   */
  async requireReauth(reason: AuthReason | null = null): Promise<void> {
    await OidcManager.init();
    if (!userManager) return;
    authStore.setStatus("reauth_required", reason);
    try {
      await OidcManager.signIn();
    } catch {
      authStore.setStatus("reauth_failed", reason);
    }
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

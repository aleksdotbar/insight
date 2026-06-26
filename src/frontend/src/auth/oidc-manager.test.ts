import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const { signinRedirect, signinRedirectCallback } = vi.hoisted(() => ({
  signinRedirect: vi.fn(),
  signinRedirectCallback: vi.fn(),
}));

vi.mock("oidc-client-ts", () => {
  class UserManager {
    events = {
      addUserLoaded: vi.fn(),
      addUserUnloaded: vi.fn(),
      addAccessTokenExpired: vi.fn(),
      addSilentRenewError: vi.fn(),
    };
    getUser = vi.fn().mockResolvedValue(null);
    signinRedirect = signinRedirect;
    signinRedirectCallback = signinRedirectCallback;
  }
  class WebStorageStateStore {}
  return { UserManager, WebStorageStateStore };
});

function withOidcConfig(): void {
  window.__OIDC_CONFIG__ = {
    issuer_url: "https://idp.example",
    client_id: "client",
    scopes: "openid profile",
  };
}

beforeEach(() => {
  // Fresh module each test resets the module-level `redirectInFlight` /
  // `initPromise` state; the hoisted `signinRedirect` spy persists.
  vi.resetModules();
  signinRedirect.mockReset();
  signinRedirect.mockResolvedValue(undefined);
  signinRedirectCallback.mockReset();
  window.history.pushState({}, "", "/");
  withOidcConfig();
});

afterEach(() => {
  delete window.__OIDC_CONFIG__;
  vi.restoreAllMocks();
});

describe("OidcManager.signIn redirect guard", () => {
  it("fires a single redirect for concurrent callers", async () => {
    const { OidcManager } = await import("./oidc-manager");
    await Promise.all([
      OidcManager.signIn(),
      OidcManager.signIn(),
      OidcManager.signIn(),
    ]);
    expect(signinRedirect).toHaveBeenCalledTimes(1);
  });

  it("preserves the current location in the redirect state", async () => {
    const { OidcManager } = await import("./oidc-manager");
    await OidcManager.signIn();
    expect(signinRedirect).toHaveBeenCalledWith({
      state: {
        returnUrl: window.location.pathname + window.location.search,
      },
    });
  });

  it("resets the guard so a failed redirect can be retried", async () => {
    signinRedirect.mockRejectedValueOnce(new Error("redirect_failed"));
    const { OidcManager } = await import("./oidc-manager");
    await expect(OidcManager.signIn()).rejects.toThrow("redirect_failed");
    await OidcManager.signIn();
    expect(signinRedirect).toHaveBeenCalledTimes(2);
  });

  it("never returns the user back to /callback", async () => {
    window.history.pushState({}, "", "/callback?code=abc");
    const { OidcManager } = await import("./oidc-manager");
    await OidcManager.signIn();
    expect(signinRedirect).toHaveBeenCalledWith({ state: { returnUrl: "/" } });
  });

  it("preserves path, query and hash of the current location", async () => {
    window.history.pushState({}, "", "/ic/bob?period=month#section");
    const { OidcManager } = await import("./oidc-manager");
    await OidcManager.signIn();
    expect(signinRedirect).toHaveBeenCalledWith({
      state: { returnUrl: "/ic/bob?period=month#section" },
    });
  });
});

describe("OidcManager.handleCallback return-url safety", () => {
  function callbackUser(returnUrl: unknown) {
    return { access_token: "tok", profile: {}, state: { returnUrl } };
  }

  it("rejects cross-origin and backslash-smuggled targets", async () => {
    const { OidcManager } = await import("./oidc-manager");
    for (const evil of [
      "//evil.com",
      "/\\evil.com",
      "https://evil.com/x",
      "javascript:alert(1)",
    ]) {
      signinRedirectCallback.mockResolvedValueOnce(callbackUser(evil));
      const url = await OidcManager.handleCallback("https://app/callback?code=x");
      expect(url).toBe("/");
    }
  });

  it("keeps a same-origin path with query and hash", async () => {
    signinRedirectCallback.mockResolvedValue(
      callbackUser("/ic/bob?period=month#section"),
    );
    const { OidcManager } = await import("./oidc-manager");
    const url = await OidcManager.handleCallback("https://app/callback?code=x");
    expect(url).toBe("/ic/bob?period=month#section");
  });
});

describe("OidcManager.requireReauth", () => {
  it("flips to reauth_required and redirects", async () => {
    const { OidcManager } = await import("./oidc-manager");
    const { authStore } = await import("./auth-store");
    await OidcManager.requireReauth("refresh_failed");
    expect(signinRedirect).toHaveBeenCalledTimes(1);
    expect(authStore.getSnapshot().status).toBe("reauth_required");
    expect(authStore.getSnapshot().reason).toBe("refresh_failed");
  });

  it("flips to reauth_failed when the redirect cannot start", async () => {
    signinRedirect.mockRejectedValueOnce(new Error("boom"));
    const { OidcManager } = await import("./oidc-manager");
    const { authStore } = await import("./auth-store");
    await OidcManager.requireReauth("token_rejected");
    expect(authStore.getSnapshot().status).toBe("reauth_failed");
  });

  it("never rejects, so callers can fire-and-forget", async () => {
    signinRedirect.mockRejectedValueOnce(new Error("boom"));
    const { OidcManager } = await import("./oidc-manager");
    await expect(OidcManager.requireReauth("refresh_failed")).resolves.toBeUndefined();
  });

  it("is a no-op without OIDC (no IdP to redirect to)", async () => {
    delete window.__OIDC_CONFIG__;
    const { OidcManager } = await import("./oidc-manager");
    const { authStore } = await import("./auth-store");
    await OidcManager.requireReauth("refresh_failed");
    expect(signinRedirect).not.toHaveBeenCalled();
    expect(authStore.getSnapshot().status).toBe("disabled");
  });
});

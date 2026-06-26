import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const { signinRedirect } = vi.hoisted(() => ({ signinRedirect: vi.fn() }));

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
  }
  class WebStorageStateStore {}
  return { UserManager, WebStorageStateStore };
});

describe("OidcManager.signIn redirect guard", () => {
  beforeEach(() => {
    // Fresh module each test resets the module-level `redirectInFlight` /
    // `initPromise` state; the hoisted `signinRedirect` spy persists.
    vi.resetModules();
    signinRedirect.mockReset();
    signinRedirect.mockResolvedValue(undefined);
    window.__OIDC_CONFIG__ = {
      issuer_url: "https://idp.example",
      client_id: "client",
      scopes: "openid profile",
    };
  });

  afterEach(() => {
    delete window.__OIDC_CONFIG__;
    vi.restoreAllMocks();
  });

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
});

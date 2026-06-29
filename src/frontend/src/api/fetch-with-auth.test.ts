import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const { getDevBearerEmail } = vi.hoisted(() => ({
  getDevBearerEmail: vi.fn<() => string | null>(),
}));
vi.mock("@/auth/use-viewer", () => ({ getDevBearerEmail }));

import { authStore } from "@/auth/auth-store";
import { OidcManager } from "@/auth/oidc-manager";
import { fetchWithAuth } from "./fetch-with-auth";

const fetchMock = () => globalThis.fetch as ReturnType<typeof vi.fn>;

function authHeaderOfLastCall(): string | null {
  const calls = fetchMock().mock.calls;
  const init = calls[calls.length - 1]?.[1] as RequestInit;
  return new Headers(init.headers).get("Authorization");
}

describe("fetchWithAuth", () => {
  beforeEach(() => {
    authStore.reset();
    authStore.setToken("tok");
    getDevBearerEmail.mockReset();
    getDevBearerEmail.mockReturnValue(null);
    vi.stubGlobal("fetch", vi.fn());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
    authStore.reset();
  });

  it("returns non-401 responses unchanged without touching auth", async () => {
    const ok = new Response(null, { status: 200 });
    fetchMock().mockResolvedValue(ok);
    const refresh = vi.spyOn(OidcManager, "refresh");
    const res = await fetchWithAuth("/x");
    expect(res).toBe(ok);
    expect(refresh).not.toHaveBeenCalled();
  });

  it("retries with the refreshed token on a 401", async () => {
    const r200 = new Response(null, { status: 200 });
    fetchMock()
      .mockResolvedValueOnce(new Response(null, { status: 401 }))
      .mockResolvedValueOnce(r200);
    vi.spyOn(OidcManager, "refresh").mockResolvedValue("newtok");
    const res = await fetchWithAuth("/x");
    expect(res).toBe(r200);
    expect(fetchMock()).toHaveBeenCalledTimes(2);
  });

  it("requires reauth and returns the 401 when refresh fails", async () => {
    const r401 = new Response(null, { status: 401 });
    fetchMock().mockResolvedValue(r401);
    vi.spyOn(OidcManager, "refresh").mockResolvedValue(null);
    const requireReauth = vi
      .spyOn(OidcManager, "requireReauth")
      .mockResolvedValue(undefined);
    const res = await fetchWithAuth("/x");
    expect(res).toBe(r401);
    expect(requireReauth).toHaveBeenCalledWith("refresh_failed");
  });

  it("requires reauth when the retry is still 401", async () => {
    fetchMock().mockResolvedValue(new Response(null, { status: 401 }));
    vi.spyOn(OidcManager, "refresh").mockResolvedValue("newtok");
    const requireReauth = vi
      .spyOn(OidcManager, "requireReauth")
      .mockResolvedValue(undefined);
    const res = await fetchWithAuth("/x");
    expect(res.status).toBe(401);
    expect(requireReauth).toHaveBeenCalledWith("token_rejected");
  });

  it("mints a dev bearer only for the disabled + dev_bypass state", async () => {
    authStore.reset();
    authStore.setStatus("disabled", "dev_bypass");
    getDevBearerEmail.mockReturnValue("dev@example.com");
    fetchMock().mockResolvedValue(new Response(null, { status: 200 }));
    await fetchWithAuth("/x");
    expect(authHeaderOfLastCall()).toMatch(/^Bearer \S+\.\S+\./);
  });

  it("never mints an unsigned bearer while renewing under OIDC", async () => {
    authStore.reset();
    authStore.setStatus("renewing");
    getDevBearerEmail.mockReturnValue("dev@example.com");
    fetchMock().mockResolvedValue(new Response(null, { status: 200 }));
    await fetchWithAuth("/x");
    expect(authHeaderOfLastCall()).toBeNull();
  });

  it("fails closed on an unconfigured deploy (missing_oidc_config)", async () => {
    authStore.reset();
    authStore.setStatus("disabled", "missing_oidc_config");
    getDevBearerEmail.mockReturnValue("dev@example.com");
    fetchMock().mockResolvedValue(new Response(null, { status: 200 }));
    await fetchWithAuth("/x");
    expect(authHeaderOfLastCall()).toBeNull();
  });
});

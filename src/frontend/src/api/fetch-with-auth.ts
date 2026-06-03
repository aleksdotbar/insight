import { authStore } from "@/auth/auth-store";
import { OidcManager } from "@/auth/oidc-manager";
import { getViewerEmail } from "@/auth/use-viewer";

function devBearer(): string | null {
  if (!import.meta.env.DEV) return null;
  const email = getViewerEmail();
  if (!email) return null;
  const b64url = (o: object): string =>
    btoa(JSON.stringify(o))
      .replace(/\+/g, "-")
      .replace(/\//g, "_")
      .replace(/=+$/, "");
  const header = b64url({ alg: "none", typ: "JWT" });
  const payload = b64url({
    email,
    preferred_username: email,
    sub: `dev:${email}`,
  });
  return `${header}.${payload}.`;
}

function injectAuthHeaders(headers: Headers): void {
  const { token, tenantId } = authStore.getSnapshot();
  const bearer = token ?? devBearer();
  if (bearer) headers.set("Authorization", `Bearer ${bearer}`);
  if (tenantId) headers.set("X-Tenant-ID", tenantId);
}

export async function fetchWithAuth(
  input: RequestInfo | URL,
  init: RequestInit = {},
): Promise<Response> {
  const headers = new Headers(init.headers);
  injectAuthHeaders(headers);

  const res = await fetch(input, { ...init, headers });

  if (res.status !== 401) return res;

  const newToken = await OidcManager.refresh();
  if (!newToken) {
    authStore.setStatus("unauthorized", "refresh_failed");
    return res;
  }

  const retryHeaders = new Headers(init.headers);
  injectAuthHeaders(retryHeaders);
  const retryRes = await fetch(input, { ...init, headers: retryHeaders });
  if (retryRes.status === 401) {
    authStore.setStatus("unauthorized", "token_rejected");
  }
  return retryRes;
}

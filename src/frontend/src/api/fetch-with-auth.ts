import { authStore } from "@/auth/auth-store";
import { OidcManager } from "@/auth/oidc-manager";

function injectAuthHeaders(headers: Headers): void {
  const { token, tenantId } = authStore.getSnapshot();
  if (token) headers.set("Authorization", `Bearer ${token}`);
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

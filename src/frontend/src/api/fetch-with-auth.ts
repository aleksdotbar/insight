import { authStore } from "@/auth/auth-store";
import { OidcManager } from "@/auth/oidc-manager";
import { getDevBearerEmail } from "@/auth/use-viewer";

function devBearer(): string | null {
  // getDevBearerEmail() returns null unless the active viewer source is
  // dev-style (dev / override). It will never resolve to an OIDC user's
  // email, even when a build-time dev fallback is configured — that
  // would risk a mid-bootstrap OIDC session (token still null) leaking
  // an unsigned JWT bearing the real user's identity.
  const email = getDevBearerEmail();
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
  const { status, reason, token, tenantId } = authStore.getSnapshot();
  // Only mint a dev/impersonation bearer for the genuine dev bypass. Under
  // real OIDC the token is briefly null while renewing — never let a URL
  // `?override=` identity forge an unsigned `alg:none` bearer there. An
  // unconfigured prod deploy lands on `disabled`/`missing_oidc_config` and
  // must fail closed too, hence the explicit `dev_bypass` reason check.
  const devEligible = status === "disabled" && reason === "dev_bypass";
  const bearer = token ?? (devEligible ? devBearer() : null);
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
    void OidcManager.requireReauth("refresh_failed");
    return res;
  }

  const retryHeaders = new Headers(init.headers);
  injectAuthHeaders(retryHeaders);
  const retryRes = await fetch(input, { ...init, headers: retryHeaders });
  if (retryRes.status === 401) {
    void OidcManager.requireReauth("token_rejected");
  }
  return retryRes;
}

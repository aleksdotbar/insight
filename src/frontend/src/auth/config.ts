import type { OidcConfig } from "./types";

export function readOidcConfig(): OidcConfig | null {
  const raw = window.__OIDC_CONFIG__;
  if (!raw?.issuer_url || !raw.client_id) return null;
  const scopes = (raw.scopes ?? "")
    .split(/\s+/)
    .map((s) => s.trim())
    .filter(Boolean);
  if (scopes.length === 0) return null;
  return {
    issuer_url: raw.issuer_url,
    client_id: raw.client_id,
    redirect_uri: `${window.location.origin}/callback`,
    scopes,
    response_type: "code",
  };
}

import type { OidcConfig } from "./types";

const CONFIG_ENDPOINT = "/api/v1/auth/config";

type RawConfig = {
  issuer_url?: unknown;
  client_id?: unknown;
  redirect_uri?: unknown;
  scopes?: unknown;
  response_type?: unknown;
};

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.length > 0;
}

export async function fetchOidcConfig(): Promise<OidcConfig | null> {
  let raw: RawConfig;
  try {
    const res = await fetch(CONFIG_ENDPOINT, {
      headers: { Accept: "application/json" },
      credentials: "omit",
      cache: "no-store",
    });
    if (!res.ok) return null;
    raw = (await res.json()) as RawConfig;
  } catch {
    return null;
  }

  if (
    !isNonEmptyString(raw.issuer_url) ||
    !isNonEmptyString(raw.client_id) ||
    !isNonEmptyString(raw.redirect_uri)
  ) {
    return null;
  }

  const scopes = Array.isArray(raw.scopes)
    ? raw.scopes.filter(isNonEmptyString)
    : [];
  if (scopes.length === 0) return null;

  return {
    issuer_url: raw.issuer_url,
    client_id: raw.client_id,
    redirect_uri: raw.redirect_uri,
    scopes,
    response_type: "code",
  };
}

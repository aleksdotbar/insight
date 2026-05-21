export type OidcConfig = {
  issuer_url: string;
  client_id: string;
  redirect_uri: string;
  scopes: string[];
  response_type: "code";
};

export type OidcSigninState = { returnUrl?: string };

export type AuthStatus =
  | "idle"
  | "loading"
  | "authenticated"
  | "expired"
  | "unauthorized";

export type AuthUser = {
  email?: string;
  name?: string;
  sub?: string;
};

export type AuthSnapshot = {
  status: AuthStatus;
  token: string | null;
  user: AuthUser | null;
  tenantId: string | null;
  error: string | null;
};

declare global {
  interface Window {
    __OIDC_CONFIG__?: {
      issuer_url?: string;
      client_id?: string;
      scopes?: string;
    };
  }
}

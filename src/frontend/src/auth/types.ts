export type OidcConfig = {
  issuer_url: string;
  client_id: string;
  redirect_uri: string;
  scopes: string[];
  response_type: "code";
};

export type OidcSigninState = { returnUrl?: string };

/**
 * OIDC session lifecycle from the app's point of view. Whether the app must
 * redirect to the IdP is a property of the status tag itself — not a predicate
 * computed over a free-form error string.
 *
 *   initializing   — bootstrapping; `init()` in flight.
 *   disabled       — OIDC not active (dev bypass, or unconfigured deploy). The
 *                    app runs without an interactive login; never redirects.
 *   authenticated  — valid session, token present.
 *   renewing       — token gone/expired, silent renew may still recover it.
 *                    Transient and non-terminal: the app keeps rendering.
 *   reauth_required— renewal is no longer possible; an interactive redirect is
 *                    needed. The setter is responsible for kicking the
 *                    redirect: `requireReauth()` does so directly, while
 *                    `doInit()` sets it for the first-load case where the root
 *                    `beforeLoad` performs the redirect. Don't assume the
 *                    component tree triggers it.
 *   reauth_failed  — the redirect itself could not start; recoverable via retry.
 */
export type AuthStatus =
  | "initializing"
  | "disabled"
  | "authenticated"
  | "renewing"
  | "reauth_required"
  | "reauth_failed";

/** Diagnostic cause carried alongside a status — for telemetry, never control flow. */
export type AuthReason =
  | "dev_bypass"
  | "missing_oidc_config"
  | "silent_renew_failed"
  | "refresh_failed"
  | "token_rejected";

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
  reason: AuthReason | null;
};

declare global {
  interface Window {
    __OIDC_CONFIG__?: {
      issuer_url?: string;
      client_id?: string;
      scopes?: string;
    };
    // Runtime dev-impersonation config. Populated by the FE container's
    // entrypoint only when DEV_USER_EMAIL is set AND no OIDC config is
    // provided. Production deploys with real OIDC must NOT set this — the
    // entrypoint refuses to emit both at once.
    __DEV_CONFIG__?: {
      devUserEmail?: string;
    };
  }
}

import type { AuthSnapshot } from "./types";

/**
 * A terminal auth failure is one that a full sign-in redirect must resolve —
 * silent renew can no longer recover the session.
 *
 * Excluded on purpose:
 *   - plain `expired` (no reason): `automaticSilentRenew` or the next
 *     `fetch-with-auth` `refresh()` may still self-heal it; redirecting here
 *     would defeat silent renew and force a full redirect on every token
 *     expiry.
 *   - `unauthorized` + `missing_oidc_config`: the dev / no-OIDC bypass.
 *     There is no IdP to redirect to, and `signIn()` would no-op anyway.
 */
export function isTerminalAuthFailure({
  status,
  error,
}: Pick<AuthSnapshot, "status" | "error">): boolean {
  if (status === "unauthorized") return error !== "missing_oidc_config";
  if (status === "expired") return error === "silent_renew_failed";
  return false;
}

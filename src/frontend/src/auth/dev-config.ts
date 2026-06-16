// Reader for window.__DEV_CONFIG__ — the runtime escape hatch that lets
// a production bundle do dev-impersonation when shipped into the
// docker-compose dev stack. The bundle stays auth-locked in any
// deployment that doesn't explicitly populate this (the entrypoint
// refuses to emit it when OIDC is also configured).
export function readDevUserEmail(): string | null {
  const raw = window.__DEV_CONFIG__?.devUserEmail;
  if (typeof raw !== "string") return null;
  const trimmed = raw.trim();
  return trimmed.length === 0 ? null : trimmed;
}

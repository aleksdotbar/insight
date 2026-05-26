# Auth

## Purpose

Authenticate every user against an external OIDC provider before any dashboard renders. Maintain session, refresh tokens silently, sign out cleanly.

## Users

- **End user** — any employee. Sees the provider's login page, never a custom one.
- **OIDC provider** (external) — issues + refreshes tokens.

## Requirements

1. **Runtime config**. Issuer URL, client id, scopes injected per deployment, not baked into the build. Different environments point at different providers without rebuild.
2. **Login**. Unauthenticated visit redirects to the provider via Authorization Code + PKCE. Original URL preserved across the round trip.
3. **Callback**. Dedicated route exchanges the code for tokens, then redirects to the preserved original URL. The return URL is validated as same-origin only — never a cross-origin or `javascript:` payload.
4. **Token storage**. Tab-scoped only (cleared on tab close). Never in cookies or persistent storage.
5. **Silent renew**. Tokens refreshed before expiry, off-screen. Concurrent refresh requests dedupe to a single in-flight call.
6. **Header injection**. Every API request carries the access token. A tenant header slot is reserved for future multi-tenant flows.
7. **401 recovery**. One retry after silent renew; repeated failure flips status to unauthenticated and surfaces a re-sign-in path.
8. **Sign out**. Clears local state, ends the provider session, redirects to a configured post-logout URL.
9. **Dev impersonation**. In dev builds only, an env var can supply a viewer email when no provider config is present. Prod builds cannot enter this branch.

## Non-goals

- Custom login UI.
- Role-based access control (derived per-screen from identity, not gated here).
- Self-service account / password flows.
- Multi-tenant switching within a single session.

## Acceptance

- Unauthenticated visit redirects to the provider within a second.
- Returning to an active session never re-prompts for credentials.
- Refresh happens off-screen; user activity is uninterrupted.
- No token value ever lands in persistent storage.
- Sign out ends both the local and provider sessions.
- A tampered return URL never navigates off-origin.

## Risks

- Tab close drops the session — by design.
- Same-tab XSS could read tab-scoped tokens. Mitigated by strict CSP + dependency hygiene, not by storage choice alone.

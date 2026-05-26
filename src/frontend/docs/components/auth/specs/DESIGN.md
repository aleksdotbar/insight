# Auth Design

## Shape

- One module owns the full OIDC lifecycle. Nothing else touches the OIDC client directly.
- A small reactive store holds the snapshot the rest of the app reads: status, token, user, tenant id, error. UI subscribes via one hook.
- A fetch wrapper injects the bearer token and tenant header on every request. On 401 it triggers a single silent renew and retries once; further failure flips status to unauthenticated.
- The router guard runs before any screen mounts. Statuses map to: authenticated → render, idle/expired/unauthenticated → start the OIDC flow, dev bypass → render. The callback route is whitelisted.
- One callback screen exchanges the code, validates the return URL is same-origin, navigates. Errors surface a retry path.

## Why this shape

- **One source of truth.** Viewer, token, status, tenant id all in one store. No scattered env reads, no per-component auth logic.
- **Library does the protocol, app does the policy.** OIDC client handles redirects, PKCE, silent renew. Wrappers above it encode app behavior: header injection, retry policy, return-URL safety.
- **Guard at the boundary.** Single check at the router root. Screens trust they're rendered only when the viewer is resolved.

## Boundaries

- Auth module never reaches into screens. Screens never reach into the OIDC client.
- The store is the only shared surface — updates flow through it, reads through one hook.
- Dev impersonation is a fallback path inside the store resolution, not a separate flow.

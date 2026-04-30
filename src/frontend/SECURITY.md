# Security

This document covers the frontend's security posture and the deployment
checklist for exposing Insight to the public internet (without VPN).

## Threat model

The frontend is a public-facing SPA that authenticates users via OIDC
(Authorization Code + PKCE) against a customer-provided issuer and calls a
single backend through `/api/*`. Trust boundary: the FE itself is untrusted —
all authorization decisions live on the backend; the FE only attaches the
bearer token and the tenant id.

## Token storage

Access tokens are stored in `sessionStorage` via `oidc-client-ts`'s
`WebStorageStateStore`. This is the standard SPA tradeoff:

- `sessionStorage` is per-tab and cleared when the tab closes — better than
  `localStorage` for blast radius.
- Tokens are accessible from JavaScript, so any XSS = token exfiltration. The
  CSP below is the primary mitigation; keep it tight.
- HttpOnly cookies would be stronger, but require a backend session bridge —
  out of scope for the current architecture.

## Headers shipped by nginx

Defined in `Dockerfile`:

| Header | Value | Why |
|---|---|---|
| `Content-Security-Policy` | see Dockerfile | Restrict script/style/connect sources, block inline scripts, prevent framing |
| `X-Frame-Options` | `DENY` | Clickjacking defense for legacy browsers (CSP `frame-ancestors` covers modern ones) |
| `X-Content-Type-Options` | `nosniff` | Prevent MIME-sniffing attacks on uploaded/static content |
| `Referrer-Policy` | `strict-origin-when-cross-origin` | Don't leak full URLs (with query params) to third parties |
| `Strict-Transport-Security` | `max-age=31536000; includeSubDomains` | Force HTTPS once a client has connected once |
| `Permissions-Policy` | `camera=(), microphone=(), geolocation=()` | Disable powerful APIs the app doesn't use |
| `Cross-Origin-Opener-Policy` | `same-origin` | Isolate browsing context — defense against cross-window leaks (Spectre, tab-napping) |
| `Cross-Origin-Resource-Policy` | `same-origin` | Block other origins from embedding our resources |

### CSP notes

- `style-src 'unsafe-inline'` is required for React inline styles and recharts.
  Tightening this requires a nonce-based pipeline — tracked as a follow-up.
- `connect-src` and `frame-src` are templated at container start by
  `docker-entrypoint.sh`: when `OIDC_ISSUER` is set, the issuer's origin is
  substituted in (tight). When not set, falls back to broad `https:` (still
  better than `*`). The substitution covers silent-renew iframe and token
  endpoint requests.
- After deployment, verify silent renew works (token auto-refreshes after 5 min)
  — if the OIDC issuer sets `X-Frame-Options: DENY` on its authorize endpoint,
  silent renew will fail and you'll need a different refresh strategy.

## Build hygiene

- `build.sourcemap: false` — production bundles never ship source maps.
- `esbuild.drop: ['debugger']` — `debugger` statements are stripped.
- `console.*` calls are gated behind `import.meta.env.DEV` and tree-shaken in
  production. After `npm run build`, verify with:
  ```sh
  grep -c "AuthPlugin\|Auto-discovered\|OIDC skipped" dist/assets/*.js
  # Expect: 0
  ls dist/assets/*.map 2>/dev/null
  # Expect: nothing
  ```

## Pre-deployment checklist (no-VPN exposure)

- [ ] `.env` on the build host does NOT contain `VITE_DEV_USER_EMAIL` or
      `VITE_ENABLE_MOCKS=true`. These are dev-only and tree-shaken in prod, but
      double-check there's no DEV build going to production.
- [ ] Container is served behind HTTPS-terminating reverse proxy. HSTS only
      makes sense over TLS.
- [ ] `window.__OIDC_CONFIG__` is injected at container startup with the real
      issuer URL, client id, and redirect URI. No fallback to mock auth in prod.
- [ ] Backend (`api-gateway`, `analytics-api`, `identity-resolution`) validates
      the `X-Tenant-ID` header against the JWT's tenant claim. The FE sets this
      header from Redux state — without server-side validation, a logged-in
      user can read other tenants by editing the header in DevTools.
- [ ] Backend rate-limits unauthenticated and authenticated endpoints
      separately. The FE has no rate-limiting and shouldn't.
- [ ] Verify silent renew works in staging for ≥10 minutes of idle session.
- [ ] Confirm CSP doesn't break recharts/HAI3 styling on every screen.
- [ ] Run `npm audit --omit=dev` — no high-severity findings.

## Reporting

Security issues: contact the Insight team lead. Do not file public GitHub
issues for vulnerabilities.

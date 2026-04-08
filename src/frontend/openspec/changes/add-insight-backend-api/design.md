# Design: add-insight-backend-api

**Change:** `add-insight-backend-api`
**Status:** Draft
**Date:** 2026-04-08

---

## Context

The `insight` screenset currently runs entirely on mocked data via `InsightApiService`
with hardcoded paths (`/api/insight/...`). The backend (`cyberfabric/insight`) is being
built in parallel and defines:

- An **API Gateway** (Rust/ModKit) handling OIDC/JWT auth, routing, rate limiting
- An **Analytics API** service at `/api/v1/analytics/` serving ClickHouse data
- An **Identity Service** at `/api/v1/identity/` managing persons and org structure

This change prepares the frontend API layer to connect to that backend when it is ready,
while keeping mock mode fully functional in the meantime.

---

## Goals

- Add auth infrastructure (OIDC config fetch, JWT injection, session expiry handling)
- Redesign `InsightApiService` endpoint paths to match the backend contract
- Add `X-Tenant-ID` header injection per backend multi-tenancy design
- Keep mock mode working — no real backend required

## Non-Goals

- OIDC callback page and redirect logic (separate change)
- Token persistence strategy (localStorage vs memory — deferred)
- Actual OIDC provider integration (requires backend to be running)
- `POST /api/v1/analytics/metrics/query` generic OData endpoint

---

## Decisions

### 1. View endpoints, not raw metric queries

The backend Analytics API has a generic `POST /metrics/query` (OData) endpoint. The
frontend does **not** use it directly. Instead, we propose screen-specific view endpoints
(`/views/executive`, `/views/team`, `/views/ic/{personId}`) that return pre-aggregated,
screen-ready data including computed bullet chart visual positions (`bar_left_pct`, etc.).

**Rationale:** Computing visual positions client-side from raw metrics would couple UI
rendering logic to data aggregation. A single RTT returning the full screen payload is
simpler and faster. The backend can implement view endpoints as thin compositions over
the OData query layer.

**Trade-off:** View endpoints create tighter coupling between frontend screen structure
and the Analytics API. Adding a new widget to Executive View will require a backend
contract change. This is acceptable at the current stage; dashboard config API
(`/api/v1/analytics/dashboards`) can replace view endpoints in a future iteration.

### 2. AuthPlugin reads store via `getStore()`

HAI3's `RestPluginHooks.onRequest` receives only a `RestRequestContext` — no Redux
access. The `AuthPlugin` uses `getStore()` from `@hai3/react` (a singleton accessor)
to read the token at request time.

**Rationale:** Injecting `getToken` via constructor config was considered, but
`getStore()` is the established HAI3 pattern for reading state outside React components.
No circular dependency: `getStore` is imported from `@hai3/react`, not from app slices.

**Header immutability:** `RestRequestContext.headers` is `readonly`. The plugin returns
a new context object: `{ ...context, headers: { ...context.headers, Authorization: ... } }`.

### 3. X-Tenant-ID injected in AuthPlugin

The backend enforces tenant isolation on every request. The tenant ID is available in
HAI3's `TENANT_SLICE_NAME` slice (`state[TENANT_SLICE_NAME].tenant?.id`).

`AuthPlugin` injects both headers when present:
```
Authorization: Bearer <token>
X-Tenant-ID: <tenantId>
```

**Rationale:** Adding this now costs nothing. Adding it later would require touching
every effect file. The architect explicitly flagged this risk.

### 4. Auth state lives at app level, not in a screenset

`authSlice`, `authEffects`, `AuthApiService` are placed under `src/app/` — not inside
`src/screensets/insight/`. All screensets will eventually share the same auth layer.

**Rationale:** SCREENSETS.md forbids sharing services between screensets. Auth is not
a screenset concern — it is an application concern.

### 5. IdentityApiService at app level

`GET /api/v1/identity/persons/me` is used to bootstrap the current user on login. This
is an app-level concern (any screenset may need the current user). The service lives in
`src/app/api/` alongside `AccountsApiService`.

---

## File Layout

```
src/app/
├── types/
│   └── auth.ts                  ← OidcConfig, ProblemDetail (NEW)
├── api/
│   ├── AuthApiService.ts        ← GET /auth/config (NEW)
│   └── IdentityApiService.ts    ← GET /api/v1/identity/persons/me (NEW)
├── events/
│   └── authEvents.ts            ← auth:configLoaded, auth:tokenStored,
│                                   auth:sessionExpired (NEW)
├── slices/
│   └── authSlice.ts             ← token, config, status (NEW directory + file)
├── effects/
│   └── authEffects.ts           ← event → slice dispatch (NEW)
└── plugins/
    └── AuthPlugin.ts            ← Bearer + X-Tenant-ID injection (NEW directory + file)

src/screensets/insight/api/
├── insightApiService.ts         ← updated paths + AuthPlugin (UPDATED)
└── mocks.ts                     ← updated mock map keys (UPDATED)
```

---

## Data Flow

### Auth bootstrap

```
main.tsx mounts
  → Layout.tsx useEffect → fetchCurrentUser()
  → bootstrapEffects: 'app/user/fetch' → AccountsApiService.getCurrentUser()

[Auth path — separate, triggered by OIDC callback later]
  → auth:tokenStored emitted (by OIDC callback handler, future change)
  → authEffects: setToken(token), setStatus('authenticated')

Every API call:
  → InsightApiService.getExecutiveViewData('month')
  → RestProtocol sends request
  → AuthPlugin.onRequest: reads token via getStore(), injects Authorization + X-Tenant-ID
  → Request reaches backend

401 response:
  → AuthPlugin.onError: emits auth:sessionExpired
  → authEffects: setToken(null), setStatus('expired')
  → [Future: App redirects to OIDC login]
```

### Insight data flow (unchanged)

```
Screen mounts → action dispatched → event emitted
→ effect subscribes → InsightApiService.get*() called
→ mock or real HTTP → response → event emitted
→ effect dispatches to slice → selector → component re-renders
```

---

## API Contract

### `AuthApiService` — base URL: `/`

| Method | HTTP | Path | Response |
|--------|------|------|----------|
| `getConfig()` | GET | `/auth/config` | `OidcConfig` |

Mock: returns `{ issuer_url: 'http://localhost:9000', client_id: 'insight-local', redirect_uri: 'http://localhost:5173/callback', scopes: ['openid','profile','email'], response_type: 'code' }`

### `IdentityApiService` — base URL: `/api/v1/identity`

| Method | HTTP | Path | Response |
|--------|------|------|----------|
| `getMe()` | GET | `/persons/me` | `PersonData` |

Mock: returns the same `PersonData` as the current mock user.

### `InsightApiService` — base URL: `/api/v1/analytics`

| Method | HTTP | Old path | New path |
|--------|------|----------|----------|
| `getExecutiveViewData(period)` | GET | `/executive-view/{p}` | `/views/executive?period={p}` |
| `getTeamViewData(period)` | GET | `/team-view/{p}` | `/views/team?period={p}` |
| `getIcDashboardData(personId, period)` | GET | `/ic-dashboard/{id}/{p}` | `/views/ic/{id}?period={p}` |
| `getIcDrillData(personId, drillId)` | GET | `/ic-dashboard/{id}/drill/{d}` | `/views/ic/{id}/drill/{d}` |
| `getTeamDrillData(drillId, period)` | GET | `/team-view/drill/{d}/{p}` | `/views/team/drill/{d}?period={p}` |

---

## Types

### New: `src/app/types/auth.ts`

```ts
interface OidcConfig {
  issuer_url: string;
  client_id: string;
  redirect_uri: string;
  scopes: string[];
  response_type: 'code';
}

interface ProblemDetail {   // RFC 9457
  type: string;
  title: string;
  status: number;
  detail?: string;
  instance?: string;
}
```

### New: `src/app/slices/authSlice.ts`

```ts
interface AuthState {
  token: string | null;
  config: OidcConfig | null;
  status: 'idle' | 'loading' | 'authenticated' | 'expired';
}
```

Selectors: `selectAuthToken`, `selectOidcConfig`, `selectAuthStatus`

---

## Open Questions

| # | Question | Impact |
|---|----------|--------|
| OQ-1 | Will token refresh (`refresh_token`) be supported, or always full re-auth on 401? | UX: users may be redirected to login mid-session |
| OQ-2 | Does the backend expect `X-Tenant-ID` as a UUID or a slug? | Header format in AuthPlugin |
| OQ-3 | Single-tenant deployment for now — is `X-Tenant-ID` mandatory or optional? | Whether to skip header when tenant is null |

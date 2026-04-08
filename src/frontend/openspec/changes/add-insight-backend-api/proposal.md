# Proposal: add-insight-backend-api

## Context

Backend repo: `cyberfabric/insight` (Rust + ModKit framework)
- PR #59: api-gateway with OIDC/JWT auth
- PR #49: backend DESIGN — 8 microservices, all on `/api/v1/{service}/` prefix

Key backend findings:
- Auth: OIDC Authorization Code + PKCE; `GET /auth/config` returns OIDC params (public endpoint)
- All protected endpoints require `Authorization: Bearer <jwt>`
- 401 → re-initiate OIDC flow (no silent refresh)
- Errors: RFC 9457 Problem Details format
- Analytics data: `POST /api/v1/analytics/metrics/query` (OData) — generic query
- Current backend has no screen-specific view endpoints → we define the contract

---

## What We Propose the Backend Implements

The frontend needs pre-aggregated, screen-ready data (with computed bullet chart
positioning: `bar_left_pct`, `median_left_pct`, etc.). We propose **view endpoints**
as part of the Analytics API service:

```
GET  /api/v1/analytics/views/executive?period={week|month|quarter|year}
     → ExecViewData

GET  /api/v1/analytics/views/team?period={week|month|quarter|year}
     → TeamViewData

GET  /api/v1/analytics/views/ic/{personId}?period={week|month|quarter|year}
     → IcDashboardData  (404 if person not found)

GET  /api/v1/analytics/views/ic/{personId}/drill/{drillId}
     → DrillData

GET  /api/v1/analytics/views/team/drill/{drillId}?period={week|month|quarter|year}
     → DrillData
```

Plus existing backend endpoints the frontend already needs:
```
GET  /auth/config                        → OidcConfig  (api-gateway, public, no token)
GET  /api/v1/identity/persons/me         → PersonData  (identity-service, authenticated)
```

---

## Auth Flow

```
App start
  → fetch GET /auth/config               (no token needed)
  → store OidcConfig in Redux authSlice
  → build OIDC authorization URL
  → redirect user to OIDC provider (Okta / Keycloak / etc.)
  → user logs in, gets redirected back with ?code=...
  → exchange code → access_token
  → store token in Redux authSlice

Every API call
  → AuthPlugin reads token via injected getToken(), adds:
    Authorization: Bearer <access_token>

401 received
  → AuthPlugin emits auth:sessionExpired
  → App clears token, re-initiates OIDC flow
```

---

## Frontend Changes

### New files under `src/app/`

**`src/app/types/auth.ts`**
```ts
export interface OidcConfig {
  issuer_url: string;
  client_id: string;
  redirect_uri: string;
  scopes: string[];
  response_type: 'code';
}

export interface ProblemDetail {
  type: string;
  title: string;
  status: number;
  detail?: string;
  instance?: string;
}
```

**`src/app/api/AuthApiService.ts`**
- Extends `BaseApiService`, base URL `/`
- Method: `getConfig(): Promise<OidcConfig>` → `GET /auth/config`
- Registered mock returns static localhost OidcConfig
- `apiRegistry.register(AuthApiService)` in main.tsx

**`src/app/api/IdentityApiService.ts`**
- Extends `BaseApiService`, base URL `/api/v1/identity`
- Method: `getMe(): Promise<PersonData>` → `GET /persons/me`
- Registered mock returns current user data
- `apiRegistry.register(IdentityApiService)` in main.tsx

**`src/app/events/authEvents.ts`**
- `auth:configLoaded` — payload: `{ config: OidcConfig }`
- `auth:tokenStored` — payload: `{ token: string }`
- `auth:sessionExpired` — no payload

**`src/app/slices/authSlice.ts`** ← new directory
```ts
interface AuthState {
  token: string | null;
  config: OidcConfig | null;
  status: 'idle' | 'loading' | 'authenticated' | 'expired';
}
// reducers: setToken, setConfig, setStatus
// selectors: selectAuthToken, selectOidcConfig, selectAuthStatus
```

**`src/app/effects/authEffects.ts`**
- Subscribe `auth:configLoaded` → dispatch `setConfig()`
- Subscribe `auth:tokenStored` → dispatch `setToken()` + `setStatus('authenticated')`
- Subscribe `auth:sessionExpired` → dispatch `setToken(null)` + `setStatus('expired')`

**`src/app/plugins/AuthPlugin.ts`**

Extends `RestPlugin` (not `ApiPluginBase`) to avoid `onError` signature conflicts.
Registered via `restProtocol.plugins.add(new AuthPlugin())`, not `this.registerPlugin()`.

- `onRequest`: reads token via `getStore()` + `selectAuthToken()`, injects `Authorization: Bearer` and `X-Tenant-ID` headers
- `onError`: checks for 401 status on first attempt (`retryCount === 0`), emits `auth:sessionExpired`

### Updated: `src/screensets/insight/api/insightApiService.ts`

- Base URL: `/api/v1/analytics` (was `/api/insight`)
- Register `AuthPlugin` with `getToken` from store
- Updated endpoint paths:

| Method | Old path | New path |
|--------|----------|----------|
| `getExecutiveViewData(period)` | `/executive-view/{period}` | `/views/executive?period={period}` |
| `getTeamViewData(period)` | `/team-view/{period}` | `/views/team?period={period}` |
| `getIcDashboardData(personId, period)` | `/ic-dashboard/{personId}/{period}` | `/views/ic/{personId}?period={period}` |
| `getIcDrillData(personId, drillId)` | `/ic-dashboard/{personId}/drill/{drillId}` | `/views/ic/{personId}/drill/{drillId}` |
| `getTeamDrillData(drillId, period)` | `/team-view/drill/{drillId}/{period}` | `/views/team/drill/{drillId}?period={period}` |

### Updated: `src/screensets/insight/api/mocks.ts`

Mock map keys updated to new paths. Currently uses `/api/insight/` prefix — after change
will use `/api/v1/analytics/views/` to match real endpoints.

---

## What Stays the Same

- All TypeScript types (`ExecViewData`, `TeamViewData`, `IcDashboardData`, etc.)
- All Redux slices, selectors, and action files
- All effects files (method signatures don't change, just the service internals)
- `RestMockPlugin` stays — mock mode still works without a real backend

---

## Out of Scope

- Actual OIDC callback/redirect page (separate feature)
- Token persistence (localStorage vs memory — deferred decision)
- Multi-tenancy headers
- `POST /api/v1/analytics/metrics/query` generic OData endpoint (different use case)

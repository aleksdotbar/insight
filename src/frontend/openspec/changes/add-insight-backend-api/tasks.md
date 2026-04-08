# Tasks: add-insight-backend-api

## Phase 1 — Auth infrastructure (app-level)

- [x] Create `src/app/types/auth.ts`
  - [x] `OidcConfig` interface
  - [x] `ProblemDetail` interface (RFC 9457)

- [x] Create `src/app/api/AuthApiService.ts`
  - [x] Extend `BaseApiService`, domain `auth`, base URL `/`
  - [x] `getConfig(): Promise<OidcConfig>` → `GET /auth/config`
  - [x] Register `RestMockPlugin` with static localhost OidcConfig
  - [x] `apiRegistry.register(AuthApiService)`

- [x] Create `src/app/api/IdentityApiService.ts`
  - [x] Extend `BaseApiService`, domain `identity`, base URL `/api/v1/identity`
  - [x] `getMe(): Promise<PersonData>` → `GET /persons/me`
  - [x] Register `RestMockPlugin` returning current user data
  - [x] `apiRegistry.register(IdentityApiService)`

- [x] Create `src/app/events/authEvents.ts`
  - [x] Local `DOMAIN_ID = 'auth'`
  - [x] `auth:configLoaded` — payload: `{ config: OidcConfig }`
  - [x] `auth:tokenStored` — payload: `{ token: string }`
  - [x] `auth:sessionExpired` — no payload

- [x] Create `src/app/slices/authSlice.ts`
  - [x] State: `{ token: string | null; config: OidcConfig | null; status: 'idle' | 'loading' | 'authenticated' | 'expired' }`
  - [x] Reducers: `setToken`, `setConfig`, `setStatus`
  - [x] Selectors: `selectAuthToken`, `selectOidcConfig`, `selectAuthStatus`
  - [x] Augment `RootState` with `'auth': AuthState`

- [x] Create `src/app/effects/authEffects.ts`
  - [x] Subscribe `auth:configLoaded` → `dispatch(setConfig(payload.config))`
  - [x] Subscribe `auth:tokenStored` → `dispatch(setToken(payload.token)); dispatch(setStatus('authenticated'))`
  - [x] Subscribe `auth:sessionExpired` → `dispatch(setToken(null)); dispatch(setStatus('expired'))`

- [x] Create `src/app/plugins/AuthPlugin.ts`
  - [x] Implements plugin hooks interface from `@hai3/react`
  - [x] `beforeRequest`: read `selectAuthToken(store.getState())`, add `Authorization: Bearer` header if present
  - [x] `onError` with status 401: check `retryCount === 0`, emit `auth:sessionExpired`, return without retry

- [x] Register in `src/app/main.tsx`
  - [x] `registerSlice(authSlice, initAuthEffects)`
  - [x] `apiRegistry.register(AuthApiService)`
  - [x] `apiRegistry.register(IdentityApiService)`

- [x] `npm run type-check` — must pass

## Phase 2 — InsightApiService endpoint redesign

- [x] Update `InsightApiService` base URL → `/api/v1/analytics`
- [x] Register `AuthPlugin` in constructor
- [x] Update `getExecutiveViewData(period)` → `GET /views/executive?period={period}`
- [x] Update `getTeamViewData(period)` → `GET /views/team?period={period}`
- [x] Update `getIcDashboardData(personId, period)` → `GET /views/ic/{personId}?period={period}`
- [x] Update `getIcDrillData(personId, drillId)` → `GET /views/ic/{personId}/drill/{drillId}`
- [x] Update `getTeamDrillData(drillId, period)` → `GET /views/team/drill/{drillId}?period={period}`
- [x] Update mock map keys in `mocks.ts` to match new paths
- [x] Verify `executiveViewEffects.ts` — no call signature changes needed
- [x] Verify `teamViewEffects.ts` — no call signature changes needed
- [x] Verify `icDashboardEffects.ts` — no call signature changes needed
- [x] `npm run type-check` — must pass

## Phase 3 — Validation

- [x] Mock mode: Executive View loads, no console errors
- [x] Mock mode: Team View loads
- [x] Mock mode: IC Dashboard loads
- [x] Mock mode: Drill modal opens with data
- [x] Redux DevTools: `auth` slice visible with `status: 'idle'`
- [x] Redux DevTools: API events fire correctly
- [x] `npm run arch:check` — must pass

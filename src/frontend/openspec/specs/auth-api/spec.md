# Spec: auth-api

**Service:** API Gateway
**Base path:** `/`
**Auth:** None (public endpoints)

---

## Overview

The Auth API provides OIDC bootstrapping configuration. It is served by the API Gateway
and does not require an `Authorization` header. All other API services require a JWT
obtained via the OIDC flow described here.

---

## Types

```ts
type OidcConfig = {
  issuer_url: string;       // OIDC provider base URL
  client_id: string;        // OAuth2 client identifier
  redirect_uri: string;     // Callback URL after login
  scopes: string[];         // e.g. ['openid', 'profile', 'email']
  response_type: 'code';    // Always 'code' (PKCE flow)
};

/** RFC 9457 Problem Details — standard error format for all services */
type ProblemDetail = {
  type: string;
  title: string;
  status: number;
  detail?: string;
  instance?: string;
};
```

---

## Endpoints

### `GET /auth/config`

Returns OIDC provider configuration. Called on app start before any authenticated request.

**Response:** `OidcConfig`

**Errors:** None expected (public endpoint, no token required).

---

## Auth Flow

```
App start
  → GET /auth/config                    (no token)
  → store OidcConfig in Redux authSlice
  → build OIDC authorization URL from config
  → redirect user to OIDC provider

User authenticates at provider
  → redirected back with ?code=...
  → exchange code → access_token        (OIDC callback page — separate change)
  → emit auth:tokenStored
  → authEffects: store token in authSlice

Every subsequent API call
  → AuthPlugin reads token from authSlice via getStore()
  → injects: Authorization: Bearer <token>
  → injects: X-Tenant-ID: <tenantId>   (from HAI3 tenant slice)

401 response
  → AuthPlugin emits auth:sessionExpired
  → authEffects: clear token, set status 'expired'
  → [Future: redirect to OIDC login]
```

---

## Frontend Implementation

- `src/app/types/auth.ts` — `OidcConfig`, `ProblemDetail`
- `src/app/api/AuthApiService.ts` — `getConfig(): Promise<OidcConfig>`
- `src/app/plugins/AuthPlugin.ts` — injects headers, handles 401
- `src/app/slices/authSlice.ts` — `token`, `config`, `status`
- `src/app/effects/authEffects.ts` — event → slice subscriptions
- `src/app/events/authEvents.ts` — `auth:configLoaded`, `auth:tokenStored`, `auth:sessionExpired`

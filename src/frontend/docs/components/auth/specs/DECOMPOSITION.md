# Decomposition: Auth

<!-- toc -->

- [1. Overview](#1-overview)
- [2. Entries](#2-entries)
  - [2.1 OIDC Core ⏳ HIGH](#21-oidc-core--high)
  - [2.2 Auth Flow ⏳ HIGH](#22-auth-flow--high)
  - [2.3 Route Protection ⏳ HIGH](#23-route-protection--high)
  - [2.4 Logout ⏳ MEDIUM](#24-logout--medium)
- [3. Feature Dependencies](#3-feature-dependencies)

<!-- /toc -->

## 1. Overview

The Auth DESIGN is decomposed into four features organized by dependency order. The foundation feature (OIDC Core) establishes the `oidc-client-ts` integration and event wiring. Subsequent features build on it to implement login flow, route protection, and logout independently.

**Decomposition Strategy**:
- Features grouped by functional cohesion around OIDC lifecycle stages
- Foundation-first: OidcManager and event infrastructure must exist before any flow
- Auth Flow and Route Protection are independent of each other after foundation
- Logout depends on both foundation (OidcManager) and auth flow (established session)
- 100% coverage of all DESIGN components, sequences, and requirements verified

## 2. Entries

**Overall implementation status:**

- [ ] `p1` - **ID**: `cpt-auth-status-overall`

### 2.1 [OIDC Core](feature-oidc-core/) ⏳ HIGH

- [ ] `p1` - **ID**: `cpt-auth-feature-oidc-core`

- **Purpose**: Establish the OIDC infrastructure by integrating `oidc-client-ts` UserManager into the HAI3 event-driven architecture. This feature creates the OidcManager service, configures sessionStorage token persistence, and wires UserManager events to the existing authEvents/authEffects/authSlice pipeline.

- **Depends On**: None

- **Scope**:
  - Install `oidc-client-ts` npm dependency
  - Create OidcManager service wrapping UserManager
  - Configure WebStorageStateStore with sessionStorage
  - Fetch OIDC config from backend via AuthApiService
  - Bridge UserManager events (userLoaded, userUnloaded, silentRenewError, accessTokenExpired) to HAI3 authEvents
  - Wire authEffects to dispatch token/config/status to authSlice
  - Add OidcManagerConfig type to auth types
  - Enable automaticSilentRenew in UserManager settings

- **Out of scope**:
  - Login redirect UI or callback handling (Auth Flow feature)
  - Route guarding (Route Protection feature)
  - Logout flow (Logout feature)

- **Requirements Covered**:

  - [ ] `p1` - `cpt-auth-fr-config-discovery`
  - [ ] `p1` - `cpt-auth-fr-token-storage`
  - [ ] `p1` - `cpt-auth-fr-silent-renew`
  - [ ] `p1` - `cpt-auth-nfr-token-security`
  - [ ] `p2` - `cpt-auth-nfr-provider-compat`

- **Design Principles Covered**:

  - [ ] `p1` - `cpt-auth-principle-event-driven`
  - [ ] `p1` - `cpt-auth-principle-provider-agnostic`
  - [ ] `p2` - `cpt-auth-principle-reuse`

- **Design Constraints Covered**:

  - [ ] `p1` - `cpt-auth-constraint-library`
  - [ ] `p1` - `cpt-auth-constraint-storage`
  - [ ] `p1` - `cpt-auth-constraint-hai3`

- **Domain Model Entities**:
  - OidcConfig
  - OidcManagerConfig
  - AuthState
  - AuthStatus

- **Design Components**:

  - [ ] `p1` - `cpt-auth-component-oidc-manager`

- **API**:
  - GET /auth/config (existing, via AuthApiService)

- **Sequences**:

  - [ ] `p1` - `cpt-auth-seq-silent-renew`

### 2.2 [Auth Flow](feature-auth-flow/) ⏳ HIGH

- [ ] `p1` - **ID**: `cpt-auth-feature-auth-flow`

- **Purpose**: Implement the OIDC login ceremony — redirect to the OIDC provider, handle the callback with code-to-token exchange, and restore the user's originally requested page after authentication.

- **Depends On**: `cpt-auth-feature-oidc-core`

- **Scope**:
  - Create CallbackScreen component at `/callback` route
  - Implement OidcManager.signIn() triggering signinRedirect with PKCE
  - Implement OidcManager.handleCallback() completing code exchange
  - Emit TokenStored event on successful callback
  - Fetch user identity (GET /identity/persons/me) after TokenStored
  - Redirect user to originally requested URL after authentication
  - Handle callback errors with retry option
  - Register callback route in app routing

- **Out of scope**:
  - Automatic route protection (Route Protection feature)
  - Logout (Logout feature)
  - Silent renew (covered in OIDC Core)

- **Requirements Covered**:

  - [ ] `p1` - `cpt-auth-fr-login-redirect`
  - [ ] `p1` - `cpt-auth-fr-callback`

- **Design Principles Covered**:

  - [ ] `p1` - `cpt-auth-principle-event-driven`

- **Design Constraints Covered**:

  - [ ] `p1` - `cpt-auth-constraint-hai3`

- **Domain Model Entities**:
  - PersonMe

- **Design Components**:

  - [ ] `p1` - `cpt-auth-component-callback-screen`

- **API**:
  - GET /api/v1/identity/persons/me (existing, via IdentityApiService)

- **Sequences**:

  - [ ] `p1` - `cpt-auth-seq-login`

### 2.3 [Route Protection](feature-route-protection/) ⏳ HIGH

- [ ] `p1` - **ID**: `cpt-auth-feature-route-protection`

- **Purpose**: Prevent any application screen from rendering until the user is authenticated. Handle session expiry by re-triggering the login flow.

- **Depends On**: `cpt-auth-feature-oidc-core`

- **Scope**:
  - Create AuthGuard component wrapping Layout
  - Read authSlice.status to determine render behavior (loading / authenticated / expired)
  - Show loading state during auth verification
  - Trigger OidcManager.signIn() when status is expired
  - Preserve current URL for post-auth redirect
  - Handle 401 from AuthPlugin → SessionExpired → re-auth

- **Out of scope**:
  - Role-based access control within screens
  - Per-route permission checks

- **Requirements Covered**:

  - [ ] `p1` - `cpt-auth-fr-protected-routes`
  - [ ] `p2` - `cpt-auth-fr-session-expiry`

- **Design Principles Covered**:

  - [ ] `p2` - `cpt-auth-principle-reuse`

- **Design Constraints Covered**:

  - [ ] `p1` - `cpt-auth-constraint-hai3`

- **Domain Model Entities**:
  - AuthState
  - AuthStatus

- **Design Components**:

  - [ ] `p1` - `cpt-auth-component-auth-guard`
  - [ ] `p1` - `cpt-auth-component-auth-plugin`

### 2.4 [Logout](feature-logout/) ⏳ MEDIUM

- [ ] `p2` - **ID**: `cpt-auth-feature-logout`

- **Purpose**: Enable users to end their session by clearing local auth state and performing RP-initiated logout at the OIDC provider.

- **Depends On**: `cpt-auth-feature-oidc-core`, `cpt-auth-feature-auth-flow`

- **Scope**:
  - Add LogoutRequested event to authEvents
  - Implement logout action emitting LogoutRequested
  - Add LogoutRequested listener in authEffects → clearAuth()
  - Implement OidcManager.signOut() calling signoutRedirect()
  - Clear sessionStorage tokens on logout
  - Redirect to post-logout URI after provider logout
  - Add logout button/action in UI (Menu or user profile area)

- **Out of scope**:
  - Forced logout from admin panel
  - Session timeout countdown UI

- **Requirements Covered**:

  - [ ] `p2` - `cpt-auth-fr-logout`

- **Design Principles Covered**:

  - [ ] `p1` - `cpt-auth-principle-event-driven`

- **Design Constraints Covered**:

  - [ ] `p1` - `cpt-auth-constraint-hai3`

- **Domain Model Entities**:
  - AuthState

- **Design Components**:

  - [ ] `p1` - `cpt-auth-component-oidc-manager`

- **Sequences**:

  - [ ] `p2` - `cpt-auth-seq-logout`

---

## 3. Feature Dependencies

```text
cpt-auth-feature-oidc-core
    ↓
    ├─→ cpt-auth-feature-auth-flow
    │       ↓
    │       └─→ cpt-auth-feature-logout
    └─→ cpt-auth-feature-route-protection
```

**Dependency Rationale**:

- `cpt-auth-feature-auth-flow` requires `cpt-auth-feature-oidc-core`: login redirect and callback depend on OidcManager and UserManager being initialized with OIDC config
- `cpt-auth-feature-route-protection` requires `cpt-auth-feature-oidc-core`: AuthGuard reads authSlice status populated by OidcManager events, and triggers signIn() on expired
- `cpt-auth-feature-logout` requires `cpt-auth-feature-oidc-core` and `cpt-auth-feature-auth-flow`: logout needs OidcManager.signOut() and a valid session to end
- `cpt-auth-feature-auth-flow` and `cpt-auth-feature-route-protection` are independent of each other and can be developed in parallel after OIDC Core

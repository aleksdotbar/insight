# PRD — Auth


<!-- toc -->

- [1. Overview](#1-overview)
  - [1.1 Purpose](#11-purpose)
  - [1.2 Background / Problem Statement](#12-background--problem-statement)
  - [1.3 Goals (Business Outcomes)](#13-goals-business-outcomes)
  - [1.4 Glossary](#14-glossary)
- [2. Actors](#2-actors)
  - [2.1 Human Actors](#21-human-actors)
  - [2.2 System Actors](#22-system-actors)
- [3. Operational Concept & Environment](#3-operational-concept--environment)
  - [3.1 Module-Specific Environment Constraints](#31-module-specific-environment-constraints)
- [4. Scope](#4-scope)
  - [4.1 In Scope](#41-in-scope)
  - [4.2 Out of Scope](#42-out-of-scope)
- [5. Functional Requirements](#5-functional-requirements)
  - [5.1 OIDC Configuration](#51-oidc-configuration)
  - [5.2 Authentication Flow](#52-authentication-flow)
  - [5.3 Token Lifecycle](#53-token-lifecycle)
  - [5.4 Route Protection](#54-route-protection)
  - [5.5 Logout](#55-logout)
- [6. Non-Functional Requirements](#6-non-functional-requirements)
  - [6.1 NFR Inclusions](#61-nfr-inclusions)
  - [6.2 NFR Exclusions](#62-nfr-exclusions)
- [7. Public Library Interfaces](#7-public-library-interfaces)
  - [7.1 Public API Surface](#71-public-api-surface)
  - [7.2 External Integration Contracts](#72-external-integration-contracts)
- [8. Use Cases](#8-use-cases)
  - [UC-001 First-Time Login](#uc-001-first-time-login)
  - [UC-002 Silent Token Renewal](#uc-002-silent-token-renewal)
  - [UC-003 User Logout](#uc-003-user-logout)
- [9. Acceptance Criteria](#9-acceptance-criteria)
- [10. Dependencies](#10-dependencies)
- [11. Assumptions](#11-assumptions)
- [12. Risks](#12-risks)

<!-- /toc -->

- [ ] `p1` - **ID**: `cpt-auth-prd-auth`
## 1. Overview

### 1.1 Purpose

The Auth module provides user authentication for the Insight Frontend application via the OpenID Connect (OIDC) protocol. It uses the `oidc-client-ts` library to implement the Authorization Code flow with PKCE, handling login, logout, token lifecycle, and route protection.

### 1.2 Background / Problem Statement

The Insight Frontend application has foundational auth infrastructure in place — a Redux auth slice, typed events and effects, an AuthPlugin that injects Bearer tokens into API requests and handles 401 responses, and API services for fetching OIDC configuration and current user identity. However, the actual OIDC ceremony (redirect to provider, callback handling, code-to-token exchange) is not implemented. Users currently access the application without authentication, which means no identity verification, no session management, and no token-based API authorization.

**Target Users**:

- Employees of organizations using Insight for engineering analytics
- IT administrators configuring OIDC providers for their organizations

**Key Problems Solved**:

- No user identity verification — anyone with the URL can access dashboards
- No token-based API authorization — backend APIs cannot enforce access control
- No session lifecycle — no refresh, no expiry handling, no logout

### 1.3 Goals (Business Outcomes)

**Success Criteria**:

- 100% of users authenticated before accessing any dashboard data (Baseline: 0% — no authentication; Target: v1.0)
- Token refresh completes without session interruption for active users (Baseline: N/A — no tokens; Target: v1.0)
- Time from "Login" click to dashboard render ≤ 3 seconds on stable network (Baseline: N/A; Target: v1.0)

**Capabilities**:

- Authenticate users via corporate OIDC providers
- Maintain user sessions with automatic token renewal
- Protect all application routes from unauthenticated access

### 1.4 Glossary

| Term | Definition |
|------|------------|
| OIDC | OpenID Connect — identity layer on top of OAuth 2.0 |
| PKCE | Proof Key for Code Exchange — prevents authorization code interception in public clients |
| Authorization Code Flow | OIDC flow where the client receives a code and exchanges it for tokens server-side or via PKCE |
| Silent Renew | Background token refresh using a hidden iframe or refresh token without user interaction |
| RP-Initiated Logout | Logout initiated by the Relying Party (frontend) that also ends the session at the OIDC provider |
| IdP | Identity Provider — the external OIDC service (Keycloak, Azure AD, Okta, Auth0) |

## 2. Actors

### 2.1 Human Actors

#### End User

**ID**: `cpt-auth-actor-end-user`

**Role**: An employee of an organization who accesses Insight dashboards to view engineering analytics. Authenticates via their organization's OIDC provider.
**Needs**: Seamless login experience, persistent session without repeated logins, secure logout.

### 2.2 System Actors

#### OIDC Provider

**ID**: `cpt-auth-actor-oidc-provider`

**Role**: External Identity Provider (Keycloak, Azure AD, Okta, Auth0) that authenticates users and issues tokens. Provides the authorization endpoint, token endpoint, and userinfo endpoint.

#### Backend API

**ID**: `cpt-auth-actor-backend-api`

**Role**: The Insight backend that serves OIDC configuration via `GET /auth/config` and current user identity via `GET /api/v1/identity/persons/me`. Validates Bearer tokens on all protected endpoints.

## 3. Operational Concept & Environment

### 3.1 Module-Specific Environment Constraints

- Requires `oidc-client-ts` library (npm dependency)
- OIDC provider must support Authorization Code flow with PKCE
- Backend must expose `GET /auth/config` returning OIDC discovery parameters
- Environment variables must be configurable per deployment for OIDC endpoints

## 4. Scope

### 4.1 In Scope

- OIDC configuration discovery from backend API
- Authorization Code flow with PKCE via `oidc-client-ts`
- Callback page handling code-to-token exchange
- Token storage in memory (not localStorage/sessionStorage)
- Silent token renewal before expiry
- Protected routing — redirect to OIDC login when unauthenticated
- Logout — local state cleanup and RP-initiated logout at provider
- Environment configuration for OIDC endpoints

### 4.2 Out of Scope

- User registration or self-service account management
- Role-based access control or permission management on the frontend
- Multi-tenant switching within a single session
- SSO federation across multiple Insight applications
- Custom login UI — the OIDC provider's login page is used

## 5. Functional Requirements

### 5.1 OIDC Configuration

#### Configuration Discovery

- [ ] `p1` - **ID**: `cpt-auth-fr-config-discovery`

The system MUST fetch OIDC configuration from `GET /auth/config` on application bootstrap and store it in the auth state. The configuration MUST include issuer URL, client ID, redirect URI, post-logout redirect URI, and scopes.

**Rationale**: The OIDC provider parameters vary per deployment; fetching them from the backend ensures a single source of truth without hardcoding provider-specific URLs in the frontend.

**Actors**: `cpt-auth-actor-backend-api`

### 5.2 Authentication Flow

#### OIDC Login Redirect

- [ ] `p1` - **ID**: `cpt-auth-fr-login-redirect`

The system MUST redirect unauthenticated users to the OIDC provider's authorization endpoint using Authorization Code flow with PKCE. The redirect MUST include `response_type=code`, a dynamically generated `code_verifier`/`code_challenge` pair, and the configured scopes.

**Rationale**: PKCE is mandatory for public clients (SPA) to prevent authorization code interception attacks.

**Actors**: `cpt-auth-actor-end-user`, `cpt-auth-actor-oidc-provider`

#### Callback Handling

- [ ] `p1` - **ID**: `cpt-auth-fr-callback`

The system MUST provide a `/callback` route that receives the authorization code from the OIDC provider and exchanges it for access, ID, and refresh tokens. After successful exchange, the system MUST store tokens in memory, emit `TokenStored` event, and redirect the user to their originally requested page.

**Rationale**: The callback completes the OIDC ceremony and transitions the user from unauthenticated to authenticated state.

**Actors**: `cpt-auth-actor-end-user`, `cpt-auth-actor-oidc-provider`

#### Token Storage

- [ ] `p1` - **ID**: `cpt-auth-fr-token-storage`

The system MUST store tokens in `sessionStorage` to survive page reloads within the same browser tab. The system MUST NOT persist tokens to `localStorage` or cookies accessible to JavaScript.

**Rationale**: `sessionStorage` preserves the session across page reloads without exposing tokens across tabs or browser restarts. This provides a good balance between UX (no re-authentication on reload) and security (tokens are scoped to the tab and cleared when it closes).

**Actors**: `cpt-auth-actor-end-user`

### 5.3 Token Lifecycle

#### Silent Token Renewal

- [ ] `p1` - **ID**: `cpt-auth-fr-silent-renew`

The system MUST automatically renew tokens before they expire using `oidc-client-ts` silent renew mechanism. Renewal MUST happen transparently without interrupting the user's workflow. If silent renew fails, the system MUST emit `SessionExpired` event.

**Rationale**: Users should not lose their session during active work. Silent renew ensures continuous access without re-authentication.

**Actors**: `cpt-auth-actor-end-user`, `cpt-auth-actor-oidc-provider`

#### Session Expiry Handling

- [ ] `p2` - **ID**: `cpt-auth-fr-session-expiry`

When silent renew fails or the backend returns 401, the system MUST clear the local auth state and redirect the user to the OIDC login flow. The system SHOULD preserve the user's current URL to restore after re-authentication.

**Rationale**: Graceful session expiry prevents users from seeing broken UI or making unauthorized API calls.

**Actors**: `cpt-auth-actor-end-user`

### 5.4 Route Protection

#### Protected Routing

- [ ] `p1` - **ID**: `cpt-auth-fr-protected-routes`

The system MUST prevent rendering any application screen until the user is authenticated. Unauthenticated users MUST be redirected to the OIDC login flow. The system MUST show a loading state during authentication verification.

**Rationale**: All Insight dashboards display organization-specific data that must only be accessible to authenticated users.

**Actors**: `cpt-auth-actor-end-user`

### 5.5 Logout

#### User Logout

- [ ] `p2` - **ID**: `cpt-auth-fr-logout`

The system MUST provide a logout action that clears local auth state (tokens, user info, Redux state) and performs RP-initiated logout at the OIDC provider. After logout, the user MUST be redirected to the post-logout redirect URI.

**Rationale**: Complete logout must end both the local session and the provider session to prevent session reuse on shared devices.

**Actors**: `cpt-auth-actor-end-user`, `cpt-auth-actor-oidc-provider`

## 6. Non-Functional Requirements

### 6.1 NFR Inclusions

#### Token Security

- [ ] `p1` - **ID**: `cpt-auth-nfr-token-security`

The system MUST NOT store tokens in `localStorage` or JavaScript-accessible cookies. Tokens MUST be stored in `sessionStorage` only, scoped to the browser tab.

**Threshold**: Zero token presence in `localStorage` or cookies at any point in the lifecycle.

**Rationale**: `sessionStorage` limits exposure to the current tab and is cleared on tab close, reducing the window for XSS-based token theft compared to `localStorage`.

#### Provider Compatibility

- [ ] `p2` - **ID**: `cpt-auth-nfr-provider-compat`

The system MUST work with any OIDC-compliant provider. Verified compatibility targets: Keycloak, Azure AD, Okta, Auth0.

**Threshold**: All four providers pass login, callback, silent renew, and logout flows in integration testing.

**Rationale**: Customers use different identity providers; the auth module must be provider-agnostic.

### 6.2 NFR Exclusions

- **Accessibility** (UX-PRD-002): Not applicable to auth module — login UI is provided by the external OIDC provider; the only frontend surfaces are a loading spinner and redirect logic
- **Internationalization** (UX-PRD-003): Not applicable — no user-facing text in the auth module itself; loading states use the existing i18n infrastructure

## 7. Public Library Interfaces

### 7.1 Public API Surface

None — the auth module is an internal application module, not a reusable library.

### 7.2 External Integration Contracts

#### OIDC Configuration Contract

- [ ] `p1` - **ID**: `cpt-auth-contract-oidc-config`

**Direction**: required from backend

**Protocol/Format**: HTTP/REST JSON

**Description**: `GET /auth/config` returns `{ issuer, clientId, redirectUri, postLogoutRedirectUri, scopes }`. This contract is already implemented in `AuthApiService`.

**Compatibility**: Backend must maintain this response shape; additions are backward-compatible, removals are breaking.

#### Identity Contract

- [ ] `p2` - **ID**: `cpt-auth-contract-identity`

**Direction**: required from backend

**Protocol/Format**: HTTP/REST JSON

**Description**: `GET /api/v1/identity/persons/me` returns current authenticated user profile. This contract is already implemented in `IdentityApiService`.

**Compatibility**: Backend must maintain this response shape; additions are backward-compatible, removals are breaking.

## 8. Use Cases

### UC-001 First-Time Login

- [ ] `p1` - **ID**: `cpt-auth-usecase-first-login`

**Actor**: `cpt-auth-actor-end-user`

**Preconditions**:
- User has a valid account in the organization's OIDC provider
- Application is loaded in the browser

**Main Flow**:
1. User navigates to the application URL
2. System detects no valid token in memory
3. System fetches OIDC config from backend
4. System redirects user to OIDC provider login page
5. User authenticates at the OIDC provider
6. OIDC provider redirects to `/callback` with authorization code
7. System exchanges code for tokens via PKCE
8. System stores tokens in memory, emits `TokenStored`
9. System fetches current user identity from backend
10. System renders the requested dashboard

**Postconditions**: User is authenticated; tokens in memory; identity loaded.

**Alternative Flows**:
- **Authentication fails at provider**: OIDC provider shows error; user remains on provider page
- **Code exchange fails**: System shows error message and offers retry
- **Backend unreachable**: System shows connection error with retry option

### UC-002 Silent Token Renewal

- [ ] `p2` - **ID**: `cpt-auth-usecase-silent-renew`

**Actor**: `cpt-auth-actor-end-user`

**Preconditions**:
- User is authenticated with a valid session
- Token is approaching expiry

**Main Flow**:
1. `oidc-client-ts` detects token nearing expiry
2. System initiates silent renew (hidden iframe or refresh token)
3. OIDC provider issues new tokens
4. System updates tokens in memory, emits `TokenStored`

**Postconditions**: User session continues uninterrupted with fresh tokens.

**Alternative Flows**:
- **Silent renew fails**: System emits `SessionExpired`, clears state, redirects to login preserving current URL

### UC-003 User Logout

- [ ] `p2` - **ID**: `cpt-auth-usecase-logout`

**Actor**: `cpt-auth-actor-end-user`

**Preconditions**:
- User is authenticated

**Main Flow**:
1. User clicks logout
2. System clears local auth state (tokens, Redux, user info)
3. System performs RP-initiated logout at OIDC provider
4. OIDC provider redirects to post-logout redirect URI

**Postconditions**: Local and provider sessions ended; user sees login prompt on next visit.

**Alternative Flows**:
- **Provider logout fails**: Local state is still cleared; user is logged out locally

## 9. Acceptance Criteria

- [ ] Unauthenticated user is redirected to OIDC provider login page within 1 second
- [ ] After successful OIDC callback, user sees their dashboard within 3 seconds
- [ ] Token renewal happens silently without visible UI interruption
- [ ] Logout clears all local state and ends the provider session
- [ ] Application works with Keycloak, Azure AD, Okta, and Auth0 providers
- [ ] No tokens are found in localStorage or cookies; tokens persist only in sessionStorage during active session

## 10. Dependencies

| Dependency | Description | Criticality |
|------------|-------------|-------------|
| `oidc-client-ts` | OIDC client library for Authorization Code + PKCE flow | p1 |
| Backend `GET /auth/config` | Provides OIDC provider configuration | p1 |
| Backend `GET /api/v1/identity/persons/me` | Returns authenticated user profile | p1 |
| Existing `authSlice` | Redux state for auth (token, config, status) | p1 |
| Existing `AuthPlugin` | Injects Bearer token into API requests, handles 401 | p1 |
| Existing `authEvents` / `authEffects` | Event-driven auth state machine | p1 |

## 11. Assumptions

- The OIDC provider is pre-configured by the customer's IT team and accessible from the user's browser
- The backend `GET /auth/config` endpoint returns correct provider parameters for the current deployment
- The OIDC provider supports Authorization Code flow with PKCE (all target providers do)
- Token expiry times are set reasonably by the provider (>5 minutes) to allow silent renew

## 12. Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| OIDC provider downtime | Users cannot authenticate; application inaccessible | Show clear error message with retry; consider cached session tolerance window |
| Silent renew blocked by browser | Third-party cookie restrictions may block iframe-based silent renew | Use refresh token rotation as primary strategy; iframe as fallback |
| sessionStorage XSS exposure | XSS attack in the same tab could read tokens from sessionStorage | Apply strict CSP headers; sanitize all user input; regular security audits |
| Provider-specific OIDC quirks | Non-standard claim names or flow behavior across providers | Abstract provider differences behind `oidc-client-ts` UserManager; test all four providers in CI |

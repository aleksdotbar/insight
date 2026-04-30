# PRD — Layout (Insight Application Shell)


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
  - [5.1 Application Bootstrap](#51-application-bootstrap)
  - [5.2 Desktop Layout Composition](#52-desktop-layout-composition)
  - [5.3 Mobile Layout Composition](#53-mobile-layout-composition)
  - [5.4 Menu Recursive Rendering](#54-menu-recursive-rendering)
  - [5.5 Active Item Highlighting](#55-active-item-highlighting)
  - [5.6 Menu Item Click Behavior](#56-menu-item-click-behavior)
  - [5.7 Role Switcher Slot](#57-role-switcher-slot)
- [6. Non-Functional Requirements](#6-non-functional-requirements)
  - [6.1 NFR Inclusions](#61-nfr-inclusions)
  - [6.2 NFR Exclusions](#62-nfr-exclusions)
- [7. Public Library Interfaces](#7-public-library-interfaces)
  - [7.1 Public API Surface](#71-public-api-surface)
  - [7.2 External Integration Contracts](#72-external-integration-contracts)
- [8. Use Cases](#8-use-cases)
  - [UC-001 User Boots the App and Lands on Their Default Screen](#uc-001-user-boots-the-app-and-lands-on-their-default-screen)
  - [UC-002 User Drills From Team Member Menu Item Into IC Dashboard](#uc-002-user-drills-from-team-member-menu-item-into-ic-dashboard)
- [9. Acceptance Criteria](#9-acceptance-criteria)
- [10. Dependencies](#10-dependencies)
- [11. Assumptions](#11-assumptions)
- [12. Risks](#12-risks)

<!-- /toc -->

## 1. Overview

### 1.1 Purpose

The Layout module is the Insight application shell — the assembly that mounts the app, kicks off authentication, wires HAI3's layout primitives (Menu, Screen, Sidebar, Footer, Popup, Overlay) into the desktop and mobile compositions, renders the recursive Insight navigation menu with active-descendant highlighting, and handles the menu-item-to-screen-with-payload navigation that Insight's domain navigation uses.

### 1.2 Background / Problem Statement

`hai3 scaffold layout` generates baseline shell components. Insight customizes them in three ways that earn this PRD:

- The mount sequence kicks off `initAuth()` and, after auth succeeds, `fetchCurrentUser()` from Identity Resolution. Without this bootstrap, no screen has the identity it needs.
- The Menu has to render a recursive subordinate / org tree (depth ≥ 4 in real tenants) with active-descendant highlighting and route a click into both a screen ID and a payload (an org-unit string or a person email) — HAI3's stock menu API knows about screens, not Insight's per-person / per-team payloads.
- A `RoleSwitcher` slot is wired into the menu footer for development tenants and dev-mode role testing.

This PRD documents what the assembly actually does, not how HAI3's underlying primitives work. HAI3's layout slices and their internals are out of scope.

### 1.3 Goals (Business Outcomes)

**Capabilities**:

- Boot the app with a deterministic auth-then-identity sequence
- Render the same shell on desktop (two-column) and mobile (collapsible sheet) without per-screen branching
- Display a navigation menu that handles arbitrarily deep org hierarchies and routes clicks into Insight's payload-aware navigation
- Surface a development-only role switcher slot inside the menu footer

**Non-Goals (this revision)**:

- Per-tenant layout customization (single shell, customization happens via screensets and menu items)
- Right-to-left layout testing beyond what HAI3 ships
- Standalone Storybook for layout components

### 1.4 Glossary

| Term | Definition |
|------|------------|
| Layout | The application shell composing HAI3 layout primitives into the Insight app surface |
| Menu Item | A node in the navigation tree carrying `id`, `label`, optional `icon`, and optional `children`. The `id` encodes both a screen ID and an optional payload (`personId` or `teamId`) via `decodeMenuItemId` |
| Active Descendant | A menu item whose own children (transitively) include the currently active screen + payload combination; rendered with a softened active style to keep the user oriented |
| Payload | The runtime parameter that paired with a screen ID resolves a specific dataset (e.g., `personId` for IC Dashboard, `teamId` for Team View) |
| `requestSelection` | The Insight-side action that updates the relevant slice with the menu item's payload before navigation fires |
| RoleSwitcher slot | A development-only pluggable slot in the menu footer used for impersonating roles during local dev |
| Mobile breakpoint | Tailwind `md` (768px); below it the menu becomes a `Sheet`, above it a fixed sidebar |

## 2. Actors

### 2.1 Human Actors

#### App User

**ID**: `cpt-layout-actor-app-user`

**Role**: Any authenticated Insight user (executive, team lead, IC) interacting with the application shell. They navigate via the menu, expand and collapse menu groups, switch screens, and dismiss popups.
**Needs**: Predictable navigation that handles deep org trees without losing context; clear active-state cues; a mobile experience that doesn't fork from desktop.

### 2.2 System Actors

#### Auth Module

**ID**: `cpt-layout-actor-auth-module`

**Role**: Handles the OIDC ceremony invoked from `Layout`'s mount via `initAuth()`. Owns the `authStatus` slice consumed by `Layout` to gate the post-auth identity fetch.
**Needs**: Mount-time invocation; a clean handoff once the auth status flips to `'authenticated'`.

#### Identity Resolution

**ID**: `cpt-layout-actor-identity-resolution`

**Role**: Backend identity service queried via `fetchCurrentUser()` after auth succeeds. Returns the current user's identity, role, and (for managers) their subordinate tree, used by Insight to build the menu.
**Needs**: A request carrying the OIDC bearer token; a response shape consumed by `currentUserSlice`.

#### HAI3 Layout Slices

**ID**: `cpt-layout-actor-hai3-layout`

**Role**: External HAI3 slices owning menu state (`layout/menu`), header / sidebar / popup / overlay state. The Layout module reads from and emits events to these slices but does not own their schemas.
**Needs**: Stable slice keys (`layout/menu`, etc.) and event names (`layout/menu/collapsed`).

## 3. Operational Concept & Environment

### 3.1 Module-Specific Environment Constraints

The Layout module lives at `src/app/layout/` and mounts at the root of the React tree. It depends on the HAI3 store being initialized before its `useAppSelector` calls fire. Bootstrap order at first paint:

1. Layout mounts; `useEffect(initAuth)` fires unconditionally.
2. While `authStatus !== 'authenticated'`, the shell still renders with whatever menu state exists (typically a stub or empty), so login redirects do not flicker through a broken layout.
3. When `authStatus` transitions to `'authenticated'`, `fetchCurrentUser()` fires; subsequent Insight slices populate from the response.
4. The menu state slice (`layout/menu`) is populated by Insight's `currentUser` effects (out of scope for this PRD; see `currentUserEffects.ts`); the Menu component renders whatever items live there.

The mobile breakpoint is enforced via Tailwind's `md` utility (768px); the Layout listens to `window.resize` to dismiss the mobile sheet when the viewport grows past the breakpoint.

## 4. Scope

### 4.1 In Scope

- `Layout.tsx` — orchestrator: auth bootstrap, identity bootstrap, mobile/desktop split, slot composition
- `Menu.tsx` — recursive menu rendering, active-descendant detection, payload-aware navigation dispatch, collapsible groups, indent-by-depth Tailwind classes, `RoleSwitcher` slot
- `Header.tsx`, `Sidebar.tsx`, `Footer.tsx`, `Screen.tsx`, `Overlay.tsx`, `Popup.tsx` — thin scaffold-generated wrappers around HAI3 primitives; documented as part of the assembly but not customized in v1
- The mount-time `initAuth()` + `fetchCurrentUser()` sequence
- The `decodeMenuItemId` contract for splitting menu item IDs into `(screenId, param)`

### 4.2 Out of Scope

- HAI3 layout slice schemas, reducers, or selectors — owned by `@hai3/react`
- HAI3 UIKit primitives (`Sidebar`, `Sheet`, etc.) — owned by `@hai3/uikit`
- The menu items themselves — built by Insight `currentUserEffects.ts` from the loaded subordinate tree (covered by the Insight screensets, not this PRD)
- The Auth module's OIDC ceremony — owned by the Auth PRD
- The screensets rendered inside `<Screen>` — each owns its own routing-table entry and PRD

## 5. Functional Requirements

### 5.1 Application Bootstrap

#### Mount-Time Auth and Identity Sequence

- [ ] `p1` - **ID**: `cpt-layout-fr-bootstrap`

On mount, the Layout component **MUST** dispatch `initAuth()` exactly once. When the auth status slice transitions to `'authenticated'`, the Layout **MUST** dispatch `fetchCurrentUser()` exactly once for that transition. The Layout **MUST NOT** gate its own rendering on auth status — the shell renders unconditionally so login redirects and post-auth restores do not flash broken layouts.

**Rationale**: A single owner for the bootstrap sequence keeps it deterministic. Putting it on individual screens would race with route changes; putting it inside Auth would couple the auth module to identity-fetch concerns it should not know.

**Actors**: `cpt-layout-actor-app-user`, `cpt-layout-actor-auth-module`, `cpt-layout-actor-identity-resolution`

### 5.2 Desktop Layout Composition

#### Two-Column Desktop Composition

- [ ] `p1` - **ID**: `cpt-layout-fr-desktop-composition`

On viewports ≥ Tailwind `md` (768px), the Layout **MUST** render a fixed-width `Menu` on the left and a flex-grow column on the right containing `Screen` (active screen content) plus an optional `Sidebar`. `Footer` **MUST** render below both columns at full width. `Popup` and `Overlay` **MUST** mount at the root level so they can portal above the layout.

**Actors**: `cpt-layout-actor-app-user`

### 5.3 Mobile Layout Composition

#### Mobile Sheet Composition

- [ ] `p1` - **ID**: `cpt-layout-fr-mobile-composition`

On viewports below `md`, the Layout **MUST** render a top bar with a hamburger trigger plus the Insight logo. Tapping the trigger **MUST** open a `Sheet` containing the same `Menu` content as desktop. The sheet **MUST** auto-dismiss when a menu item is tapped (via the `onNavigate` callback) and **MUST** auto-dismiss when the viewport grows past the mobile breakpoint.

**Rationale**: Mobile and desktop share the same `Menu` component to avoid drift. The sheet wrapper is the only mobile-specific surface.

**Actors**: `cpt-layout-actor-app-user`

### 5.4 Menu Recursive Rendering

#### Recursive Menu Rendering with Indent-by-Depth

- [ ] `p1` - **ID**: `cpt-layout-fr-menu-recursive`

The Menu **MUST** render `MenuItem` nodes recursively to support arbitrarily deep subordinate trees. Indentation per depth **MUST** use static Tailwind classes from a depth-keyed map (`pl-8`, `pl-12`, `pl-16`, `pl-20`, `pl-24`); template-string class names like `pl-${n}` are not permitted because Tailwind's JIT compiler cannot pick them up.

Top-level groups **MUST** default to expanded; nested groups **MUST** default to collapsed. User toggle state stored in component-local `expandedGroups` overrides defaults.

**Rationale**: Real-tenant org trees reach depth 4-5; static class enumeration is the only reliable way to keep Tailwind aware of all the indent classes the Menu can produce. Defaulting nested groups to collapsed prevents the menu from exploding open when expanding a manager.

**Actors**: `cpt-layout-actor-app-user`

### 5.5 Active Item Highlighting

#### Active Item and Active Descendant Highlighting

- [ ] `p1` - **ID**: `cpt-layout-fr-active-highlighting`

The Menu **MUST** highlight the menu item matching the current `(screenId, param)` tuple as `isActive`. When a non-matching parent has a descendant (transitively, at any depth) that matches, the parent **MUST** render with a softened active style indicating an active descendant. `param` resolution reads from `insight/icDashboard.selectedPersonId` and `insight/teamView.selectedTeamId` — the Menu picks whichever non-null param matches the descendant's encoded payload.

**Rationale**: Without active-descendant highlighting, a user drilling into a deeply nested subordinate loses orientation when scrolling the menu. With it, every parent up the chain stays softly lit, and the tree remains navigable.

**Actors**: `cpt-layout-actor-app-user`

### 5.6 Menu Item Click Behavior

#### Payload-Aware Menu Click Dispatch

- [ ] `p1` - **ID**: `cpt-layout-fr-menu-click`

A click on a menu item **MUST** decode the item's id via `decodeMenuItemId(itemId)` into `(screenId, param)` and dispatch the Insight-side `requestSelection(screenId, param)` action. Then it **MUST** invoke `navigateToScreen(currentScreenset, screenId)`. If the item also has children, the click **MUST** additionally toggle the group's expanded state. If `onNavigate` was passed (mobile sheet), it **MUST** be invoked after navigation.

**Rationale**: HAI3's stock navigation knows about screens but not about Insight's payloads. Dispatching `requestSelection` first ensures the target screen mounts with the right `selectedPersonId` / `selectedTeamId` already in the slice; navigating second avoids the screen rendering with a stale param.

**Actors**: `cpt-layout-actor-app-user`

### 5.7 Role Switcher Slot

#### Role Switcher Pluggable Slot

- [ ] `p2` - **ID**: `cpt-layout-fr-role-switcher-slot`

The Layout **MUST** mount a `RoleSwitcher` slot in the menu footer for both desktop and mobile compositions. The slot is intended for development tenants and dev-mode role impersonation; in production tenants the underlying component **MAY** render nothing or be gated upstream.

**Rationale**: Without an in-shell role switcher, testing role-aware screens locally would require manual store mutation or environment overrides per page reload.

**Actors**: `cpt-layout-actor-app-user`

## 6. Non-Functional Requirements

### 6.1 NFR Inclusions

#### Bootstrap Determinism

- [ ] `p1` - **ID**: `cpt-layout-nfr-bootstrap-determinism`

The Layout **MUST** invoke `initAuth()` exactly once per mount. The post-auth `fetchCurrentUser()` **MUST** fire exactly once per transition into `'authenticated'`. Re-renders that do not change auth status **MUST NOT** trigger redundant identity fetches.

#### Tailwind-Only Styling

- [ ] `p1` - **ID**: `cpt-layout-nfr-tailwind-only`

The Layout module **MUST** use Tailwind utility classes for all styling. Indent classes **MUST** be statically enumerated (depth-keyed map) so Tailwind's JIT compiler can pick them up. Template-string class names are not permitted.

#### Mobile / Desktop Parity

- [ ] `p1` - **ID**: `cpt-layout-nfr-responsive-parity`

The same `Menu` content **MUST** render in both desktop and mobile compositions; mobile-specific behavior is restricted to the wrapping `Sheet` and the auto-dismiss callbacks. Forking the menu component to add mobile-specific features is not permitted.

#### Internationalization

- [ ] `p2` - **ID**: `cpt-layout-nfr-i18n`

Menu item labels **MUST** route through the `useTranslation` hook. The label string in `MenuItem.label` is treated as a translation key, not a literal display string.

### 6.2 NFR Exclusions

The following non-functional concerns are out of scope: server-side rendering of the shell, accessibility audit beyond what HAI3 sidebar/sheet primitives inherit from Radix UI, RTL layout testing, animated transitions beyond what underlying primitives provide.

## 7. Public Library Interfaces

### 7.1 Public API Surface

This module exposes the following named exports from `src/app/layout/index.ts`: `Layout`, `Header`, `Footer`, `Menu`, `Sidebar`, `Screen`, `Popup`, `Overlay`, plus their `*Props` types. The exports are consumed only by `App.tsx` and (for `Menu`) by mobile composition inside `Layout` itself; they are not published as an external package.

### 7.2 External Integration Contracts

#### HAI3 Layout Slice Contract

- [ ] `p1` - **ID**: `cpt-layout-contract-hai3-layout-slice`

The Menu depends on `state['layout/menu']` matching HAI3's `MenuState` shape (`{ collapsed, items: MenuItem[] }`). Toggling collapse **MUST** be done by emitting `eventBus.emit('layout/menu/collapsed', { collapsed })`; direct mutation is not permitted. A breaking rename of the slice key or event name upstream **MUST** be reconciled in this module; consuming screens stay insulated.

#### HAI3 Navigation Contract

- [ ] `p1` - **ID**: `cpt-layout-contract-hai3-navigation`

The Menu depends on `useNavigation()` from `@hai3/react` returning `{ currentScreen, navigateToScreen, currentScreenset }`. `navigateToScreen(screenset, screenId)` is the only navigation entry point used by Layout; deep-linking with a payload happens via `requestSelection` + `navigateToScreen` rather than a single combined call.

#### Insight Selection Action Contract

- [ ] `p1` - **ID**: `cpt-layout-contract-insight-selection`

The Menu depends on `requestSelection(screenId, param)` from `src/screensets/insight/actions/insightNavigationActions.ts`. The action updates the appropriate Insight slice (`icDashboard.selectedPersonId` or `teamView.selectedTeamId`) before the screen mounts. This module does not own the action's implementation.

#### Insight Slice Read Contract

- [ ] `p2` - **ID**: `cpt-layout-contract-insight-slice-read`

The Menu reads `selectedPersonId` from `state['insight/icDashboard']` and `selectedTeamId` from `state['insight/teamView']` to drive active-descendant highlighting. A schema change to either slice **MUST** be reconciled in `Menu.tsx`.

## 8. Use Cases

### UC-001 User Boots the App and Lands on Their Default Screen

**ID**: `cpt-layout-usecase-bootstrap-flow`

**Actors**: `cpt-layout-actor-app-user`, `cpt-layout-actor-auth-module`, `cpt-layout-actor-identity-resolution`

**Preconditions**: User has a valid OIDC session or is willing to complete the OIDC ceremony.

**Flow**:

1. App mounts; Layout's `useEffect(initAuth)` fires.
2. Auth module redirects to OIDC if needed; on return, `authStatus` flips to `'authenticated'`.
3. Layout's second `useEffect` observes the transition and fires `fetchCurrentUser()`.
4. Identity Resolution returns the current user; downstream effects populate the menu items.
5. Default screen mounts inside `<Screen>` based on screenset routing.

**Postconditions**: User sees their menu populated, their default screen rendered, and their identity available to downstream screens.

### UC-002 User Drills From Team Member Menu Item Into IC Dashboard

**ID**: `cpt-layout-usecase-menu-drill`

**Actors**: `cpt-layout-actor-app-user`

**Preconditions**: Menu populated; user is a team lead with subordinates.

**Flow**:

1. User clicks a menu item representing one of their direct reports. The item's id encodes `(IC_DASHBOARD_SCREEN_ID, personEmail)`.
2. Menu invokes `decodeMenuItemId` and dispatches `requestSelection(IC_DASHBOARD_SCREEN_ID, personEmail)`.
3. The Insight slice for IC Dashboard updates `selectedPersonId`.
4. Menu calls `navigateToScreen(currentScreenset, IC_DASHBOARD_SCREEN_ID)`.
5. IC Dashboard mounts with the correct `personId` already in the slice; `loadIcDashboard` fires.
6. Menu's active-item highlighting flips to the new item; the menu lineage above it lights as active descendant.

**Postconditions**: User is on the IC Dashboard for the chosen person; menu reflects the selection without any flicker.

## 9. Acceptance Criteria

- [ ] `initAuth()` is called exactly once per Layout mount; redundant re-renders do not re-trigger auth.
- [ ] `fetchCurrentUser()` is called exactly once per `'authenticated'` transition.
- [ ] Desktop composition renders Menu, Screen, optional Sidebar, and Footer in the documented arrangement; Popup and Overlay mount at the root.
- [ ] Mobile composition renders the top-bar trigger; tapping it opens a Sheet containing the same Menu; tapping a menu item dismisses the sheet.
- [ ] Menu indentation classes match the static depth-keyed Tailwind map; no template-string class names exist in `Menu.tsx`.
- [ ] Active item highlighting uses the `(screenId, param)` tuple resolved from the corresponding Insight slice.
- [ ] Active descendant highlighting works at depth ≥ 3.
- [ ] Menu item click dispatches `requestSelection` before `navigateToScreen`; the target screen mounts with the correct payload already in its slice.
- [ ] `RoleSwitcher` slot is present in both desktop and mobile menu footers.
- [ ] All visible labels route through `useTranslation`.

## 10. Dependencies

| Dependency | Description | Criticality |
|------------|-------------|-------------|
| `@hai3/react` | `useAppSelector`, `useNavigation`, `useTranslation`, `eventBus`, layout slice types | p1 |
| `@hai3/uikit` | `Sheet`, `Sidebar`, `SidebarContent`, `SidebarMenu`, `SidebarMenuItem`, `SidebarMenuButton`, `SidebarMenuIcon`, `SidebarHeader` | p1 |
| Auth module | `initAuth()` action and `selectAuthStatus` selector | p1 |
| `bootstrapActions.ts` | `fetchCurrentUser()` action | p1 |
| `currentUserEffects.ts` | Builds menu items from the loaded subordinate tree | p1 |
| `insightNavigationActions.ts` | `requestSelection` action consumed by the Menu | p1 |
| `decodeMenuItemId` utility | Splits menu item id into `(screenId, param)` | p1 |
| `RoleSwitcher` component | Dev-mode role switcher mounted in the menu footer slot | p2 |
| `@iconify/react` | Icon rendering inside menu items and the mobile top bar | p2 |
| Tailwind CSS | Sole styling mechanism | p1 |

## 11. Assumptions

- HAI3's `MenuState` shape and slice key (`layout/menu`) remain stable for the v1 lifecycle; breaking changes are reconciled here.
- Menu items emitted by `currentUserEffects.ts` always carry valid encoded ids that `decodeMenuItemId` can parse.
- The Auth module's `initAuth()` is idempotent at the slice level — calling it once per mount is correct even if the user is already authenticated.
- The mobile breakpoint stays aligned with Tailwind's `md` (768px); changing the breakpoint is a one-line update in `Layout.tsx` and the resize handler.

## 12. Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| HAI3 renames `layout/menu` slice key or the `MenuState` shape | Menu silently shows empty items, navigation appears broken | Pin the contract via `cpt-layout-contract-hai3-layout-slice`; reconcile in this module on upgrade rather than in every screen |
| `requestSelection` and `navigateToScreen` race | Screen mounts before the slice is updated; loads with stale payload | Ordering is mandated by `cpt-layout-fr-menu-click`; integration tests should cover the dispatch order when introduced |
| Tailwind JIT silently misses indent classes from template strings | Menu loses its hierarchy at certain depths | `cpt-layout-fr-menu-recursive` mandates static enumeration; reviewers reject template-string indent classes |
| New menu depth beyond depth 5 ships | Items render with the depth-5 indent fallback and look visually identical to depth 5 | Extend `INDENT_BY_DEPTH` map when a real tenant exceeds depth 5; keep the fallback so the menu never breaks |
| `RoleSwitcher` accidentally ships to production tenants | Production users see a developer-only control | Gate the `RoleSwitcher` component itself on environment, not the slot in the layout |

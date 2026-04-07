# Design: Add Initial Insight Screens

**Change:** `add-initial-insight-screens`
**Status:** Draft
**Date:** 2026-04-07

---

## Context

The `insight` screenset currently holds four scaffolding/demo screens (`documentation`, `dashboard`, `speed`, `uikit`). These are not product screens. The Virtuoso prototype defines the actual product: three role-specific engineering analytics dashboards. This change adds those three screens as production-quality HAI3 screens backed by mock data, and promotes the screenset category from `Drafts` to `Production`.

The existing screenset structure is monolithic by current standards: a single slice (`insightSlice.ts`), single events file (`insightEvents.ts`), and single effects file (`insightEffects.ts`). That pattern was acceptable for scaffolding. The new screens each have independent data concerns and must follow SCREENSETS.md's requirement for per-domain state files.

The prototype used CSV parsing via a `useCsv` hook and React Router's `useParams`/`useNavigate`. Neither applies in HAI3; all data comes through the event bus and API service, and URL params are read via a screen-level action on mount.

---

## Goals

- Add `executiveView`, `teamView`, and `icDashboard` screens to the `insight` screenset.
- Implement full UI per prototype visual spec (bullet charts, radar, bar, LOC stacked bar, delivery trends, drill modal, period selector).
- Serve all data from typed mock objects in `InsightApiService` — no CSV parsing, no network dependency.
- Follow HAI3 architectural rules strictly: per-domain slices/events/effects, no direct slice dispatch, no inline components in screen files.
- Register `recharts` as a project dependency (currently absent from `package.json`).

## Non-Goals

- Real backend integration (all data is mocked).
- Removing or modifying the four existing scaffolding screens.
- Multi-tenant / multi-org support.
- Period selector UI widget (period state is managed in slice; a selector component can be added later without architectural changes).

---

## Decisions

### 1. State management domains — per-screen, not shared

Each new screen gets its own domain: `executiveView`, `teamView`, `icDashboard`. This means three separate slice files, three separate events files, and three separate effects files.

**Rationale:** The three screens are fully independent data-wise. Executive View aggregates org-level team rows; Team View shows member rosters and alerts; IC Dashboard shows a single person's full metric profile. There is no state that needs to flow between them except the period selector (addressed separately below). Sharing a single domain would produce a monolithic slice that SCREENSETS.md explicitly forbids.

**File layout:**
```
slices/
  executiveViewSlice.ts
  teamViewSlice.ts
  icDashboardSlice.ts
events/
  executiveViewEvents.ts
  teamViewEvents.ts
  icDashboardEvents.ts
effects/
  executiveViewEffects.ts
  teamViewEffects.ts
  icDashboardEffects.ts
```

The existing `insightSlice.ts` / `insightEvents.ts` / `insightEffects.ts` serve the legacy scaffolding screens (`dashboard`, `speed`) and are left unchanged.

Each new slice is registered independently via `registerSlice(slice, initEffects)` in `insightScreenset.tsx`. The period slice (see Decision 4) is registered alongside them.

### 2. API service structure — extend existing `InsightApiService`

Add new methods to the single `InsightApiService` in `src/screensets/insight/api/insightApiService.ts`. Do not create separate per-screen API services.

**Rationale:** SCREENSETS.md requires that API services are screenset-local and not shared between screensets — it does not require per-screen services within a single screenset. Splitting into three services would produce three independent `BaseApiService` instances with overlapping base URLs and mock registrations, adding complexity without benefit. One service, one mock map, three method groups.

New mock data is added to `mocks.ts` as typed TypeScript constants (no CSV parsing, no `fetch` at runtime). The mock map gains entries for six new routes:

```
GET /api/insight/executive-view
GET /api/insight/team-view
GET /api/insight/team-view/:teamId/alerts
GET /api/insight/ic-dashboard/:personId
GET /api/insight/ic-dashboard/:personId/charts
GET /api/insight/ic-dashboard/:personId/drill/:drillId
```

All mock functions return inline TypeScript objects whose shapes are defined in `src/screensets/insight/types.ts`.

### 3. Shared composite components — `uikit/composite/`

`BulletChart`, `KpiStrip`, `MetricCard`, `CollapsibleSection`, `LocStackedBar`, `DeliveryTrends`, `DrillModal`, `PeriodSelectorBar`, `ViewModeToggle`, and `RoleSwitcher` are used across multiple screens and belong in `src/screensets/insight/uikit/composite/`.

**Rationale:** SCREENSETS.md specifies `uikit/composite/` for screenset-specific composites shared across screens. These components are purely presentational (props-in, render-out) — they receive typed data objects and callbacks, carry no state, and have no imports from `@hai3/state` or `@hai3/framework`. This is correct placement.

Components that are used only within a single screen belong in `screens/{screen}/components/`. The only candidate here is the person header strip in `icDashboard`, which is IC-Dashboard-only and screen-local.

`BulletChart` accepts a `BulletMetric` prop (the pre-computed API shape from the proposal) and an optional `onDrillClick` callback. It does not compute bar percentages — those arrive pre-calculated from the mock API (`bar_width_pct`, `median_left_pct`). The component renders only.

`recharts` components (`RadarChart`, `BarChart`, `LineChart`, `BarChart` stacked) are used directly inside `LocStackedBar`, `DeliveryTrends`, and the executive view chart composites. These composites live in `uikit/composite/` since they are screenset-specific wrappers around recharts with Insight design tokens baked in.

### 4. Period state — shared slice in `insight` domain

Period is a global concern within the screenset: all three screens display period-scoped data and must respond to period changes together. It is modeled as a dedicated Redux domain (`period`) with its own slice, events, and effects.

```
slices/periodSlice.ts          — { period: 'week' | 'month' | 'quarter' | 'year' | 'custom', customRange?: {...} }
events/periodEvents.ts         — PeriodChanged event
effects/periodEffects.ts       — subscribes to PeriodChanged, updates periodSlice
actions/periodActions.ts       — setPeriod(period) → emits PeriodChanged
```

All three screen data-loading actions (`loadExecutiveView`, `loadTeamView`, `loadIcDashboard`) read the current period from the store before calling the API service — but since actions cannot call `getState`, the period value is passed as an argument from the component. Components read period via a selector hook (`usePeriod` → `useSelector(selectPeriod)`).

A UI component for the period selector (`PeriodSelector`) belongs in `uikit/composite/` and calls `setPeriod` via the action. It can be wired into the screen layout header. For this change the selector renders the default period (`month`) with no toggle UI; the full toggle widget can be added in a follow-on.

**Why not a React context?** HAI3 forbids Zustand-style stores and custom subscribe/notify patterns. Redux via the event bus is the required pattern. React context would bypass the event bus and break time-travel debugging.

### 5. `recharts` dependency — must be added

`recharts` is not present in `package.json` (confirmed via grep — no match). The prototype's `ExecutiveView.tsx` imports directly from `recharts`. This dependency must be added before implementation:

```
npm install recharts
```

`recharts` is a peer-dep of React 18+. No version conflicts are expected in this stack.

### 6. DrillModal state — `icDashboard` domain slice only

`drillId: string | null` is screen-local IC Dashboard state. It belongs in `icDashboardSlice.ts`, not in a shared slice.

**Rationale:** No other screen opens the drill modal. The drill modal is only rendered inside `IcDashboardScreen`. Putting `drillId` in a shared slice would expose it to contexts that cannot act on it and would violate the "effects update only their own slice" rule if another domain tried to reset it.

The slice shape:
```ts
interface IcDashboardState {
  person: PersonData | null;
  kpis: KpiRow[];
  bulletMetrics: BulletMetric[];
  charts: IcChartsData | null;
  drillData: DrillData | null;     // loaded lazily on drill open
  drillId: string | null;
  loading: boolean;
  error: string | null;
}
```

`openDrill(drillId)` action emits `DrillOpened` event → effect sets `state.drillId` and triggers a `loadDrill` action. `closeDrill()` action emits `DrillClosed` → effect resets `drillId` and `drillData` to null.

### 7. IC Dashboard routing — Redux `selectedPersonId`, not URL params

`useRouteParams()` in HAI3 reads from `window.location.pathname` only. Since IC Dashboard is accessed via menu items (not URL path segments), URL-based param passing does not work. The resolved pattern uses Redux state.

**`selectedPersonId` in `icDashboardSlice`:**
```ts
interface IcDashboardState {
  selectedPersonId: string;   // default 'p1'; set before navigation
  // … other fields
}
```

**Navigation flow:**
1. Caller (TeamViewScreen or Menu effect) dispatches `setSelectedPersonId(personId)` via the `selectIcPerson(personId)` action, which emits `PersonSelected` → effect sets `state.selectedPersonId`.
2. Caller then calls `navigateToScreen(screensetId, IC_DASHBOARD_SCREEN_ID)`.
3. IC Dashboard screen reads `selectedPersonId` from Redux and loads data for that person.

**My Dashboard — `MY_DASHBOARD_SCREEN_ID`:**
For team leads and ICs who navigate to their own dashboard, a second screen ID `my-dashboard` is registered, pointing to the same `IcDashboardScreen` component. The screen detects `currentScreen === MY_DASHBOARD_SCREEN_ID` and overrides `personId` with `currentUser.personId`, bypassing `selectedPersonId` entirely.

```ts
// IcDashboardScreen.tsx
const { currentScreen } = useNavigation();
const currentUser = useAppSelector(selectCurrentUser);
const selectedPersonId = useAppSelector(selectSelectedPersonId);
const personId = currentScreen === MY_DASHBOARD_SCREEN_ID
  ? currentUser.personId
  : selectedPersonId;
```

**Menu items with `::` separator:**
IC Dashboard menu entries use the compound format `IC_DASHBOARD_SCREEN_ID + '::' + personId` (e.g. `'ic-dashboard::p2'`). `Menu.tsx` detects `::`, emits `layout/menu/itemParam` with `{ screenId, param }`, and navigates to `screenId`. The `currentUserEffects` handler receives the event and dispatches `setSelectedPersonId(param)` before navigation completes.

**Unknown personId handling:** If `selectedPersonId` does not match any person in the mock data, the screen renders a "Person not found" empty state when `loading` is false.

### 8a. Role system — `currentUser` domain

A fourth state domain was added to support role-based access: `currentUser`.

**Files:**
```
slices/currentUserSlice.ts        — { currentUser: CurrentUser }
events/currentUserEvents.ts       — UserChanged event
effects/currentUserEffects.ts     — builds and sets dynamic menu on UserChanged; handles layout/menu/itemParam
actions/currentUserActions.ts     — MOCK_USERS array, switchUser(user) action
uikit/composite/RoleSwitcher.tsx  — role-switch UI in sidebar bottom
```

**`CurrentUser` type:**
```ts
export type UserRole = 'executive' | 'team_lead' | 'ic';
export interface CurrentUser {
  personId: string;
  name: string;
  role: UserRole;
  teamId: string;
}
```

**Dynamic menu via `setMenuItems`:**
On every `UserChanged` event, `currentUserEffects` calls `buildMenu(role, user)` and dispatches `setMenuItems(...)` from `@hai3/react`. Menu structure per role:
- `executive`: Executive View item + all `ORG` teams (recursive sub-items).
- `team_lead`: My Dashboard + Team View + Team Members sub-menu (all member IDs from their team).
- `ic`: My Dashboard only.

**Org hierarchy — `OrgTeam` with recursive `subTeams`:**
```ts
interface OrgTeam {
  teamId: string;
  label: string;
  leadId: string;
  memberIds: string[];
  subTeams?: OrgTeam[];
}
export const ORG: OrgTeam[] = [ /* defined in currentUserEffects.ts */ ];
```
`teamToMenuItem(team: OrgTeam)` is recursive: sub-teams produce nested `children` arrays at arbitrary depth with no changes to `Menu.tsx`.

**HAI3 header removed:** The app-shell header component was removed from `Layout.tsx`. Each screen merges the person/team name with period controls into a single header row within the screen itself.

### 8b. Static screenset menu vs dynamic role menu

`insightScreenset.tsx` declares a static `menu[]` array (required by the framework to register screen loaders). On app bootstrap, `fetchCurrentUser()` fires `UserChanged`, which causes `currentUserEffects` to call `setMenuItems(buildMenu(role, user))` — this **replaces** the static menu entirely with the role-specific set.

The static entries for `ic-dashboard` and `my-dashboard` exist only to register the screen loaders with the router. They are never visible as top-level menu items in practice; `buildMenu` generates the actual menu structure.

**`layout/menu/itemParam` event** is declared in `currentUserEvents.ts` EventPayloadMap (not in `icDashboardEvents.ts`), because it is a layout-level protocol used by `currentUserEffects`.

### 8c. Drill state — TeamView uses local state, IC Dashboard uses Redux

The two screens handle drill modals differently:

- **IC Dashboard**: `drillId` and `drillData` live in `icDashboardSlice`. `openDrill(personId, drillId)` action calls the API and emits `DrillOpened` → effect sets slice state. This allows drill state to survive screen re-renders and can be observed by other effects.

- **Team View**: `drillData` and `drillOpen` are `useState` in `TeamViewScreen`. TeamView supports two drill entry points: `handleCellDrill(personId, drillId)` (per-cell metric drill into a team member's data) and `handleMembersDrill()` (overview drill of all team members). Neither requires cross-effect coordination, so local state is sufficient.

### 8. Mock data approach — inlined TypeScript constants, no CSV parsing

All mock data is authored as TypeScript constants in `mocks.ts`, typed against the interfaces in `types.ts`. The prototype's CSV files serve as the data source but are transcribed into TypeScript objects — no runtime CSV parsing.

**Rationale:** CSV parsing at runtime adds a `fetch` dependency, error handling surface, and parse/type-cast complexity. The mock data is static and well-known. Inlining as TypeScript constants gives type safety, zero runtime cost, and trivial IDE navigation.

The mock data covers:
- 5 teams (Executive View)
- 8 team members across two teams, two periods (`week`, `month`) — enough for TeamView and IC Dashboard
- Full bullet metric set for one person × two periods (as per proposal metric tables)
- LOC trend and delivery trend series for one person × two periods
- All 8 drill datasets

---

## Risks and Trade-offs

| Risk | Likelihood | Mitigation |
|---|---|---|
| `recharts` not in `package.json` | Confirmed absent | Add via `npm install recharts` before any other work. Block implementation until done. |
| Period state uninitialized when screen mounts | Medium — slice initializes to `'month'` by default, but effects must not assume prior period event was fired | Initialize `periodSlice` with `period: 'month'` as `initialState`. All load actions work with the current slice state, not by waiting for a `PeriodChanged` event. |
| IC Dashboard: unknown `personId` | Low (mock covers known IDs) | Defensive null check in screen; render empty state. Mock returns `null` for unknown IDs. |
| BulletChart re-render thrashing (20+ instances on IC Dashboard) | Low — components are pure presentational | Wrap `BulletChart` in `React.memo`. Since all props are primitives or shallow objects from the API response, memo equality check is fast. |
| Monolithic `mocks.ts` grows large | Low for this change | Accept for now. If mock data grows beyond ~600 lines, split into `mocks/executiveView.ts` etc. and re-export from `mocks.ts`. |
| Existing `insightSlice` / events / effects are not per-domain | Pre-existing tech debt | Leave as-is. They serve the legacy scaffolding screens. Not in scope to refactor here. |

---

## Migration Plan

1. **`npm install recharts`** — update `package.json` and `package-lock.json`. Commit separately.
2. **Types** — add new interfaces to `src/screensets/insight/types.ts`: `ExecViewData`, `TeamViewData`, `IcDashboardData`, `BulletMetric`, `KpiRow`, `DrillData`, `PeriodValue`, etc.
3. **Period domain** — add `periodSlice.ts`, `periodEvents.ts`, `periodEffects.ts`, `periodActions.ts`.
4. **API service** — extend `InsightApiService` with six new methods; add mock constants to `mocks.ts`.
5. **Shared uikit composites** — implement `BulletChart`, `KpiStrip`, `MetricCard`, `CollapsibleSection`, `LocStackedBar`, `DeliveryTrends`, `DrillModal`, `PeriodSelectorBar`, `ViewModeToggle`, `RoleSwitcher` in `uikit/composite/`.
6. **Executive View domain + screen** — slice, events, effects, actions, screen components, i18n.
7. **Team View domain + screen** — same pattern; navigation action to IC Dashboard passes `personId`.
8. **IC Dashboard domain + screen** — slice (including `drillId`), events, effects, actions (including drill open/close), screen components.
9. **IDs and screenset registration** — add three screen ID constants to `ids.ts`; register all three screens plus period slice in `insightScreenset.tsx`; change category to `Production`.
10. **i18n** — add menu label keys for all three screens to all 36 language files (English labels first; others can be empty strings or carry English as placeholder).

Steps 3–5 can run in parallel. Steps 6–8 are independent of each other but depend on 3–5. Step 9 depends on all prior steps.

---

## Open Questions (Resolved)

1. **HAI3 URL param access pattern** — **RESOLVED.** `useRouteParams()` reads only from `window.location.pathname`. Since IC Dashboard is reached via menu items (not URL segments), URL params are not usable. Solution: `selectedPersonId` in Redux, set by the caller before `navigateToScreen`. See Decision 7.

2. **Period selector UI** — **RESOLVED.** Period selector renders inside each screen's header row (not in the app shell). Each screen includes a `PeriodSelectorBar` composite component with the period tabs and optional custom range picker. Period state lives in `insight/period` screenset slice.

3. **Navigation from Team View to IC Dashboard** — **RESOLVED.** `navigateToScreen(currentScreenset, IC_DASHBOARD_SCREEN_ID)` after dispatching `setSelectedPersonId(personId)`. No dynamic URL segment needed — the target `personId` is passed via Redux state. See Decision 7.

4. **`viewMode` (chart / tile) on IC Dashboard** — `useState` at the screen level. No event bus involvement needed for pure UI presentation state.

5. **Stale scaffolding screens** — Kept as-is. Out of scope for this change.

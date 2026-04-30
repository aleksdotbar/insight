# PRD — Team View


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
  - [5.1 Team Hero Strip](#51-team-hero-strip)
  - [5.2 Attention Needed Panel](#52-attention-needed-panel)
  - [5.3 Members Table](#53-members-table)
  - [5.4 Bullet Sections](#54-bullet-sections)
  - [5.5 Direct Reports Filter](#55-direct-reports-filter)
  - [5.6 Period Selection](#56-period-selection)
  - [5.7 Drill Modal](#57-drill-modal)
  - [5.8 Navigation to IC Dashboard](#58-navigation-to-ic-dashboard)
- [6. Non-Functional Requirements](#6-non-functional-requirements)
  - [6.1 NFR Inclusions](#61-nfr-inclusions)
  - [6.2 NFR Exclusions](#62-nfr-exclusions)
- [7. Public Library Interfaces](#7-public-library-interfaces)
  - [7.1 Public API Surface](#71-public-api-surface)
  - [7.2 External Integration Contracts](#72-external-integration-contracts)
- [8. Use Cases](#8-use-cases)
  - [UC-001 Team Lead Reviews Their Direct Reports](#uc-001-team-lead-reviews-their-direct-reports)
  - [UC-002 Executive Drills Into a Subordinate Team](#uc-002-executive-drills-into-a-subordinate-team)
  - [UC-003 Team Lead Drills Into a Member's Metric Cell](#uc-003-team-lead-drills-into-a-members-metric-cell)
- [9. Acceptance Criteria](#9-acceptance-criteria)
- [10. Dependencies](#10-dependencies)
- [11. Assumptions](#11-assumptions)
- [12. Risks](#12-risks)

<!-- /toc -->

## 1. Overview

### 1.1 Purpose

Team View is the role-aware screen that lets a team lead (or an executive drilling down) inspect a single team's engineering health, member-by-member. It surfaces who needs attention, what bullet metrics look like across task delivery / code quality / estimation / AI adoption / collaboration sections, and lets the viewer drill into any cell or member for details.

### 1.2 Background / Problem Statement

Team leads and VPs need a per-team picture that goes beyond the executive aggregate. Without it, the only way to find which member is dragging a team's `focus_time_pct` down was to query OData directly. Team View binds member rows + section-level aggregates + alert thresholds into one screen, scoped by team, with a toggle to narrow to direct reports.

**Target Users**:

- Team leads consuming the screen for their own team
- Executives drilling from Executive View into a subordinate's team

**Key Problems Solved**:

- No surface to see all team members and their per-person metrics in one place
- No alert mechanism for members tripping `alert_thresholds`
- No drill from team-level to member-level without leaving the dashboard

### 1.3 Goals (Business Outcomes)

**Capabilities**:

- Render team-level KPI chips (`atRisk`, `belowFocus`, `noAi`, median dev time) computed client-side from the visible member set
- List every team member with per-person metric columns and threshold-based coloring
- Show a configurable "Attention Needed" panel listing members who tripped any `alert_threshold`
- Render bullet sections (task delivery / code quality / estimation / AI adoption / collaboration) with their own thresholds
- Let the viewer drill from any team-level chart, members-table cell, or member row into a detail modal or the IC Dashboard

**Non-Goals (this revision)**:

- Editing team membership, supervisor relationships, or thresholds (owned by Identity Resolution and Metric Catalog)
- Per-team dashboard composition (covered by Dashboard Configurator post-MVP)
- Cross-team comparison (lives in Executive View)

### 1.4 Glossary

| Term | Definition |
|------|------------|
| Team View | The per-team dashboard rendered for team leads or for executives drilling from the Executive View |
| Team Lead | A user whose `currentUser.role === 'team_lead'`; the supervisor anchor for "direct reports only" |
| Direct Reports Only | A viewer toggle that narrows the member list to people whose `supervisor_email` matches the team lead's email |
| Hero Strip | The top KPI chip strip (`atRisk`, `belowFocus`, `noAi`, `devTimeMedian`), recomputed client-side over the currently visible member set via `deriveTeamKpis` |
| Bullet Section | A grouped set of bullet charts for a metric area (`task_delivery`, `code_quality`, `estimation`, `ai_adoption`, `collaboration`) |
| Alert Threshold | A `trigger` value per metric used to flag at-risk members; sourced from `TEAM_VIEW_CONFIG.alert_thresholds` |
| Drill | A focused detail popup (`DrillModal`) opened from a team-level or member-level interaction |
| Honest null | A metric value rendered as em-dash / `ComingSoon` rather than `0` when the underlying connector is not configured |

## 2. Actors

### 2.1 Human Actors

#### Team Lead

**ID**: `cpt-team-view-actor-team-lead`

**Role**: A supervisor of one or more direct reports; consumes Team View for their own team. Never sees their own row in the members table — their personal metrics live in the IC Dashboard. The "Direct reports only" toggle is anchored on their email.
**Needs**: A clear picture of who on their team needs attention; the ability to drill into specific members and metrics; honest signals when a metric source is missing.

#### Executive Drilling Down

**ID**: `cpt-team-view-actor-executive-drill`

**Role**: A VP or department head who navigated from Executive View by clicking on a team. Their `teamId` is an email of the subordinate whose department they're inspecting; the screen scopes the table accordingly.
**Needs**: To see a subordinate's team without switching personas.

### 2.2 System Actors

#### Analytics API

**ID**: `cpt-team-view-actor-analytics-api`

**Role**: Backend exposing per-metric OData queries — team members, bullet aggregates, and drill details.
**Needs**: Filtered queries scoped by `team_id` and date range.

#### Connector Manager

**ID**: `cpt-team-view-actor-connector-manager`

**Role**: Backend endpoint reporting per-source availability, consumed identically to Executive View.

## 3. Operational Concept & Environment

### 3.1 Module-Specific Environment Constraints

The screen runs in the browser under the Insight screenset. It depends on `useNavigation` from `@hai3/react` to navigate to IC Dashboard; on a Redux slice for team state; and on a peer slice (`currentUserSlice`) for the viewer's role and identity.

`teamId` is derived from upstream selection (`selectedTeamId` slice) and may be either an email (executive drilled from Executive View into a subordinate) or an `org_unit_name` string (team lead viewing their own department). The "Direct reports only" toggle's anchor email is resolved from `teamId` when it's an email, otherwise from the current user's identity for a team lead, otherwise the toggle is hidden.

Period changes re-fire `loadTeamView`. Navigation to IC Dashboard sets the IC person and pushes through `useNavigation`.

## 4. Scope

### 4.1 In Scope

- `TeamHeroStrip` — top KPI chips computed client-side over visible members
- `AttentionNeeded` — list of members tripping any `alert_threshold`
- `MembersTable` — sortable per-member table with threshold-driven cell coloring and click-through
- `TeamBulletSections` — bullet charts grouped by metric area (task delivery, code quality, estimation, AI adoption, collaboration)
- `PeriodSelectorBar` + `ViewModeToggle` — viewer controls
- `DrillModal` — detail popup opened from team-level or member-level interactions
- "Direct reports only" toggle — anchored on team-lead email
- Navigation to IC Dashboard from any member-row / member-cell click
- Honest-null rendering across hero, table, and bullets

### 4.2 Out of Scope

- Mutating team metadata, supervisor relationships, or member assignments
- Defining alert thresholds (sourced from `TEAM_VIEW_CONFIG`, ownership migrating to Metric Catalog)
- Cross-team comparison or rollup (lives in Executive View)
- Custom column composition per viewer (Dashboard Configurator post-MVP)
- CSV / PDF export
- Real-time updates — the screen is request-response per period change or team selection

## 5. Functional Requirements

### 5.1 Team Hero Strip

#### Hero KPI Chip Computation

- [ ] `p1` - **ID**: `cpt-team-view-fr-hero-strip`

The screen **MUST** render `TeamHeroStrip` populated from `deriveTeamKpis(members, period)` over the currently visible member set. KPIs **MUST** include `atRisk`, `belowFocus`, `noAi`, and `devTimeMedian`. When `members.length === 0` and the load is not yet complete, the strip **MAY** fall back to the Redux-stored KPIs to avoid a flicker; otherwise empty members **MUST** produce empty KPIs that render as `ComingSoon`, not `0`.

**Rationale**: Hero KPIs are scoped to whatever the viewer is actually looking at. If the "Direct reports only" toggle narrows the table to 5 people, the hero strip must reflect those 5, not the full team.

**Actors**: `cpt-team-view-actor-team-lead`, `cpt-team-view-actor-executive-drill`

### 5.2 Attention Needed Panel

#### Alert Triggering

- [ ] `p1` - **ID**: `cpt-team-view-fr-attention-needed`

The screen **MUST** render `AttentionNeeded` listing members who tripped at least one `alert_threshold` from `TEAM_VIEW_CONFIG.alert_thresholds`. A null metric value **MUST NOT** trigger the alert; only finite numbers below the trigger qualify. Each entry **MUST** be clickable to navigate to the IC Dashboard for that person.

**Rationale**: A member without a focus-time source has `focus_time_pct === null`, and `null < trigger` would coerce to `true` in JavaScript, falsely listing them as at-risk. The contract here is explicit: missing data ≠ red flag.

**Actors**: `cpt-team-view-actor-team-lead`

### 5.3 Members Table

#### Members Table Rendering

- [ ] `p1` - **ID**: `cpt-team-view-fr-members-table`

The screen **MUST** render `MembersTable` with one row per visible member. Threshold-driven cell coloring **MUST** use `teamViewConfig.column_thresholds`. Null cells **MUST** render as em-dash / `ComingSoon`, not `0`. Clicking a row **MUST** navigate to the IC Dashboard for that person; clicking a cell **MUST** open a per-cell drill via `openTeamDrill({ kind: 'cell', personId, drillId })`.

**Actors**: `cpt-team-view-actor-team-lead`, `cpt-team-view-actor-executive-drill`

### 5.4 Bullet Sections

#### Section-Level Bullet Charts

- [ ] `p1` - **ID**: `cpt-team-view-fr-bullet-sections`

The screen **MUST** render `TeamBulletSections` for the configured sections (`task_delivery`, `code_quality`, `estimation`, `ai_adoption`, `collaboration`). Each section **MUST** show its bullets with their own thresholds; a section with all-null bullets **MUST** render as `ComingSoon` rather than disappearing.

Section names **MUST** match the canonical taxonomy in `BULLET_DEFS` (`thresholdConfig.ts`). `git_output` is rendered only on IC Dashboard, not Team View.

**Actors**: `cpt-team-view-actor-team-lead`

### 5.5 Direct Reports Filter

#### Direct Reports Toggle

- [ ] `p1` - **ID**: `cpt-team-view-fr-direct-reports-toggle`

The screen **MUST** render a "Direct reports only" toggle, defaulting to `true`, when an anchor email (`teamOwnerEmail`) is resolvable. The anchor is the lowercased `teamId` when it's an email; otherwise the team lead's own email; otherwise the toggle is hidden. When the toggle is on, the visible member set **MUST** be filtered to people whose `supervisor_email` matches the anchor (case-insensitive).

The toggle's label **MUST** include a counter showing `(filtered/total)` so the viewer understands the scope.

**Known limitation**: in v1 the toggle scopes only the members table and the client-derived hero strip; bullet sections still come from server-side per-team aggregates and are not narrowed by the toggle. A separate FR will close that gap once a backend filter is available — see Risks.

**Actors**: `cpt-team-view-actor-team-lead`

### 5.6 Period Selection

#### Period Selector Integration

- [ ] `p1` - **ID**: `cpt-team-view-fr-period-selection`

The screen **MUST** render `PeriodSelectorBar`. Period changes **MUST** trigger a fresh `loadTeamView(teamId, period, resolveDateRange(period, customRange))`. Custom ranges **MUST** persist and be supplied to OData `$filter` via `odataDateFilter(range)`.

**Actors**: `cpt-team-view-actor-team-lead`

### 5.7 Drill Modal

#### Drill Modal Integration

- [ ] `p1` - **ID**: `cpt-team-view-fr-drill-modal`

The screen **MUST** open a `DrillModal` when `openTeamDrill` is dispatched, with two kinds: `'team'` (a section-level drill scoped by `teamId` + `drillId`) and `'cell'` (a per-person drill scoped by `personId` + `drillId`). The modal **MUST** close cleanly via `closeTeamDrill` and **MUST NOT** persist drill state across team changes or period changes.

**Actors**: `cpt-team-view-actor-team-lead`

### 5.8 Navigation to IC Dashboard

#### Cross-Screen Navigation

- [ ] `p1` - **ID**: `cpt-team-view-fr-navigate-to-ic`

The screen **MUST** support navigation to the IC Dashboard for any visible member by setting `selectIcPerson(personId)` followed by `navigateToScreen(INSIGHT_SCREENSET_ID, IC_DASHBOARD_SCREEN_ID)`. The navigation **MUST** preserve the viewer's currently selected period.

**Actors**: `cpt-team-view-actor-team-lead`, `cpt-team-view-actor-executive-drill`

## 6. Non-Functional Requirements

### 6.1 NFR Inclusions

#### First Meaningful Paint

- [ ] `p1` - **ID**: `cpt-team-view-nfr-first-paint`

First meaningful paint of the screen for a typical team (≤30 members) **SHOULD** complete within 2 seconds on stable network from `loadTeamView` invocation to `TeamViewLoaded` event emission, excluding initial bundle download.

#### Honest-Null Rendering

- [ ] `p1` - **ID**: `cpt-team-view-nfr-honest-null`

No metric value **MUST** silently coerce a null source row into `0` for display, KPI computation, or alert evaluation. `deriveTeamKpis` and `AttentionNeeded` **MUST** filter null values before any inequality check.

#### Internationalization

- [ ] `p2` - **ID**: `cpt-team-view-nfr-i18n`

All visible strings **MUST** route through the `useTranslation` hook with translation keys defined in `i18n/<lang>.json` for every supported language registered on `INSIGHT_SCREENSET_ID` / `TEAM_VIEW_SCREEN_ID`.

#### Large-Team Rendering

- [ ] `p2` - **ID**: `cpt-team-view-nfr-large-team`

The screen **SHOULD** render correctly for teams of up to 200 members without breaking layout. Past that, performance is best-effort until pagination or virtualization lands.

### 6.2 NFR Exclusions

The following non-functional concerns are out of scope: real-time / streaming updates, offline operation, server-side rendering, accessibility audit beyond what UIKit components inherit, observability instrumentation (owned at the screenset boundary).

## 7. Public Library Interfaces

### 7.1 Public API Surface

This screen does not expose a public library interface. It is consumed only by the Insight screenset routing table at `/team-view`.

### 7.2 External Integration Contracts

#### Team Members Contract

- [ ] `p1` - **ID**: `cpt-team-view-contract-team-members`

The screen depends on the `TEAM_MEMBERS` metric query (`METRIC_REGISTRY.TEAM_MEMBERS`) returning `RawTeamMemberRow[]` with at least `person_id`, `person_name`, `email`, `supervisor_email`, plus per-metric numerics (`focus_time_pct`, `ai_loc_share_pct`, `dev_time_h`, etc.). A breaking contract change (renamed columns, removed UUID) **MUST** be coordinated with `transformTeamMembers`.

#### Bullet Aggregates Contract

- [ ] `p1` - **ID**: `cpt-team-view-contract-bullets`

The screen depends on per-section bullet aggregate queries returning `RawBulletAggregateRow[]`. Sections covered: `task_delivery`, `code_quality`, `estimation`, `ai_adoption`, `collaboration`. Adding a section is additive; removing one is breaking and **MUST** be coordinated with `TeamBulletSections`.

#### Drill Detail Contract

- [ ] `p2` - **ID**: `cpt-team-view-contract-drill`

The screen depends on a drill query returning `RawDrillRow[]` keyed by `drillId`. The shape is consumed via `transformDrill`.

## 8. Use Cases

### UC-001 Team Lead Reviews Their Direct Reports

**ID**: `cpt-team-view-usecase-team-lead-review`

**Actors**: `cpt-team-view-actor-team-lead`, `cpt-team-view-actor-analytics-api`

**Preconditions**: User is authenticated with `role === 'team_lead'`; their identity has at least one direct report.

**Flow**:

1. User navigates to `/team-view`. `teamId` resolves to the team lead's own department string.
2. `loadTeamView(teamId, period, dateRange)` fires; member list and bullets load.
3. Default toggle "Direct reports only" is on — table narrows to direct reports.
4. Hero strip shows KPIs computed over the visible member set.
5. `AttentionNeeded` panel surfaces members below alert thresholds.

**Postconditions**: Team lead has a focused picture of their direct reports for the chosen period.

### UC-002 Executive Drills Into a Subordinate Team

**ID**: `cpt-team-view-usecase-exec-drill`

**Actors**: `cpt-team-view-actor-executive-drill`, `cpt-team-view-actor-analytics-api`

**Preconditions**: Executive on Executive View clicked on a team owned by a subordinate.

**Flow**:

1. Selection sets `teamId` to the subordinate's email.
2. Screen mounts; `teamOwnerEmail` resolves from the email-shaped `teamId`.
3. "Direct reports only" toggle defaults on; table shows the subordinate's direct reports.
4. Header subtitle reads `Direct reports of <subordinate name>` (the subordinate's name comes from the loaded team response).
5. Executive can toggle the filter off to see the subordinate's full department.

**Postconditions**: Executive inspected a subordinate team without leaving the dashboard.

### UC-003 Team Lead Drills Into a Member's Metric Cell

**ID**: `cpt-team-view-usecase-cell-drill`

**Actors**: `cpt-team-view-actor-team-lead`

**Preconditions**: Team View loaded with at least one member.

**Flow**:

1. User clicks a metric cell in `MembersTable` for a specific person.
2. `openTeamDrill({ kind: 'cell', personId, drillId })` dispatches.
3. `DrillModal` opens with the per-person, per-metric detail.
4. User closes via `closeTeamDrill`; modal state resets.

**Postconditions**: Drill state is cleared; underlying screen state is unchanged.

## 9. Acceptance Criteria

- [ ] Hero strip KPIs are computed over the currently visible member set, not the unfiltered total.
- [ ] `AttentionNeeded` does not flag members whose triggering metric is null.
- [ ] Members table renders honest-null cells (em-dash / `ComingSoon`) rather than `0`.
- [ ] "Direct reports only" toggle is hidden when no anchor email is resolvable.
- [ ] Counter on the toggle shows `(filtered/total)` accurately.
- [ ] Period change re-fires `loadTeamView` with the resolved range; previous data stays visible until the new load completes.
- [ ] Clicking a member row navigates to IC Dashboard for that person; clicking a cell opens a cell-drill modal.
- [ ] Drill modal closes cleanly and does not leak state across team or period changes.
- [ ] Every visible string routes through `useTranslation`; no hardcoded English literals in the rendered DOM.

## 10. Dependencies

| Dependency | Description | Criticality |
|------------|-------------|-------------|
| Analytics API team-member + bullet queries | `METRIC_REGISTRY.TEAM_MEMBERS` and per-section bullet UUIDs | p1 |
| Connector Manager Service | Per-source availability, consumed identically to Executive View | p2 |
| `@hai3/react` | `eventBus`, `apiRegistry`, `useAppSelector`, `useNavigation`, `useTranslation`, `I18nRegistry` | p1 |
| Period slice + `usePeriod` | Source of truth for current period and custom range | p1 |
| `currentUserSlice` | Provides `currentUser.role` and identity used to resolve the direct-reports anchor | p1 |
| `insightUiSlice` | Provides `viewMode` for the bullets toggle | p2 |
| `teamHealthStatus` (`metricSemantics.ts`) | Maps counts to status colors based on team size | p1 |
| `transformTeamMembers`, `transformBulletMetrics`, `transformDrill` | OData → domain shape | p1 |
| UIKit composite components | `TeamHeroStrip`, `AttentionNeeded`, `MembersTable`, `TeamBulletSections`, `DrillModal`, `PeriodSelectorBar`, `ViewModeToggle` | p1 |
| IC Dashboard screen | Navigation target via `selectIcPerson` + `navigateToScreen` | p1 |

## 11. Assumptions

- Authentication is enforced upstream (Auth module / `AuthGuard`).
- `currentUserSlice` is hydrated before Team View mounts; `role` and `email` are available.
- `teamId` is set by upstream selection (Executive View drill or default for team leads); the screen does not invent one.
- `supervisor_email` on `RawTeamMemberRow` is reliably populated by Identity Resolution; missing values silently exclude the member from the direct-reports filter rather than panicking.
- Threshold definitions in `TEAM_VIEW_CONFIG` are stable across viewers; per-viewer customization is out of scope.

## 12. Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| "Direct reports only" toggle scopes only the table and client-derived hero strip; bullet sections remain whole-team aggregates | Viewer toggling on the filter sees inconsistent view: 5 people in the table but bullet `74/112` for the whole org | Tracked separately as Todoist task `6gR9R4jgVRpW7Wv6`. Short-term: rename toggle to "Filter table by direct reports" with tooltip; long-term: client-side recomputation for member-scale bullets, or backend per-person filter |
| Period change while a load is in flight | Older response overwrites newer state | Introduce `AbortController` per load (tracked in P0 audit task `6gQxvQg4Hf4P8HJ6`) |
| `null < trigger` evaluating truthy in JS without filtering | Phantom at-risk members in `AttentionNeeded` and inflated `atRisk` count in hero | Already addressed in `deriveTeamKpis`; covered by `cpt-team-view-fr-attention-needed` and `cpt-team-view-nfr-honest-null` — keep test coverage when tests are introduced |
| `teamId` shape ambiguity (email vs. org_unit_name string) | Drill from Executive View might mislabel header or fail to resolve direct-reports anchor | Header subtitle and anchor resolution explicitly branch on `teamId.includes('@')`; canonical team-id model is part of the long-running data-model task `6gQxwMPQwJvj2Gq6` |
| Threshold definitions diverge between FE `TEAM_VIEW_CONFIG` and backend Metric Catalog post-migration | Cell coloring drifts from official thresholds | Treat `TEAM_VIEW_CONFIG` as v1-only; once Metric Catalog ships, source coloring from `GET /catalog/metrics` |
| Team grows past 200 members | Single-shot OData load and unvirtualized table degrade | Promote `cpt-team-view-nfr-large-team` to `p1` and add pagination / virtualization once a real tenant trips the threshold |

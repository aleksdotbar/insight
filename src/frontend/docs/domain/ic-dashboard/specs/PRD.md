# PRD — IC Dashboard


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
  - [5.1 Person Header](#51-person-header)
  - [5.2 KPI Strip](#52-kpi-strip)
  - [5.3 Time-Off Banner](#53-time-off-banner)
  - [5.4 Bullet Sections](#54-bullet-sections)
  - [5.5 Trend Charts](#55-trend-charts)
  - [5.6 Drill Modal](#56-drill-modal)
  - [5.7 Period Selection & View Mode](#57-period-selection--view-mode)
  - [5.8 Per-Section Error Recovery](#58-per-section-error-recovery)
  - [5.9 Person Not Found State](#59-person-not-found-state)
  - [5.10 Privacy Footer](#510-privacy-footer)
- [6. Non-Functional Requirements](#6-non-functional-requirements)
  - [6.1 NFR Inclusions](#61-nfr-inclusions)
  - [6.2 NFR Exclusions](#62-nfr-exclusions)
- [7. Public Library Interfaces](#7-public-library-interfaces)
  - [7.1 Public API Surface](#71-public-api-surface)
  - [7.2 External Integration Contracts](#72-external-integration-contracts)
- [8. Use Cases](#8-use-cases)
  - [UC-001 IC Reviews Their Own Dashboard](#uc-001-ic-reviews-their-own-dashboard)
  - [UC-002 Team Lead Drills Into a Member's IC Dashboard](#uc-002-team-lead-drills-into-a-members-ic-dashboard)
  - [UC-003 IC Drills Into a Bullet Metric for Detail](#uc-003-ic-drills-into-a-bullet-metric-for-detail)
- [9. Acceptance Criteria](#9-acceptance-criteria)
- [10. Dependencies](#10-dependencies)
- [11. Assumptions](#11-assumptions)
- [12. Risks](#12-risks)

<!-- /toc -->

## 1. Overview

### 1.1 Purpose

IC Dashboard is the per-person view that shows engineering metrics for a single individual contributor — task delivery, git output, code quality, AI tool usage, collaboration, and trend charts — with drill-down on every bullet metric. It is the same view a person sees for themself ("My Dashboard") and a team lead or executive sees when inspecting a specific member.

### 1.2 Background / Problem Statement

Team-level metrics aggregate signal but hide the per-person picture. To answer "is this engineer overloaded / under-supported / ramping up?", a viewer needs the same metrics scoped to one person, with a time-off banner so absent days do not look like underperformance, with section-level error recovery so a single broken connector does not blank the whole screen, and with drill-downs that explain what each number is built from.

**Target Users**:

- Individual contributors viewing their own dashboard
- Team leads inspecting a specific direct report
- Executives drilling further from Team View into a particular person

**Key Problems Solved**:

- No surface for "what am I shipping / how am I working"
- No way to scope team-level metrics to a single person without leaving the dashboard
- A single failing data source (e.g., Bitbucket connector) used to blank the whole screen — the per-section error recovery contract changes that

### 1.3 Goals (Business Outcomes)

**Capabilities**:

- Render a per-person KPI strip (period-filtered) with consistent semantics across sections
- Render bullet sections grouped by area (`task_delivery`, `git_output`, `code_quality`, `ai_adoption`, `collaboration`)
- Show LOC and delivery trend charts with period-over-period context
- Banner time-off days so the viewer interprets dips honestly
- Drill into any bullet metric for a per-person, per-metric detail breakdown
- Recover from per-section load failures without blanking the screen

**Non-Goals (this revision)**:

- Per-viewer customization of which sections appear (Dashboard Configurator post-MVP)
- Editing thresholds (sourced from `METRIC_SEMANTICS` / `BULLET_DEFS`; ownership migrating to backend Metric Catalog)
- Cross-person comparison (use Team View)
- Personal "My View" customization layered on top of the role default — explicitly rejected, not deferred (see §12 Risks)

### 1.4 Glossary

| Term | Definition |
|------|------------|
| IC Dashboard | The per-person view rendered for a selected `personId` |
| My Dashboard | IC Dashboard rendered for the viewer themselves (no explicit selection); routed through `userContext` rather than a routing branch |
| Person Header | The top-of-screen identity block (name, role, avatar) rendered by `PersonHeader` |
| KPI Strip | The horizontal KPI band (`KpiStrip`) under the header, period-filtered |
| Bullet Section | A grouped set of bullet charts for a metric area (`task_delivery`, `git_output`, `code_quality`, `ai_adoption`, `collaboration`) |
| Trend Chart | LOC stacked bar chart and delivery trend chart, both period-filtered |
| Time-Off Notice | A banner ribbon shown when the loaded `TimeOffNotice` reports absent days inside the selected period |
| Errored Section | A section whose backend load failed and which renders an inline retry CTA instead of empty bullets |
| Drill | A focused detail popup (`DrillModal`) opened from a bullet metric click |
| Honest null | A metric value rendered as em-dash / `ComingSoon` rather than `0` when the underlying connector is not configured for the person |

## 2. Actors

### 2.1 Human Actors

#### Self-Viewing IC

**ID**: `cpt-ic-dashboard-actor-self-ic`

**Role**: An individual contributor viewing their own dashboard. The selected `personId` resolves to the viewer via `userContext` ("My Dashboard" semantics).
**Needs**: Visibility into their own engineering footprint; honest signals during time-off; trust that nobody is showing them a different view than what their team lead sees.

#### Team Lead Drilling Into a Member

**ID**: `cpt-ic-dashboard-actor-team-lead-drill`

**Role**: A team lead navigated from Team View by clicking a member row. The `personId` is set explicitly to that member.
**Needs**: A 1:1-ready picture of the member's engineering activity for the chosen period.

#### Executive Drilling Down

**ID**: `cpt-ic-dashboard-actor-executive-drill`

**Role**: An executive who continued the drill chain from Team View into a specific person.
**Needs**: To inspect a specific contributor without switching personas.

### 2.2 System Actors

#### Analytics API

**ID**: `cpt-ic-dashboard-actor-analytics-api`

**Role**: Backend exposing per-metric OData queries — IC aggregates, bullet aggregates, LOC trend, delivery trend, time-off, drill details.
**Needs**: Filtered queries scoped by `person_id` and date range.

#### Identity API

**ID**: `cpt-ic-dashboard-actor-identity-api`

**Role**: Provides the `Person` profile (name, role, avatar, supervisor) consumed by `PersonHeader`.

#### Connector Manager

**ID**: `cpt-ic-dashboard-actor-connector-manager`

**Role**: Backend endpoint reporting per-source availability, consumed identically to Executive and Team Views.

## 3. Operational Concept & Environment

### 3.1 Module-Specific Environment Constraints

The screen runs in the browser under the Insight screenset. `personId` comes from `userContextSlice.selectSelectedPersonId`, which falls back to the viewer's own identity when no explicit IC was selected ("My Dashboard"). This removes the need for a routing branch between "/ic-dashboard" and "/ic-dashboard/me" — the slice encodes the intent.

`loadIcDashboard(personId, period, dateRange)` fires every time `personId`, `period`, or `customRange` changes. Internally it issues `Promise.allSettled` over six independent queries (IC aggregates, bullet aggregates, LOC trend, delivery trend, time-off, person profile). Per-section failures are tracked in `erroredSections` so each section can render its own retry CTA without blanking the screen.

The screen is read-only; it never writes back to analytics, identity, or connector services.

## 4. Scope

### 4.1 In Scope

- `PersonHeader` — name, role, avatar, supervisor link
- `KpiStrip` — period-filtered KPI band
- `TimeOffBanner` — absence notice for the selected period
- Bullet sections: Task Delivery, Git Output, Code Quality, AI Dev Tools & AI Chat, Collaboration (each via `MetricCard` or `AiToolsSection` / `CollaborationSection`)
- `LocStackedBar` — LOC trend chart (lines added per period, AI-assisted vs. manual vs. spec/config)
- `DeliveryTrends` — commit / PR / task activity trends
- `DrillModal` — detail popup, opened on bullet metric click
- `PeriodSelectorBar` + `ViewModeToggle` — viewer controls
- Per-section error state + retry (via `erroredSections` + `onRetry={reload}`)
- "Person not found" state for invalid `personId`
- `PrivacyFooter` — privacy / data-handling notice

### 4.2 Out of Scope

- Editing the rendered composition (which sections appear, in what order)
- Personal "My View" customization that diverges from what a team lead sees — explicitly rejected (see §12 Risks)
- CSV / PDF export
- Real-time / streaming updates
- Cross-person comparison (use Team View)

## 5. Functional Requirements

### 5.1 Person Header

#### Person Header Rendering

- [ ] `p1` - **ID**: `cpt-ic-dashboard-fr-person-header`

The screen **MUST** render `PersonHeader` from the loaded `Person` profile. While the profile is loading and `personId` is set, the header **MAY** render a skeleton; on a fully resolved missing person (`!loading && !person && personId`), the screen **MUST** render the "Person not found" surface (see `cpt-ic-dashboard-fr-not-found`) instead of an empty header.

**Actors**: `cpt-ic-dashboard-actor-self-ic`, `cpt-ic-dashboard-actor-team-lead-drill`

### 5.2 KPI Strip

#### KPI Strip Rendering

- [ ] `p1` - **ID**: `cpt-ic-dashboard-fr-kpi-strip`

The screen **MUST** render `KpiStrip` from the period-filtered `kpis`. Null KPIs **MUST** render as honest-null (em-dash / `ComingSoon`), never as `0`. The KPI source is the backend response — no client-side fabrication.

**Actors**: `cpt-ic-dashboard-actor-self-ic`, `cpt-ic-dashboard-actor-team-lead-drill`

### 5.3 Time-Off Banner

#### Time-Off Notice Surface

- [ ] `p1` - **ID**: `cpt-ic-dashboard-fr-time-off-banner`

The screen **MUST** render `TimeOffBanner` when the loaded `TimeOffNotice` reports absent days inside the selected period. The banner **MUST NOT** render when there is no notice or when the period contains no absences. The banner is informational only — it does not adjust metric values.

**Rationale**: A dip in `dev_time_h` for a person on holiday is not underperformance. The banner gives the viewer the context to interpret the period correctly without changing the underlying numbers.

**Actors**: `cpt-ic-dashboard-actor-self-ic`, `cpt-ic-dashboard-actor-team-lead-drill`

### 5.4 Bullet Sections

#### Bullet Section Composition

- [ ] `p1` - **ID**: `cpt-ic-dashboard-fr-bullet-sections`

The screen **MUST** render five bullet sections from `bulletMetrics`, partitioned by `section`: `task_delivery`, `git_output`, `code_quality`, `ai_adoption`, `collaboration`. Each section's bullets **MUST** carry their own thresholds via `BULLET_DEFS`. Empty sections (all bullets null) **MUST** render as `ComingSoon` rather than disappearing.

Section names **MUST** match Team View's section taxonomy so the same `BULLET_DEFS` drive labels and thresholds in both screens.

**Actors**: `cpt-ic-dashboard-actor-self-ic`, `cpt-ic-dashboard-actor-team-lead-drill`

### 5.5 Trend Charts

#### LOC and Delivery Trend Charts

- [ ] `p1` - **ID**: `cpt-ic-dashboard-fr-trend-charts`

The screen **MUST** render `LocStackedBar` from `charts.locTrend` and `DeliveryTrends` from `charts.deliveryTrend` inside collapsible sections. Each chart **MUST** render `ComingSoon` when its underlying series is empty. The charts **MUST NOT** silently render zero-bars when the source connector is missing.

**Actors**: `cpt-ic-dashboard-actor-self-ic`, `cpt-ic-dashboard-actor-team-lead-drill`

### 5.6 Drill Modal

#### Drill Modal Integration

- [ ] `p1` - **ID**: `cpt-ic-dashboard-fr-drill-modal`

The screen **MUST** open a `DrillModal` when `openDrill(personId, drillId)` is dispatched. The modal **MUST** close cleanly via `closeDrill` and **MUST NOT** persist drill state across `personId` or period changes.

**Actors**: `cpt-ic-dashboard-actor-self-ic`, `cpt-ic-dashboard-actor-team-lead-drill`

### 5.7 Period Selection & View Mode

#### Period Selector and View Mode Integration

- [ ] `p1` - **ID**: `cpt-ic-dashboard-fr-period-and-view-mode`

The screen **MUST** render `PeriodSelectorBar` and `ViewModeToggle`. Period changes **MUST** trigger a fresh `loadIcDashboard`. View-mode changes **MUST** propagate to bullet sections via the `viewMode` prop.

**Actors**: `cpt-ic-dashboard-actor-self-ic`, `cpt-ic-dashboard-actor-team-lead-drill`

### 5.8 Per-Section Error Recovery

#### Per-Section Error State

- [ ] `p1` - **ID**: `cpt-ic-dashboard-fr-per-section-error`

When a backend query for a specific section fails, the section **MUST** be marked in `erroredSections` and **MUST** render an inline retry CTA via `MetricCard`'s `errored` + `onRetry` props. Sibling sections **MUST** continue rendering normally; a single section failure **MUST NOT** blank the whole screen.

**Rationale**: Before this contract a transient Bitbucket failure could blank Task Delivery, Git Output, Code Quality, and Trends together — the viewer was forced to refresh the page. Per-section recovery means the viewer keeps the parts that loaded and recovers the parts that didn't.

**Actors**: `cpt-ic-dashboard-actor-self-ic`, `cpt-ic-dashboard-actor-team-lead-drill`

### 5.9 Person Not Found State

#### Person Not Found Surface

- [ ] `p2` - **ID**: `cpt-ic-dashboard-fr-not-found`

When `personId` is set, the load is complete (`loading === false`), and `person` is null, the screen **MUST** render a "Person not found" surface explaining that no data is available for the supplied ID. The surface **MUST NOT** render while the load is still in progress, to avoid flashing.

**Actors**: `cpt-ic-dashboard-actor-team-lead-drill`, `cpt-ic-dashboard-actor-executive-drill`

### 5.10 Privacy Footer

#### Privacy Footer Rendering

- [ ] `p2` - **ID**: `cpt-ic-dashboard-fr-privacy-footer`

The screen **MUST** render `PrivacyFooter` at the bottom, surfacing the privacy / data-handling notice for per-person metrics. The footer is informational and **MUST NOT** block any other rendering.

**Rationale**: IC Dashboard is the most person-identifying surface in the product; a persistent privacy notice is a baseline trust signal.

**Actors**: `cpt-ic-dashboard-actor-self-ic`

## 6. Non-Functional Requirements

### 6.1 NFR Inclusions

#### First Meaningful Paint

- [ ] `p1` - **ID**: `cpt-ic-dashboard-nfr-first-paint`

First meaningful paint **SHOULD** complete within 2 seconds on stable network from `loadIcDashboard` invocation to the first non-loading dispatch, excluding initial bundle download.

#### Honest-Null Rendering

- [ ] `p1` - **ID**: `cpt-ic-dashboard-nfr-honest-null`

No metric value **MUST** silently coerce a null source row into `0` for display, KPI rendering, bullet rendering, or trend chart rendering. This applies uniformly across `KpiStrip`, `MetricCard`, `LocStackedBar`, and `DeliveryTrends`.

#### View Parity

- [ ] `p1` - **ID**: `cpt-ic-dashboard-nfr-view-parity`

The IC Dashboard rendered for a person **MUST** be byte-for-byte identical regardless of whether the viewer is the person themself, their team lead, or a VP. Personalization that diverges from the role-default view is explicitly out of scope (see §12 Risks). Side-by-side comparability across people in the same role is a load-bearing property of Insight as a management instrument.

#### Internationalization

- [ ] `p2` - **ID**: `cpt-ic-dashboard-nfr-i18n`

All visible strings **MUST** route through the `useTranslation` hook with translation keys defined in `i18n/<lang>.json` for every supported language registered on `INSIGHT_SCREENSET_ID` / `IC_DASHBOARD_SCREEN_ID`.

### 6.2 NFR Exclusions

The following non-functional concerns are out of scope: real-time / streaming updates, offline operation, server-side rendering, accessibility audit beyond what UIKit components inherit, observability instrumentation beyond what the screenset already emits.

## 7. Public Library Interfaces

### 7.1 Public API Surface

This screen does not expose a public library interface. It is consumed only by the Insight screenset routing table at `/ic-dashboard`.

### 7.2 External Integration Contracts

#### IC Aggregates Contract

- [ ] `p1` - **ID**: `cpt-ic-dashboard-contract-ic-aggregates`

The screen depends on `METRIC_REGISTRY.IC_AGGREGATES` returning `RawIcAggregateRow[]` filtered by `person_id` and the date range. `transformIcKpis` maps the response shape to `KpiStrip` props.

#### Bullet Aggregates Contract

- [ ] `p1` - **ID**: `cpt-ic-dashboard-contract-bullet-aggregates`

The screen depends on `METRIC_REGISTRY.IC_BULLETS` (or equivalent per-section bullet UUIDs) returning `RawBulletAggregateRow[]` partitioned by `section`. Section names **MUST** stay aligned with Team View's `BULLET_DEFS` taxonomy.

#### LOC and Delivery Trend Contracts

- [ ] `p1` - **ID**: `cpt-ic-dashboard-contract-trends`

The screen depends on LOC trend (`RawLocTrendRow[]`) and delivery trend (`RawDeliveryTrendRow[]`) queries returning per-period aggregates for the selected person.

#### Time-Off Contract

- [ ] `p2` - **ID**: `cpt-ic-dashboard-contract-time-off`

The screen depends on a time-off query returning `RawTimeOffRow[]` for the selected `person_id` and date range. The transform `transformTimeOff` produces a `TimeOffNotice` consumed by `TimeOffBanner`.

#### Identity Person Contract

- [ ] `p1` - **ID**: `cpt-ic-dashboard-contract-identity-person`

The screen depends on `IdentityApiService` returning a `Person` for the selected `personId`. Required fields include `name`, `role`, `avatar` (optional), and `supervisor_email` (optional).

#### Drill Detail Contract

- [ ] `p2` - **ID**: `cpt-ic-dashboard-contract-drill`

The screen depends on a drill query returning `RawDrillRow[]` keyed by `drillId` for a specific person. Consumed via `transformDrill`.

## 8. Use Cases

### UC-001 IC Reviews Their Own Dashboard

**ID**: `cpt-ic-dashboard-usecase-self-review`

**Actors**: `cpt-ic-dashboard-actor-self-ic`, `cpt-ic-dashboard-actor-analytics-api`, `cpt-ic-dashboard-actor-identity-api`

**Preconditions**: User is authenticated; `userContext` resolves to their own `personId`.

**Flow**:

1. User navigates to `/ic-dashboard`.
2. `selectSelectedPersonId` returns the viewer's own `personId`; `loadIcDashboard` fires.
3. KPIs, bullets, trends, time-off banner, and person header populate.
4. User changes period; `loadIcDashboard` re-fires.

**Postconditions**: User has a self-view picture of their engineering activity for the chosen period.

### UC-002 Team Lead Drills Into a Member's IC Dashboard

**ID**: `cpt-ic-dashboard-usecase-team-lead-drill`

**Actors**: `cpt-ic-dashboard-actor-team-lead-drill`, `cpt-ic-dashboard-actor-analytics-api`

**Preconditions**: Team lead is on Team View and clicked a member row.

**Flow**:

1. Team View dispatches `selectIcPerson(personId)` then navigates to IC Dashboard.
2. IC Dashboard mounts with the explicit `personId`.
3. KPIs, bullets, trends, header, and time-off banner populate for that person.
4. Team lead drills into a bullet for further detail.

**Postconditions**: Team lead inspected a member's full IC view; what they see is byte-for-byte identical to what the IC sees for themselves.

### UC-003 IC Drills Into a Bullet Metric for Detail

**ID**: `cpt-ic-dashboard-usecase-bullet-drill`

**Actors**: `cpt-ic-dashboard-actor-self-ic`

**Preconditions**: IC Dashboard loaded with at least one bullet rendered.

**Flow**:

1. User clicks a bullet in any section.
2. `openDrill(personId, drillId)` dispatches.
3. `DrillModal` opens with the per-person, per-metric breakdown.
4. User closes the modal via `closeDrill`.

**Postconditions**: Drill state cleared; underlying screen state unchanged.

## 9. Acceptance Criteria

- [ ] The IC Dashboard rendered for a person is identical regardless of viewer (self / team lead / executive).
- [ ] Null KPIs, null bullets, and empty trend series render as honest-null (em-dash / `ComingSoon`), never as `0`.
- [ ] A per-section load failure marks that section in `erroredSections` and renders an inline retry CTA without affecting siblings.
- [ ] "Person not found" renders only after a complete load with no `Person`; never during loading.
- [ ] Time-off banner appears when and only when the loaded `TimeOffNotice` overlaps the selected period.
- [ ] Period change re-fires the load; previous data stays visible until the new load completes.
- [ ] Drill modal closes cleanly and does not leak state across `personId` or period changes.
- [ ] Every visible string routes through `useTranslation`; no hardcoded English literals in the rendered DOM.
- [ ] `PrivacyFooter` is present on every render of the screen.

## 10. Dependencies

| Dependency | Description | Criticality |
|------------|-------------|-------------|
| Analytics API IC + bullet + trend + time-off queries | Per-metric OData UUIDs from `METRIC_REGISTRY` | p1 |
| Identity API (`IdentityApiService`) | Returns the `Person` profile consumed by `PersonHeader` | p1 |
| Connector Manager Service | Per-source availability, consumed identically to Executive and Team Views | p2 |
| `@hai3/react` | `eventBus`, `apiRegistry`, `useAppSelector`, `useTranslation`, `I18nRegistry` | p1 |
| Period slice + `usePeriod` | Source of truth for current period and custom range | p1 |
| `userContextSlice` | Resolves `selectedPersonId`, falling back to viewer for "My Dashboard" semantics | p1 |
| `insightUiSlice` | Provides `viewMode` for bullet sections | p2 |
| `BULLET_DEFS` (`viewConfigs.ts`) | Section labels, units, thresholds — shared with Team View | p1 |
| `transformIcKpis`, `transformBulletMetrics`, `transformLocTrend`, `transformDeliveryTrend`, `transformTimeOff`, `transformDrill` | OData → domain shape | p1 |
| UIKit composite components | `PersonHeader`, `KpiStrip`, `TimeOffBanner`, `MetricCard`, `LocStackedBar`, `DeliveryTrends`, `DrillModal`, `PeriodSelectorBar`, `ViewModeToggle`, `CollapsibleSection`, `PrivacyFooter` | p1 |

## 11. Assumptions

- Authentication is enforced upstream (Auth module / `AuthGuard`).
- `userContextSlice` is hydrated before IC Dashboard mounts; viewer identity is available for "My Dashboard" fallback.
- Identity Resolution returns a valid `Person` for any `personId` an upstream surface exposed (Team View row click, Executive View drill chain).
- All metric values returned by IC and bullet queries are either finite numbers or honest null; no fabricated zeros at the transform layer.
- Threshold definitions in `BULLET_DEFS` are stable across viewers; per-viewer customization is out of scope.

## 12. Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Personal "My View" customization layered over the role-default IC view | A team lead and the IC see different IC Dashboards → comparability across people in the same role breaks → Insight stops working as a management instrument | Explicitly rejected as a v1 design choice. Personal customization can only live on a separate "My View" / pinning surface that never replaces the role-default — covered by `cpt-ic-dashboard-nfr-view-parity` |
| Period change while a load is in flight | Older response overwrites newer state | Introduce `AbortController` per load (tracked in P0 audit task `6gQxvQg4Hf4P8HJ6`) |
| `personId` set to a stale or invalid value after a Team View change | "Person not found" flashes mid-load instead of after | The "not found" surface gates on `!loading && !person && personId` — the load-complete check is non-negotiable |
| Connector failure on one section blanks the whole screen | Viewer thinks the entire dashboard is broken | Already addressed by `cpt-ic-dashboard-fr-per-section-error`; preserved in PR #44 (per-section progressive loading) |
| Threshold definitions diverge between FE `BULLET_DEFS` and backend Metric Catalog post-migration | Bullet coloring drifts from official thresholds | Treat `BULLET_DEFS` as v1-only; once Metric Catalog ships, source thresholds from `GET /catalog/metrics` |
| `stale_in_progress` and other snapshot bullets dropping out of historical periods | Bullet renders as ComingSoon when the snapshot's `metric_date = today()` is outside the selected period | Tracked separately as Todoist task `6gW3J5JqpXv2X8R7`; long-term move snapshot metrics into a separate "Operational health" section that does not get filtered by period |

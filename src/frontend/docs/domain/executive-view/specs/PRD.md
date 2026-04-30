# PRD — Executive View


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
  - [5.1 Org-Level KPI Cards](#51-org-level-kpi-cards)
  - [5.2 Org Health Radar](#52-org-health-radar)
  - [5.3 Team Metrics Bar](#53-team-metrics-bar)
  - [5.4 Teams Table](#54-teams-table)
  - [5.5 Period Selection](#55-period-selection)
  - [5.6 Data Availability Awareness](#56-data-availability-awareness)
  - [5.7 Loading & Error States](#57-loading--error-states)
- [6. Non-Functional Requirements](#6-non-functional-requirements)
  - [6.1 NFR Inclusions](#61-nfr-inclusions)
  - [6.2 NFR Exclusions](#62-nfr-exclusions)
- [7. Public Library Interfaces](#7-public-library-interfaces)
  - [7.1 Public API Surface](#71-public-api-surface)
  - [7.2 External Integration Contracts](#72-external-integration-contracts)
- [8. Use Cases](#8-use-cases)
  - [UC-001 VP Reviews Org-Wide Engineering Health for Last Quarter](#uc-001-vp-reviews-org-wide-engineering-health-for-last-quarter)
  - [UC-002 Executive Spots a Team Lagging Behind on Build Success](#uc-002-executive-spots-a-team-lagging-behind-on-build-success)
- [9. Acceptance Criteria](#9-acceptance-criteria)
- [10. Dependencies](#10-dependencies)
- [11. Assumptions](#11-assumptions)
- [12. Risks](#12-risks)

<!-- /toc -->

## 1. Overview

### 1.1 Purpose

Executive View is the role-invariant top-level dashboard for VPs and senior engineering leaders. It aggregates per-team metrics from the Analytics API into an organization-wide picture and surfaces the teams most likely to need attention. It does not customize per professional role — every viewer at executive scope sees the same composition.

### 1.2 Background / Problem Statement

Insight's analytics pipeline produces per-team metrics (build success, focus time, AI adoption, PR cycle time, etc.) in ClickHouse, exposed through an OData endpoint at the `analytics-api`. Without Executive View, a VP would have to either query OData directly or assemble screenshots from per-team views to get an org-wide picture. The screen exists to make the step from "I want to know how engineering is doing this quarter" to "show me one page" a single click.

**Target Users**:

- VPs of Engineering and CTO-level leaders accountable for org-wide delivery health
- Heads of department reviewing teams that report into them

**Key Problems Solved**:

- No single surface for org-wide engineering health — leaders had to compose the picture from per-team views
- Team-level outliers were invisible without manually walking through each team
- Mixed-source data (`ci`, `tasks`, `git`, AI tooling) needed a single page that handles missing sources honestly

### 1.3 Goals (Business Outcomes)

**Capabilities**:

- Render org-wide KPI averages (build success, AI adoption, focus time) at a glance
- Show every team in one sortable table, ranked or filtered to surface outliers
- Communicate clearly when a metric is missing because the source connector isn't configured (vs. zero or unknown)
- Accept any period the analytics pipeline supports (`30d`, `90d`, `quarter`, custom range)

**Non-Goals (this revision)**:

- Drill-down from the executive view into team-internal breakdowns (lives in Team View)
- Per-VP customization of which metrics appear (planned via Dashboard Configurator)

### 1.4 Glossary

| Term | Definition |
|------|------------|
| Executive View | The role-invariant top-level dashboard rendered for any executive-scope viewer |
| Team | A management unit identified by `team_id` and `team_name`, mapped from Identity Resolution's `org_unit` taxonomy |
| Org KPI | A scalar averaged across all teams in the org for a single metric (e.g., `avgBuildSuccess`) |
| Honest null | A metric value rendered as em-dash / "ComingSoon" rather than `0` when the underlying connector is not configured for the team |
| Connector | A backend ingestion source (`ci`, `tasks`, `git`, AI tooling, HR, comms) whose presence or absence is reported by `data_availability` |
| Period | A user-selected date range driving every metric query on the screen |
| Threshold | A `good` / `warn` boundary used to color a metric value; sourced from `METRIC_SEMANTICS` in v1, will move to backend Metric Catalog |

## 2. Actors

### 2.1 Human Actors

#### Executive Viewer

**ID**: `cpt-executive-view-actor-executive-viewer`

**Role**: VP of Engineering, CTO, head of department, or any user with executive-scope access. Reads the dashboard; does not edit it.
**Needs**: A one-page picture of org-wide engineering health for an arbitrary period; the ability to identify teams that need attention; clear signals when a metric is missing rather than silently zero.

### 2.2 System Actors

#### Analytics API

**ID**: `cpt-executive-view-actor-analytics-api`

**Role**: Backend service exposing `GET /odata/v4/insight/queries/<metric_id>` (per-metric OData), where `<metric_id>` is a UUID from `METRIC_REGISTRY`.
**Needs**: A request carrying `$filter` (date range), `$orderby`, `$top` parameters; returns OData rows shaped per-metric.

#### Connector Manager

**ID**: `cpt-executive-view-actor-connector-manager`

**Role**: Backend endpoint returning per-source availability flags (`git`, `tasks`, `ci`, `comms`, `hr`, `ai`).
**Needs**: A periodic poll from the screen at load; response shape `{ git: 'ok' | 'no-connector' | ... }`.

## 3. Operational Concept & Environment

### 3.1 Module-Specific Environment Constraints

The screen runs entirely in the browser under the Insight screenset. It does not own a Web Worker, IndexedDB store, or shared service worker; all state is in-memory in Redux. It assumes:

- A live `analytics-api` deployment at the configured base URL.
- A live `connector-manager` endpoint reachable through `apiRegistry`.
- The auth layer has already attached a Bearer token via `AuthPlugin` to every API call.
- The browser supports ES2020, `fetch`, and `AbortController`.

The viewer navigates to `/executive-view` (route owned by the Insight screenset's routing table). On mount, the screen reads the current `period` from the period slice and fires `loadExecutiveView(resolveDateRange(period, customRange))`. The action emits `ExecutiveViewLoadStarted`, then calls `Promise.allSettled([api.queryMetric(EXEC_SUMMARY, …), connectors.getDataAvailability()])`. On success it transforms OData rows into `ExecTeamRow[]`, computes `OrgKpis` (averaging `build_success_pct`, `ai_adoption_pct`, `focus_time_pct` across teams that have non-null values), and emits `ExecutiveViewLoaded` plus `ExecutiveViewAvailabilityLoaded`. The corresponding effect dispatches Redux actions on the `executiveView` slice; React components re-render from the slice via typed selectors. Period changes re-fire the same load.

The screen does not write back to the analytics service; it only reads.

## 4. Scope

### 4.1 In Scope

- Org-level KPI cards (`OrgKpiCards`) summarizing avg build success, AI adoption, focus time
- Org health radar (`OrgHealthRadar`) — multi-axis visualization of org KPIs
- Team metrics bar (`TeamMetricsBar`) — per-team comparison along selected metrics
- Teams table (`TeamsTable`) with sortable columns and threshold-based status color
- `PeriodSelectorBar` integration: predefined periods + custom date range
- Honest-null rendering for missing metrics (delegated to the underlying composite components: em-dash / `ComingSoon` for null values)
- Data-availability badges showing which connectors are wired for the org

### 4.2 Out of Scope

- Drill-down into per-team detail (covered by Team View screen)
- Drill-down into per-person detail (covered by IC Dashboard screen)
- Editing thresholds — values sourced from `METRIC_SEMANTICS`, ownership migrating to the backend Metric Catalog
- Per-viewer customization of which metrics appear (covered by Dashboard Configurator post-MVP)
- Export (CSV/PDF) of the rendered view
- Real-time / streaming updates — the screen is request-response per period change

## 5. Functional Requirements

### 5.1 Org-Level KPI Cards

#### Org KPI Aggregation

- [ ] `p1` - **ID**: `cpt-executive-view-fr-org-kpi-cards`

The screen **MUST** render a row of org-level KPI cards summarizing `avgBuildSuccess`, `avgAiAdoption`, and `avgFocus`. Each KPI **MUST** be computed as the rounded mean across teams whose corresponding metric is non-null in the loaded `ExecTeamRow[]`. When every team's value is null, the KPI **MUST** render as honest-null (em-dash or `ComingSoon`), not `0`.

**Rationale**: An empty org returns no rows; an org where no team has the source connector configured produces all-null per-team values. Both must render distinguishably from "zero", or the screen lies about the state of the org.

**Actors**: `cpt-executive-view-actor-executive-viewer`

### 5.2 Org Health Radar

#### Radar Rendering

- [ ] `p1` - **ID**: `cpt-executive-view-fr-org-health-radar`

The screen **MUST** render a radar visualization (`OrgHealthRadar`) using the loaded `orgKpis` when at least one axis has a non-null value. When `orgKpis` is null (load not yet complete), the radar slot **MUST** render an empty placeholder of equivalent height to avoid layout shift.

**Actors**: `cpt-executive-view-actor-executive-viewer`

### 5.3 Team Metrics Bar

#### Per-Team Comparison

- [ ] `p1` - **ID**: `cpt-executive-view-fr-team-metrics-bar`

The screen **MUST** render `TeamMetricsBar` showing per-team comparison along the executive-scope metrics. Teams with all-null values **MUST** still appear so the viewer sees the team exists but its data is unavailable.

**Actors**: `cpt-executive-view-actor-executive-viewer`

### 5.4 Teams Table

#### Teams Table Rendering

- [ ] `p1` - **ID**: `cpt-executive-view-fr-teams-table`

The screen **MUST** render `TeamsTable` with one row per team in `ExecTeamRow[]`, with columns for `team_name`, `headcount`, `tasks_closed`, `bugs_fixed`, `build_success_pct`, `focus_time_pct`, `ai_adoption_pct`, `ai_loc_share_pct`, and `pr_cycle_time_h`. Threshold-driven cell coloring **MUST** use `EXEC_VIEW_CONFIG.column_thresholds` for the configured metrics. Null cells **MUST** render as em-dash / `ComingSoon`, not `0`.

**Actors**: `cpt-executive-view-actor-executive-viewer`

### 5.5 Period Selection

#### Period Selector Integration

- [ ] `p1` - **ID**: `cpt-executive-view-fr-period-selection`

The screen **MUST** render a `PeriodSelectorBar` bound to the period slice. Period changes **MUST** trigger a fresh `loadExecutiveView` with the resolved date range. Custom date range selection **MUST** persist across the load and be supplied to the OData `$filter` via `odataDateFilter(range)`.

**Actors**: `cpt-executive-view-actor-executive-viewer`

### 5.6 Data Availability Awareness

#### Connector Availability Surfacing

- [ ] `p2` - **ID**: `cpt-executive-view-fr-data-availability`

The screen **SHOULD** call `ConnectorManagerService.getDataAvailability()` in parallel with the metric query and **SHOULD** surface per-connector status (`ok` / `no-connector` / `error`) somewhere on the page so a viewer interpreting all-null cells understands whether the data is missing because the period is empty or because the connector is not wired.

**Rationale**: Without availability context, an executive seeing all-null `pr_cycle_time_h` cannot tell if Bitbucket is broken, not yet integrated, or simply quiet for this period. Today the value is wired but not always rendered prominently — this FR keeps the contract explicit.

**Actors**: `cpt-executive-view-actor-executive-viewer`, `cpt-executive-view-actor-connector-manager`

### 5.7 Loading & Error States

#### Loading and Error Surfaces

- [ ] `p1` - **ID**: `cpt-executive-view-fr-loading-error`

While `loading = true`, the screen **MUST** keep the previously loaded data visible (no flicker) and render a non-blocking loading indicator. On `ExecutiveViewLoadFailed`, it **MUST** render a non-blocking error surface that explains what failed without losing the viewer's previously rendered period selection.

**Rationale**: Blanking the screen on every period change creates a flickery experience; failed loads must not leave the viewer with an empty page they cannot recover from without a manual refresh.

**Actors**: `cpt-executive-view-actor-executive-viewer`

## 6. Non-Functional Requirements

### 6.1 NFR Inclusions

#### First Meaningful Paint

- [ ] `p1` - **ID**: `cpt-executive-view-nfr-first-paint`

First meaningful paint of the screen for a typical org (≤30 teams) **SHOULD** complete within 2 seconds on stable network from `loadExecutiveView` invocation to `ExecutiveViewLoaded` event emission, excluding initial bundle download.

#### Large-Org Rendering

- [ ] `p2` - **ID**: `cpt-executive-view-nfr-large-org`

The screen **SHOULD** render correctly for orgs of up to 200 teams without breaking layout or scroll. Past that, performance is best-effort until pagination or virtualization is introduced.

#### Honest-Null Rendering

- [ ] `p1` - **ID**: `cpt-executive-view-nfr-honest-null`

No metric value **MUST** silently coerce a null source row into `0` for display purposes. The screen relies on `ExecTeamRow` carrying `number | null` per metric and on UI primitives that render `null` as em-dash / `ComingSoon`.

#### Internationalization

- [ ] `p2` - **ID**: `cpt-executive-view-nfr-i18n`

All visible strings **MUST** route through the `useTranslation` hook with translation keys defined in `i18n/<lang>.json` for every supported language registered on `INSIGHT_SCREENSET_ID` / `EXECUTIVE_VIEW_SCREEN_ID`.

### 6.2 NFR Exclusions

The following non-functional concerns are out of scope for this PRD: real-time / streaming updates, offline operation, server-side rendering, accessibility audit beyond what UIKit components inherit, and observability instrumentation (telemetry events from this screen are owned at the screenset boundary).

## 7. Public Library Interfaces

### 7.1 Public API Surface

This screen does not expose a public library interface. It is consumed only by the Insight screenset routing table at `/executive-view` and is not imported by other screensets or external packages.

### 7.2 External Integration Contracts

- [ ] `p1` - **ID**: `cpt-executive-view-contract-exec-summary`

The screen depends on the `EXEC_SUMMARY` metric query (`METRIC_REGISTRY.EXEC_SUMMARY`, UUID `00000000-0000-0000-0001-000000000001`) returning OData rows shaped as `RawExecSummaryRow[]`, with one row per team in the org. Required raw fields include `org_unit_id`, `org_unit_name`, `headcount`, plus the metric numerics consumed by `transformExecRows`. A breaking change to this contract (renamed columns, removed UUID, scope inversion to per-person) **MUST** be coordinated with this screen's transform layer (`src/screensets/insight/api/transforms.ts`).

- [ ] `p2` - **ID**: `cpt-executive-view-contract-data-availability`

The screen depends on `ConnectorManagerService.getDataAvailability()` returning a `DataAvailability` object keyed by connector type (`git`, `tasks`, `ci`, `comms`, `hr`, `ai`). Adding a new connector type is additive; renaming or removing one is breaking.

## 8. Use Cases

### UC-001 VP Reviews Org-Wide Engineering Health for Last Quarter

**ID**: `cpt-executive-view-usecase-quarterly-review`

**Actors**: `cpt-executive-view-actor-executive-viewer`, `cpt-executive-view-actor-analytics-api`

**Preconditions**: User is authenticated with executive-scope role. Analytics pipeline has populated metrics for the requested period.

**Flow**:

1. User navigates to `/executive-view`.
2. Screen mounts with the default period (`30d` or persisted value); `loadExecutiveView` fires.
3. User clicks the period selector and chooses "Last quarter".
4. Period slice updates; effect re-fires `loadExecutiveView` with the new range.
5. KPI cards, radar, bar, and table re-render with quarter-scoped data.

**Postconditions**: User has a one-page picture of org engineering health for the chosen period; selected period persists on subsequent visits.

### UC-002 Executive Spots a Team Lagging Behind on Build Success

**ID**: `cpt-executive-view-usecase-spot-laggard`

**Actors**: `cpt-executive-view-actor-executive-viewer`

**Preconditions**: Screen loaded with at least one team's `build_success_pct` non-null.

**Flow**:

1. User scans the Teams Table, sorted by `build_success_pct` ascending.
2. The lowest team's `build_success_pct` cell is rendered with the `bad` threshold color per `EXEC_VIEW_CONFIG.column_thresholds`.
3. User makes a mental note of the team and follows up out-of-band (drill-down is out of scope for this PRD).

**Postconditions**: User identified the lagging team; no state change in the screen.

## 9. Acceptance Criteria

- [ ] Org KPI cards render for any org with at least one team and at least one non-null source-connected metric, with all-null KPIs rendering as em-dash.
- [ ] Teams Table lists every team returned by `EXEC_SUMMARY`, including teams with all-null metrics; null cells render as em-dash and never as `0`.
- [ ] Period selection drives a fresh metric load; both predefined periods and custom date ranges produce a correctly filtered query.
- [ ] Loading state retains previous data without flicker; failed loads render a non-blocking error surface that preserves period selection.
- [ ] No string in the rendered DOM is a hardcoded English literal — every label routes through `useTranslation`.
- [ ] The screen renders without layout breakage for orgs up to ~200 teams.
- [ ] First-meaningful-paint after period change is ≤ 2 seconds for orgs of ≤ 30 teams on stable network.

## 10. Dependencies

| Dependency | Description | Criticality |
|------------|-------------|-------------|
| Analytics API `EXEC_SUMMARY` query | Provides per-team metric rows; UUID `METRIC_REGISTRY.EXEC_SUMMARY` | p1 |
| Connector Manager Service | Reports per-source availability; consumed by `cpt-executive-view-fr-data-availability` | p2 |
| `@hai3/react` | Provides `eventBus`, `apiRegistry`, `useAppSelector`, `useTranslation`, `I18nRegistry` | p1 |
| Period slice + `usePeriod` hook | Source of truth for current period and custom range | p1 |
| `METRIC_SEMANTICS` (`thresholdConfig.ts`) | Provides `good` / `warn` thresholds per metric for cell coloring | p1 |
| `transformExecRows` (`api/transforms.ts`) | Maps `RawExecSummaryRow[]` to `ExecTeamRow[]` | p1 |
| UIKit composite components | `OrgKpiCards`, `OrgHealthRadar`, `TeamMetricsBar`, `TeamsTable`, `PeriodSelectorBar` | p1 |

## 11. Assumptions

- Authentication is enforced upstream (Auth module / `AuthGuard`); the screen assumes the caller is already authorized.
- The `EXEC_SUMMARY` OData query is not paginated by the screen; up to 200 rows fit in a single response (`$top: 200`).
- Threshold definitions in `METRIC_SEMANTICS` are stable across executive viewers; per-viewer threshold customization is out of scope.
- All metric values returned by `EXEC_SUMMARY` are either a finite number or honest null; the transform layer does not invent values for missing rows.

## 12. Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| `EXEC_SUMMARY` returns rows shaped differently after a backend rewrite | Screen renders blank or breaks transforms | Pin contract via `cpt-executive-view-contract-exec-summary`; transform layer validates required fields and degrades gracefully on unknown columns |
| All connectors report `no-connector` for a tenant | Every metric renders as em-dash; viewer thinks the screen is broken | Surface availability badges (`cpt-executive-view-fr-data-availability`) so the viewer understands the state |
| Org grows past 200 teams | Single-shot OData load and unvirtualized table degrade | Promote `cpt-executive-view-nfr-large-org` to `p1` and introduce pagination / virtualization once a real tenant trips the threshold |
| Threshold definitions diverge between FE `METRIC_SEMANTICS` and backend Metric Catalog post-migration | Cell colors drift from official thresholds | Treat `METRIC_SEMANTICS` as v1-only; once Metric Catalog ships, retire local thresholds and source coloring from `GET /catalog/metrics` |
| Period changes during in-flight load create a stale-data race | Older response overwrites newer state | Introduce an `AbortController` per load (already tracked as `Insight-front: критический аудит` P0 #3) |

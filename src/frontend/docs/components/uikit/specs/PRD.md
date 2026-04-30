# PRD — UIKit (Insight Composites)


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
  - [5.1 Stateless Presentational Contract](#51-stateless-presentational-contract)
  - [5.2 ComingSoon Empty / Error State](#52-comingsoon-empty--error-state)
  - [5.3 Bullet Chart Composite](#53-bullet-chart-composite)
  - [5.4 Metric Card Composite](#54-metric-card-composite)
  - [5.5 KPI Strip Composite](#55-kpi-strip-composite)
  - [5.6 Trend Chart Composites](#56-trend-chart-composites)
  - [5.7 Drill Modal Composite](#57-drill-modal-composite)
  - [5.8 Period Selector Bar Composite](#58-period-selector-bar-composite)
  - [5.9 View Mode Toggle Composite](#59-view-mode-toggle-composite)
  - [5.10 Collapsible Section Composite](#510-collapsible-section-composite)
- [6. Non-Functional Requirements](#6-non-functional-requirements)
  - [6.1 NFR Inclusions](#61-nfr-inclusions)
  - [6.2 NFR Exclusions](#62-nfr-exclusions)
- [7. Public Library Interfaces](#7-public-library-interfaces)
  - [7.1 Public API Surface](#71-public-api-surface)
  - [7.2 External Integration Contracts](#72-external-integration-contracts)
- [8. Use Cases](#8-use-cases)
  - [UC-001 Screen Renders a Bullet Section with Mixed Honest-Null Bullets](#uc-001-screen-renders-a-bullet-section-with-mixed-honest-null-bullets)
  - [UC-002 Screen Recovers From a Per-Section Backend Failure](#uc-002-screen-recovers-from-a-per-section-backend-failure)
- [9. Acceptance Criteria](#9-acceptance-criteria)
- [10. Dependencies](#10-dependencies)
- [11. Assumptions](#11-assumptions)
- [12. Risks](#12-risks)

<!-- /toc -->

## 1. Overview

### 1.1 Purpose

The Insight UIKit is the set of composite UI components owned by the Insight frontend that wrap HAI3 primitives (`@hai3/uikit`) into domain-meaningful building blocks: bullet charts, metric cards, KPI strips, trend charts, drill modals, period and view-mode controls, and the universal `ComingSoon` placeholder. Every Insight screen (Executive View, Team View, IC Dashboard) composes its UI from these pieces.

### 1.2 Background / Problem Statement

`@hai3/uikit` ships generic primitives (Badge, Card, Button, Popover, Calendar, Collapsible, ToggleGroup, Dialog, Table, Chart). Insight needs domain composites that encode our metric semantics, our threshold coloring, our honest-null rules, and our period taxonomy without having every screen reinvent those decisions inline.

Without this layer, three problems emerge:

- Each screen would re-implement the same bullet, KPI, and metric-card patterns with subtle drift between them.
- Honest-null behavior would not be uniform — some surfaces would render `0`, others em-dash, others nothing at all.
- Threshold coloring and `good`/`warn`/`bad` semantics would diverge between Executive, Team, and IC views.

This UIKit layer collapses those decisions into a single owned library.

### 1.3 Goals (Business Outcomes)

**Capabilities**:

- Provide one canonical set of composites consumed by every Insight screen
- Enforce the honest-null contract uniformly across bullets, KPIs, and trends
- Expose a single `ComingSoon` primitive with two well-defined states (`empty` vs. `error`)
- Stay purely presentational — no state imports, no Redux access, no event-bus emissions inside composites
- Build entirely on `@hai3/uikit` primitives + Tailwind, never on inline styles or duplicate primitives

**Non-Goals (this revision)**:

- Replacing or shadowing HAI3 primitives — composites wrap, never re-implement
- Cross-tenant theming — single Tailwind theme is in scope, per-tenant theming is not
- Storybook / visual regression infrastructure — tracked separately

### 1.4 Glossary

| Term | Definition |
|------|------------|
| Composite | An Insight-owned UI component that wraps HAI3 primitives (`Badge`, `Card`, etc.) into a domain-meaningful building block |
| Base helper | A low-level visual building block under `uikit/base/` (`DynamicWidthBar`, `MetricInfo`, `ProgressTrack`) used inside composites |
| HAI3 primitive | A component imported from `@hai3/uikit`; not owned by Insight, not in scope for this PRD |
| ComingSoon | The universal placeholder for absent backend data; states are `empty` (no rows for filter) or `error` (load rejected) |
| Honest null | Rendering `null` metric values as em-dash or `ComingSoon`, never coercing to `0` |
| Tile mode / Chart mode | Two render modes for bullet sections — compact card grid vs. full track + footer |
| Drill | Per-metric or per-cell detail popup rendered by `DrillModal` |
| Stateless / props-only | A composite **MUST NOT** import slices, dispatch actions, or emit events — state flows in through props, changes flow out through callbacks |

## 2. Actors

### 2.1 Human Actors

#### Indirect End User

**ID**: `cpt-uikit-actor-end-user-indirect`

**Role**: Any human (executive, team lead, IC) who interacts with Insight screens. They never touch composites directly — every interaction is mediated by a screen — but the composite's accessibility, keyboard navigation, and visual contract is what they actually experience.
**Needs**: Predictable visual semantics across screens (a `ComingSoon` looks the same in IC Dashboard as in Team View; a bullet's threshold coloring means the same thing everywhere); accessibility characteristics inherited from HAI3 (focus, escape, ARIA roles).

### 2.2 System Actors

#### Consuming Screen

**ID**: `cpt-uikit-actor-consuming-screen`

**Role**: Any Insight screen (Executive View, Team View, IC Dashboard) that composes its layout from UIKit composites. Owns state, dispatches actions, emits events; passes derived data to composites as props and receives user intent through callbacks.
**Needs**: Predictable, prop-driven composites with consistent honest-null behavior, threshold coloring, and accessibility characteristics inherited from HAI3.

#### HAI3 UIKit Library

**ID**: `cpt-uikit-actor-hai3-uikit`

**Role**: External package `@hai3/uikit` providing primitives (Badge, Card, Button, Popover, Calendar, Collapsible, ToggleGroup, Dialog, Table, Chart wrappers) plus their Radix UI / recharts foundation.
**Needs**: Not owned by Insight; consumed as a versioned dependency. Breaking changes upstream cascade to composites.

## 3. Operational Concept & Environment

### 3.1 Module-Specific Environment Constraints

Composites live under `src/screensets/insight/uikit/`:

- `composite/` — domain composites listed in §5
- `base/` — low-level visual helpers (`DynamicWidthBar`, `MetricInfo`, `ProgressTrack`, `chartColors`)

They are imported only by Insight screens and by other composites. They **MUST NOT** import from `src/app/`, `src/screensets/insight/actions/`, `src/screensets/insight/slices/`, or `src/screensets/insight/effects/`. They **MAY** import from `src/screensets/insight/types`, `src/screensets/insight/utils`, and from `@hai3/uikit` / `@hai3/react`.

This boundary keeps composites trivially testable and reusable across screens — the moment a composite depends on a slice, it ceases to be reusable in a different state shape.

## 4. Scope

### 4.1 In Scope

- Ten composite components under `composite/`: `BulletChart`, `MetricCard`, `KpiStrip`, `LocStackedBar`, `DeliveryTrends`, `DrillModal`, `PeriodSelectorBar`, `ViewModeToggle`, `CollapsibleSection`, `ComingSoon`
- Four base helpers under `base/`: `DynamicWidthBar`, `MetricInfo`, `ProgressTrack`, `chartColors`
- Honest-null contract enforced inside the composites (so every consuming screen inherits it for free)
- The `ComingSoon` empty / error state contract
- Stateless / props-only invariant

### 4.2 Out of Scope

- HAI3 primitives themselves (`Badge`, `Card`, `Button`, etc.) — owned by `@hai3/uikit` upstream
- Recharts internals (consumed indirectly through `@hai3/uikit` chart wrappers + a small number of direct imports for `Tooltip` / `Legend`)
- Per-tenant theming or branding
- Storybook, visual regression testing, or pixel-perfect snapshots
- Mock data for screen demos (lives in `src/screensets/insight/api/mocks/`, not in UIKit)

## 5. Functional Requirements

### 5.1 Stateless Presentational Contract

#### Stateless / Props-Only Composites

- [ ] `p1` - **ID**: `cpt-uikit-fr-stateless`

Every composite **MUST** be purely presentational: no Redux slice imports, no `useAppSelector`, no `eventBus.emit`, no API service calls, no `apiRegistry` access. State flows in via props; changes flow out via callbacks. Internal `useState` is permitted only for ephemeral UI state (hover, popover open, controlled date picker draft).

**Rationale**: A composite that knows about Redux is no longer reusable in a different state shape and cannot be rendered in isolation for tests or stories. Keeping the boundary strict is the cheapest way to keep the layer composable.

**Actors**: `cpt-uikit-actor-consuming-screen`

### 5.2 ComingSoon Empty / Error State

#### Universal Placeholder Contract

- [ ] `p1` - **ID**: `cpt-uikit-fr-coming-soon`

The screen / composite **MUST** render `ComingSoon` for any surface whose backend data is absent, with one of two explicit states:

- `state='empty'` (default): the backend responded successfully but has no rows for the current filter (e.g., a CPO has no commits). Rendered as informational, no retry.
- `state='error'`: the backend query rejected (network error, 5xx, etc.). Rendered with a Retry button when `onRetry` is supplied.

Empty vs. error decisions **MUST** be made in actions / slices based on `Promise.allSettled` results. The component itself **MUST NOT** guess at the cause.

`ComingSoon` accepts a `variant` (`card` / `chip` / `row`) so callers can match the layout slot it replaces.

**Rationale**: Without this, every consuming surface ends up inventing its own empty / error placeholder, and "no data" looks indistinguishable from "request failed". Centralizing the placeholder makes both states unambiguous.

**Actors**: `cpt-uikit-actor-consuming-screen`

### 5.3 Bullet Chart Composite

#### Bullet Chart Composite Rendering

- [ ] `p1` - **ID**: `cpt-uikit-fr-bullet-chart`

`BulletChart` **MUST** render a single bullet metric in two modes: `chart` (full track + footer) or `tile` (compact card). Threshold coloring **MUST** come from props (`good`, `warn`, `higher_is_better`); the composite **MUST NOT** look thresholds up itself. Null `value` **MUST** render as `ComingSoon` of variant `row`.

**Actors**: `cpt-uikit-actor-consuming-screen`

### 5.4 Metric Card Composite

#### Metric Card Grouping

- [ ] `p1` - **ID**: `cpt-uikit-fr-metric-card`

`MetricCard` **MUST** wrap a group of `BulletChart` instances inside a `Card` with optional title and footer legend. The card **MUST** support an `errored` flag — when true, the entire card body **MUST** render `ComingSoon` of variant `card` with state `error` and an `onRetry` callback. Sibling cards on the same screen **MUST NOT** be affected when one card is errored.

**Actors**: `cpt-uikit-actor-consuming-screen`

### 5.5 KPI Strip Composite

#### KPI Strip Layout

- [ ] `p1` - **ID**: `cpt-uikit-fr-kpi-strip`

`KpiStrip` **MUST** render a flex row of KPI cells with `value`, `label`, `sublabel`, and an optional delta `Badge`. A `plain` prop toggles the wrapping `Card` (used by `IC Dashboard` to embed inside a parent card without doubling the chrome). Null KPI values **MUST** render as `ComingSoon` of variant `chip`, never as `0`.

**Actors**: `cpt-uikit-actor-consuming-screen`

### 5.6 Trend Chart Composites

#### LOC and Delivery Trend Charts

- [ ] `p1` - **ID**: `cpt-uikit-fr-trend-charts`

`LocStackedBar` and `DeliveryTrends` **MUST** render trend visualizations from prop-supplied data series. Both composites **MUST** aggregate raw weekly rows by the consumer-supplied period (`week→daily`, `month→4w`, `quarter→monthly`, `year→quarterly`) on the client side. Both **MUST** render `ComingSoon` when the input series is empty.

Chart primitives **MUST** come from `@hai3/uikit`; only `Tooltip` / `Legend` may be imported directly from `recharts` for cases not yet covered by HAI3 wrappers.

**Actors**: `cpt-uikit-actor-consuming-screen`

### 5.7 Drill Modal Composite

#### Drill Modal Rendering

- [ ] `p1` - **ID**: `cpt-uikit-fr-drill-modal`

`DrillModal` **MUST** render a modal overlay using HAI3's `Dialog` primitive (focus trap, escape-key handling, portal). It **MUST** display tabular drill data using HAI3's `Table` primitive and **MUST** call the `onClose` prop on dismissal. The modal **MUST** be open when `open` is `true` and a `drill` payload is supplied; otherwise it renders nothing.

**Actors**: `cpt-uikit-actor-consuming-screen`

### 5.8 Period Selector Bar Composite

#### Period Selector Bar Behavior

- [ ] `p1` - **ID**: `cpt-uikit-fr-period-selector-bar`

`PeriodSelectorBar` **MUST** render a segmented control of period tabs (Week / Month / Quarter / Year) and an inline custom date range picker. The composite **MUST** be controlled — period state and `customRange` flow in via props, and `onPeriodChange` / `onRangeChange` flow out. The composite **MUST NOT** read from Redux directly.

The custom-range picker **MUST** use HAI3's `Popover`, `Calendar`, and `Button` primitives. `react-day-picker` is consumed for `DateRange` typings only.

**Actors**: `cpt-uikit-actor-consuming-screen`

### 5.9 View Mode Toggle Composite

#### View Mode Toggle

- [ ] `p2` - **ID**: `cpt-uikit-fr-view-mode-toggle`

`ViewModeToggle` **MUST** render a Charts / Tiles segmented control via HAI3's `ToggleGroup`. State and change handler flow through props. The composite carries no internal state.

**Actors**: `cpt-uikit-actor-consuming-screen`

### 5.10 Collapsible Section Composite

#### Collapsible Section

- [ ] `p2` - **ID**: `cpt-uikit-fr-collapsible-section`

`CollapsibleSection` **MUST** wrap arbitrary children in a HAI3 `Collapsible` with a clickable trigger row carrying a title and optional subtitle. The composite **MAY** hold internal expanded / collapsed state (UI-ephemeral, not application state).

**Actors**: `cpt-uikit-actor-consuming-screen`

## 6. Non-Functional Requirements

### 6.1 NFR Inclusions

#### Tailwind-Only Styling

- [ ] `p1` - **ID**: `cpt-uikit-nfr-tailwind-only`

Composites **MUST NOT** use inline `style` attributes for static values, custom CSS files, or styled-components. Tailwind utility classes are the sole styling mechanism. Dynamic style values that cannot be expressed as Tailwind classes (e.g., a computed `width` percentage on `DynamicWidthBar`) are the only permitted exception.

#### HAI3 Primitive Reuse

- [ ] `p1` - **ID**: `cpt-uikit-nfr-hai3-primitive-reuse`

Composites **MUST** wrap HAI3 primitives rather than reimplementing them. Adding a new composite that duplicates a HAI3 primitive is rejected; the right move is to consume the HAI3 primitive directly or to push the missing capability upstream.

#### Honest-Null Throughout

- [ ] `p1` - **ID**: `cpt-uikit-nfr-honest-null`

No composite **MUST** silently coerce a null prop into `0` for display. `KpiStrip`, `BulletChart`, `MetricCard`, `LocStackedBar`, `DeliveryTrends`, and any future composite consuming numeric data **MUST** render `ComingSoon` for null / empty inputs.

#### Accessibility Inherited from HAI3

- [ ] `p2` - **ID**: `cpt-uikit-nfr-a11y-inherit`

Composites **SHOULD** inherit Radix UI accessibility characteristics (focus management, keyboard navigation, ARIA roles) from `@hai3/uikit`'s primitives. They **MUST NOT** override or strip those characteristics. Composite-specific ARIA attributes are added on top, not in replacement.

### 6.2 NFR Exclusions

The following non-functional concerns are out of scope: server-side rendering, RTL layout testing beyond what HAI3 ships, animation choreography beyond what underlying Radix primitives provide, performance benchmarking of charts beyond what `recharts` already optimizes for.

## 7. Public Library Interfaces

### 7.1 Public API Surface

This library is internal to the Insight frontend monorepo and is not published as an npm package. Its consumers are limited to Insight screens. The "public surface" is the set of named exports from `src/screensets/insight/uikit/composite/*.tsx` and `src/screensets/insight/uikit/base/*.tsx`.

### 7.2 External Integration Contracts

#### HAI3 UIKit Contract

- [ ] `p1` - **ID**: `cpt-uikit-contract-hai3-uikit`

Composites depend on the `@hai3/uikit` package providing the following primitives at the imported names: `Badge`, `Card`, `CardContent`, `Button`, `Popover`, `PopoverContent`, `PopoverTrigger`, `Calendar`, `ToggleGroup`, `ToggleGroupItem`, `Collapsible`, `CollapsibleContent`, `CollapsibleTrigger`, `Dialog` (and its sub-components), `Table` (and its sub-components), and chart wrappers consumed by `LocStackedBar` / `DeliveryTrends`. A breaking rename or removal upstream **MUST** be reconciled in this layer; downstream screens stay insulated.

#### Recharts Tooltip / Legend Contract

- [ ] `p2` - **ID**: `cpt-uikit-contract-recharts-direct`

`LocStackedBar` and `DeliveryTrends` import `Tooltip` and `Legend` directly from `recharts` for cases not yet covered by HAI3 wrappers. When HAI3 ships equivalent wrappers, these direct imports **SHOULD** be migrated.

## 8. Use Cases

### UC-001 Screen Renders a Bullet Section with Mixed Honest-Null Bullets

**ID**: `cpt-uikit-usecase-mixed-null-bullets`

**Actors**: `cpt-uikit-actor-consuming-screen`

**Preconditions**: A consuming screen has loaded a section of bullet metrics where some carry finite values and others are null (source connector not configured for the subject).

**Flow**:

1. Screen passes the array of bullets to `MetricCard`.
2. `MetricCard` renders one `BulletChart` per bullet.
3. Bullets with finite values render with threshold-driven coloring.
4. Bullets with null values render as `ComingSoon` of variant `row`.
5. The card itself remains expanded and interactive — partial-null does not collapse the section.

**Postconditions**: Viewer sees the bullets that loaded, plus honest placeholders for the ones that did not, side by side without ambiguity about cause.

### UC-002 Screen Recovers From a Per-Section Backend Failure

**ID**: `cpt-uikit-usecase-section-error-retry`

**Actors**: `cpt-uikit-actor-consuming-screen`

**Preconditions**: Consuming screen detected a failed load for one section (e.g., Bitbucket connector throwing 5xx), recorded it in `erroredSections`, and passed `errored={true}` plus `onRetry` to that section's `MetricCard`.

**Flow**:

1. `MetricCard` renders `ComingSoon` of variant `card`, state `error`, with a Retry button wired to `onRetry`.
2. Sibling cards remain unaffected.
3. User clicks Retry.
4. `MetricCard` invokes `onRetry`; consuming screen re-fires its load action.
5. On success, `errored` flips to `false`; card re-renders bullets.

**Postconditions**: User recovered the failed section without a full-page reload; sibling sections were never disturbed.

## 9. Acceptance Criteria

- [ ] No composite imports from `src/app/`, slices, actions, effects, or `apiRegistry`.
- [ ] Every composite consuming numeric props renders `ComingSoon` for null values and never `0`.
- [ ] `ComingSoon` is the only empty/error placeholder; no other composite re-implements that surface.
- [ ] All visual styling is achieved through Tailwind utility classes; no inline `style` for static values; no custom CSS files in `uikit/`.
- [ ] Composites build on HAI3 primitives — the same primitive is never re-implemented inside this layer.
- [ ] `MetricCard` `errored` state isolates failures to a single card; sibling cards on the same screen continue rendering.
- [ ] `DrillModal` traps focus and dismisses on escape (inherited from HAI3 `Dialog`).
- [ ] `PeriodSelectorBar` and `ViewModeToggle` are fully controlled — they hold no application state and exclusively use props + callbacks.

## 10. Dependencies

| Dependency | Description | Criticality |
|------------|-------------|-------------|
| `@hai3/uikit` | Primitives consumed by every composite | p1 |
| `@hai3/react` | Hook helpers (consumed indirectly via screens; minimally referenced here) | p2 |
| `recharts` | Direct `Tooltip` / `Legend` imports in trend chart composites | p2 |
| `react-day-picker` | `DateRange` type used by `PeriodSelectorBar` | p2 |
| `@iconify/react` | Icon rendering inside `ComingSoon` and other composites | p2 |
| Tailwind CSS | Sole styling mechanism for the layer | p1 |
| `src/screensets/insight/types` | Shared domain types (`BulletMetric`, `ViewMode`, `PeriodValue`, `CustomRange`, etc.) | p1 |

## 11. Assumptions

- `@hai3/uikit` ships and remains compatible with the React 18+ runtime used by Insight.
- HAI3 primitives are stable enough that breaking renames are rare and announced; this PRD does not pin a specific HAI3 version.
- Tailwind classes used by composites resolve against the Insight Tailwind config; no per-composite custom theme.
- Screen-level state (period, view mode, drill open/close, errored sections) is owned by Redux slices and flows into composites only through props.

## 12. Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| A future composite quietly imports a slice or dispatches an action | The stateless invariant breaks; the composite stops being reusable in isolation | `cpt-uikit-fr-stateless` is the contract; reviewers reject PRs that import from `slices/`, `actions/`, `effects/`, or `apiRegistry` inside a composite |
| HAI3 ships a breaking rename of a primitive used by composites | All consuming screens break simultaneously | Composites are the single isolation layer; reconciling the rename happens here once, not in every screen |
| A new composite duplicates a HAI3 primitive (e.g., reimplements a button) | Two sources of truth diverge over time | `cpt-uikit-nfr-hai3-primitive-reuse` rejects duplication; the right move is to consume HAI3 directly or push the gap upstream |
| Screens bypass the `ComingSoon` contract and render their own empty placeholder | Empty vs. error states become inconsistent across the product | Acceptance criterion explicitly forbids this; reviewers route empty/error rendering through `ComingSoon` |
| Inline styles or custom CSS sneak in for "just this one thing" | The Tailwind-only invariant erodes; component theming becomes inconsistent | `cpt-uikit-nfr-tailwind-only` explicitly carves out the only acceptable exception (dynamically computed numeric style values) |

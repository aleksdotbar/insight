# Spec: executive-view

**Capability:** `executive-view`
**Change:** `add-initial-insight-screens`
**Status:** Draft

## Overview

`executive-view` is an org-level engineering dashboard. It shows four KPI summary cards, a radar chart of org health, a grouped bar chart of key metrics by team, and a teams overview table with per-row status badges. All data is sourced from `InsightApiService` and reflects the globally active period.

**Access:** `executive` role only. The menu item is only added to the sidebar when `buildMenu` resolves role to `executive`. `team_lead` and `ic` roles do not see this screen in their menu.

### Screen header

The screen header row is rendered inside the screen (no app-shell header):
- **Left**: "Executive View" title (`15px bold`) + "All teams · Organization overview" subtitle (`10px muted`).
- **Right**: `PeriodSelectorBar` (period tabs + optional custom range picker).

### Org hierarchy in sidebar

For the `executive` role, the sidebar lists all teams from `ORG` as nested menu groups. Each group contains:
- The team lead (with `· Lead` suffix and `lucide:user-check` icon).
- All regular team members (with `lucide:user` icon).
- Any sub-teams as nested groups (recursive, arbitrary depth).

Clicking a team member menu item sets `selectedPersonId` and navigates to `IC_DASHBOARD_SCREEN_ID`.

---

## ADDED Requirements

### Requirement: Org KPI Cards

The screen SHALL display four summary cards at the top of the page:

| Card | Value | Green threshold | Amber threshold |
|---|---|---|---|
| Teams at Risk | Count of teams with `status === 'warn'` or `status === 'bad'` | 0 | > 0 |
| Avg Build Success | Mean `build_success_pct` across all teams, rounded to nearest integer, displayed as `N%` | ≥ 90% | < 90% |
| Avg AI Adoption | Mean `ai_adoption_pct` across all teams, rounded to nearest integer, displayed as `N%` | ≥ 60% | < 60% |
| Avg Focus Time | Mean `focus_time_pct` across all teams, rounded to nearest integer, displayed as `N%` | ≥ 60% | < 60% |

Each card MUST apply `text-insight-green` when the green threshold is met and `text-insight-amber` otherwise. No red state exists for KPI cards — amber is the worst state.

The card grid MUST use 2 columns on mobile and 4 columns on desktop (≥ `sm` breakpoint).

#### Scenario: All teams healthy — Teams at Risk shows zero in green

Given the API returns team data where every team has `status === 'good'`,
when the screen renders,
then the "Teams at Risk" card SHALL display `0` with `text-insight-green` styling.

#### Scenario: Some teams at risk — card shows warn count in amber

Given the API returns 6 teams where 2 have `status === 'warn'` and 1 has `status === 'bad'`,
when the screen renders,
then the "Teams at Risk" card SHALL display `3` with `text-insight-amber` styling.

#### Scenario: Avg Build Success at threshold — green color applied

Given the API returns teams whose mean `build_success_pct` is exactly `90`,
when the screen renders,
then the "Avg Build Success" card SHALL display `90%` with `text-insight-green` styling.

#### Scenario: Avg Build Success below threshold — amber color applied

Given the API returns teams whose mean `build_success_pct` rounds to `87`,
when the screen renders,
then the "Avg Build Success" card SHALL display `87%` with `text-insight-amber` styling.

#### Scenario: Avg AI Adoption at threshold — green color applied

Given the API returns teams whose mean `ai_adoption_pct` is exactly `60`,
when the screen renders,
then the "Avg AI Adoption" card SHALL display `60%` with `text-insight-green` styling.

#### Scenario: Avg Focus Time below threshold — amber color applied

Given the API returns teams whose mean `focus_time_pct` rounds to `55`,
when the screen renders,
then the "Avg Focus Time" card SHALL display `55%` with `text-insight-amber` styling.

#### Scenario: Period change updates KPI card values

Given the active period is `month` and the screen shows KPI values computed from monthly data,
when the user switches the period to `quarter`,
then the screen SHALL request data for the `quarter` period and recalculate all four KPI card values to reflect quarterly averages.

---

### Requirement: Radar Chart — Org Health

The screen SHALL display a radar chart titled "Team Health Overview" with exactly five axes:

| Axis label | Source value |
|---|---|
| Build Success | `avgBuildSuccess` (mean `build_success_pct`, 0–100 scale) |
| AI Adoption | `avgAiAdoption` (mean `ai_adoption_pct`, 0–100 scale) |
| Focus Time | `avgFocus` (mean `focus_time_pct`, 0–100 scale) |
| Bug Resolution | provided by API as a pre-aggregated org metric |
| PR Cycle | provided by API as a pre-aggregated org metric (0–100 normalized scale) |

The chart MUST use a single `Radar` series named "Org Average" with stroke `#2563EB`, fill `#2563EB` at 15% opacity.

Bug Resolution and PR Cycle values MUST come from `InsightApiService`, not be hardcoded constants. (The prototype hardcoded `78` and `65` respectively — this MUST NOT be reproduced in production.)

#### Scenario: Radar renders five axes with org-average data

Given the API returns org-level data including `bugResolutionScore` and `prCycleScore` alongside the team list,
when the screen renders,
then the radar chart SHALL display five polygon vertices, one per axis, at positions corresponding to the org averages for each metric.

#### Scenario: Period change updates radar data

Given the active period is `month`,
when the user switches the period to `quarter`,
then the radar chart SHALL re-request org data for `quarter` and redraw all five axis values.

#### Scenario: All axes at maximum — radar fills the polygon

Given all five org metrics equal `100`,
when the screen renders,
then the radar polygon SHALL occupy the full area of the chart grid.

#### Scenario: Bug Resolution and PR Cycle not hardcoded

Given the `InsightApiService` mock returns `bugResolutionScore: 50` and `prCycleScore: 40`,
when the screen renders,
then the Bug Resolution axis SHALL show `50` and the PR Cycle axis SHALL show `40`, not the prototype constants `78` and `65`.

---

### Requirement: Bar Chart — Key Metrics by Team

The screen SHALL display a grouped bar chart titled "Key Metrics by Team" showing three series per team:

| Series label | Source field | Bar color |
|---|---|---|
| Build % | `build_success_pct` | `#2563EB` |
| AI Adoption % | `ai_adoption_pct` | `#7C3AED` |
| Focus % | `focus_time_pct` | `#16A34A` |

The x-axis MUST use `team_name` as the category label. The y-axis domain MUST be `[0, 100]`. One bar group per team, with each group containing all three series side by side.

#### Scenario: Bar chart renders one group per team

Given the API returns 6 teams,
when the screen renders,
then the bar chart SHALL display exactly 6 groups on the x-axis, each with 3 bars.

#### Scenario: Bar heights correspond to metric values

Given a team named "Platform" has `build_success_pct: 89`, `ai_adoption_pct: 58`, `focus_time_pct: 66`,
when the screen renders,
then the Build %, AI Adoption %, and Focus % bars for Platform SHALL have proportional heights of 89, 58, and 66 on the 0–100 scale respectively.

#### Scenario: Period change updates bar chart data

Given the active period is `month`,
when the user switches the period to `week`,
then the bar chart SHALL re-fetch team data for `week` and redraw bars with the new period's values.

#### Scenario: Empty team list renders empty chart

Given the API returns an empty team list,
when the screen renders,
then the bar chart SHALL render with no bar groups and no x-axis labels, without throwing an error.

---

### Requirement: Teams Overview Table

The screen SHALL display a "Teams Overview" table with one row per team, containing the following columns in order:

| Column header | Source field | Unit | Color rule |
|---|---|---|---|
| Team | `team_name` | — | — |
| Headcount | `headcount` | count | muted |
| Tasks Closed | `tasks_closed` | count | normal |
| Bugs Fixed | `bugs_fixed` | count | normal |
| Build % | `build_success_pct` | % | ≥ 90 → `text-insight-green`; < 90 → `text-insight-amber` |
| Focus % | `focus_time_pct` | % | ≥ 60 → `text-insight-green`; < 60 → `text-insight-amber` |
| AI Adoption | `ai_adoption_pct` | % | ≥ 60 → `text-insight-green`; < 60 → `text-insight-amber` |
| AI LOC % | `ai_loc_share_pct` | % | muted |
| PR Cycle | `pr_cycle_time_h` | h (suffix) | muted |
| Status | `status` | badge | `good` / `warn` / `bad` badge styles |

The Status column MUST render a `Badge` component with variant `outline` styled per:

| Status value | Badge class |
|---|---|
| `good` | `bg-insight-green-bg text-insight-green border-insight-green/20` |
| `warn` | `bg-insight-amber-bg text-insight-amber border-insight-amber/20` |
| `bad` | `bg-insight-red-bg text-insight-red border-insight-red/20` |

If `status` contains an unrecognised value, the `good` style SHALL be used as the fallback.

PR Cycle values MUST be rendered as `{value}h` (e.g. `"12h"`).

#### Scenario: Table renders all columns and rows

Given the API returns 6 teams,
when the screen renders,
then the table SHALL display 6 rows, each containing all 10 columns with correct values.

#### Scenario: Build % threshold coloring

Given a team has `build_success_pct: 89`,
when the table renders,
then the Build % cell for that team SHALL display `89%` with `text-insight-amber`.

Given a team has `build_success_pct: 91`,
when the table renders,
then the Build % cell for that team SHALL display `91%` with `text-insight-green`.

#### Scenario: Boundary value Build % = 90 is green

Given a team has `build_success_pct: 90`,
when the table renders,
then the Build % cell SHALL display `90%` with `text-insight-green`.

#### Scenario: AI Adoption and Focus % threshold coloring

Given a team has `ai_adoption_pct: 59` and another team has `ai_adoption_pct: 60`,
when the table renders,
then the first team's AI Adoption cell SHALL use `text-insight-amber` and the second SHALL use `text-insight-green`.

Given a team has `focus_time_pct: 59` and another team has `focus_time_pct: 60`,
when the table renders,
then the first team's Focus % cell SHALL use `text-insight-amber` and the second SHALL use `text-insight-green`.

#### Scenario: Status badge renders correct style per status value

Given teams with `status: 'good'`, `status: 'warn'`, and `status: 'bad'`,
when the table renders,
then each Status cell SHALL render a Badge with the matching style class from the STATUS_BADGE map.

#### Scenario: PR Cycle value rendered with h suffix

Given a team has `pr_cycle_time_h: 12`,
when the table renders,
then the PR Cycle cell SHALL display `12h`.

#### Scenario: Period change updates count columns

Given the active period is `month` and a team shows `tasks_closed: 76`,
when the user switches the period to `quarter`,
then the screen SHALL re-fetch data for `quarter` and the team's Tasks Closed and Bugs Fixed values SHALL update to reflect the quarterly period (approximately 3× the monthly values per the API mock).

#### Scenario: Rate columns do not change on period switch

Given the active period is `month` and a team shows `build_success_pct: 91`,
when the user switches the period to `quarter`,
then the Build %, Focus %, and AI Adoption % values for that team SHALL remain stable (rate metrics are not scaled by period).

---

### Requirement: Period Awareness

All data displayed on the Executive View — KPI cards, radar chart, bar chart, and teams table — MUST reflect the globally active period. The screen MUST NOT display static data independent of the period selector.

The screen MUST call `InsightApiService.getExecutiveViewData(period)` (or equivalent) passing the active period on every period change. The prototype's pattern of reading directly from a static CSV file MUST NOT be reproduced.

When the period changes via the NavBar global period selector, the screen MUST re-fetch all data without requiring a page reload.

#### Scenario: Screen loads with default period (month)

Given the app launches with the default period set to `month`,
when the Executive View screen mounts,
then the screen SHALL call the API with `period: 'month'` and display data for the month period.

#### Scenario: Navigating to screen after period was changed

Given the user changed the period to `quarter` on the IC Dashboard and then navigated to Executive View,
when the Executive View screen mounts,
then the screen SHALL call the API with `period: 'quarter'` and display quarterly data immediately — it SHALL NOT default back to `month`.

#### Scenario: Period selector triggers full data refresh

Given the user is on Executive View with `period: 'month'`,
when the user clicks the "Quarter" tab in the NavBar period selector,
then all four KPI cards, the radar chart, the bar chart, and every table row SHALL update to reflect quarterly data.

#### Scenario: Custom date range uses month granularity

Given the user selects a custom date range,
when Executive View re-fetches data,
then the screen SHALL pass `period: 'month'` (custom maps to month granularity) to the API.

---

### Requirement: Data Loading via InsightApiService

All data on the Executive View MUST be loaded from `InsightApiService`, not from CSV files or any other static assets. The prototype used `useCsv('/data/executive_kpis.csv')` — this pattern MUST NOT appear in the production screen.

The service MUST provide mock data that mirrors the CSV fixture (6 teams: Platform, Mobile, Data, Frontend, DevOps, QA) for the `month` period. Count metrics for other periods MUST be scaled according to the period scaling rules (week ÷ 4, quarter × 3, year × 12).

While data is loading, the teams table area MUST display a loading state. KPI cards and charts MAY show skeleton or empty states during loading.

#### Scenario: Teams table shows loading state while fetching

Given the `InsightApiService` call is pending,
when the teams table area renders,
then it SHALL display a loading indicator (e.g. "Loading..." text or skeleton rows) instead of table rows.

#### Scenario: Data renders after service resolves

Given the `InsightApiService` resolves with 6 teams,
when the promise resolves,
then the loading state SHALL be replaced by the full table with all 6 rows.

#### Scenario: No CSV imports in production screen

The `ExecutiveView` screen component and its child components SHALL NOT import from any `.csv` file path. All data MUST flow through `InsightApiService`.

#### Scenario: Mock data matches prototype fixture for month period

Given the active period is `month` and the `InsightApiService` mock is used,
when the screen loads,
then the teams table SHALL contain rows for Platform (12 headcount), Mobile (8), Data (6), Frontend (10), DevOps (5), and QA (7) with values matching the `executive_kpis.csv` fixture.

---

### Requirement: Responsive Layout

The Executive View layout MUST adapt to viewport size as follows:

- **KPI cards grid**: 2 columns on mobile (default), 4 columns at `sm` breakpoint and above (`grid-cols-2 sm:grid-cols-4`).
- **Charts section**: single column (stacked) on mobile, 2-column side-by-side at `lg` breakpoint and above (`grid-cols-1 lg:grid-cols-2`).
- **Teams table**: full-width card, horizontally scrollable on mobile if content overflows.

#### Scenario: KPI cards render 2 columns on mobile viewport

Given the viewport width is below the `sm` breakpoint (< 640px),
when the screen renders,
then the four KPI cards SHALL be arranged in a 2-column grid (2 rows of 2).

#### Scenario: KPI cards render 4 columns on desktop viewport

Given the viewport width is at or above the `sm` breakpoint (≥ 640px),
when the screen renders,
then the four KPI cards SHALL be arranged in a single row of 4 columns.

#### Scenario: Charts render stacked on mobile viewport

Given the viewport width is below the `lg` breakpoint (< 1024px),
when the screen renders,
then the radar chart and bar chart SHALL be stacked vertically, each occupying full width.

#### Scenario: Charts render side by side on desktop viewport

Given the viewport width is at or above the `lg` breakpoint (≥ 1024px),
when the screen renders,
then the radar chart and bar chart SHALL appear side by side in a 2-column grid.

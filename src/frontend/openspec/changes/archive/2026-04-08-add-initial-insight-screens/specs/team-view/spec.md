# Spec: team-view

## Capability

`team-view` — Team manager dashboard. Displays a hero KPI strip, an attention-needed alert section, a team members table with per-member metrics, and collapsible bullet chart sections covering task delivery, code quality, estimation accuracy, AI adoption, and collaboration. All aggregate metrics compare the team median against the company median.

**Screen ID:** `TEAM_VIEW_SCREEN_ID`
**Access:** `team_lead` and `executive` roles. `ic` role has no access (menu does not show Team View for `ic`).

### Screen header

The screen header row is rendered inside the screen, merging team identity and controls into one row:
- **Left**: team avatar icon (initials of `teamName`, indigo bg) + team name (`TeamViewData.teamName`) + subtitle "Team Dashboard".
- **Right**: `PeriodSelectorBar` + `ViewModeToggle` (chart / tile).

There is no separate HAI3 app-shell header; it was removed from `Layout.tsx`.

### View mode

`viewMode` (`'chart' | 'tile'`) is `useState` local to `TeamViewScreen` and passed as a prop to `TeamBulletSections`. Toggling it switches bullet charts between full chart mode and compact tile mode. The toggle does not affect `MembersTable`.

### Drill modals

`TeamViewScreen` manages drill state locally via `useState` (not Redux). Two drill entry points:
- **Cell drill** (`handleCellDrill(personId, drillId)`): opens a drill modal for a specific metric of a specific team member; calls `InsightApiService.getIcDrillData(personId, drillId)`.
- **Members drill** (`handleMembersDrill()`): opens a drill modal for the team-members overview; calls `InsightApiService.getTeamDrillData('team-members', period)`.

`DrillModal` is rendered inside `TeamViewScreen` with `open`/`drill`/`onClose` props.

### Role-based member filtering

When the current user's role is `team_lead`, the screen SHALL **exclude the current user's own person** from the `TeamMember[]` list before rendering. The team lead has a dedicated "My Dashboard" screen and does not need to see themselves in Team View.

When the role is `executive`, all members are shown without filtering.

---

## Data Sources

| Shape | Source | Notes |
|---|---|---|
| `TeamKpi[]` | `InsightApiService.getTeamKpis(period)` | Fields: `metric_key`, `label`, `value`, `unit`, `status`, `section` |
| `TeamMember[]` | `InsightApiService.getTeamMembers(period)` | Fields: `period`, `person_id`, `name`, `seniority`, `tasks_closed`, `bugs_fixed`, `dev_time_h`, `prs_merged`, `build_success_pct`, `focus_time_pct`, `ai_tools`, `ai_loc_share_pct` |
| `BulletSection[]` | `InsightApiService.getTeamBulletSections(period)` | Pre-computed per-section bullet metrics for Team View |

---

## ADDED Requirements

### Requirement: Hero KPI Strip

The Team View SHALL render a hero KPI strip containing exactly four metric cards derived from the `TeamKpi` API response for the active period: `at_risk_count` (At Risk), `team_dev_time` (Team Dev Time), `focus_gte_60` (Focus ≥ 60%), and `not_using_ai` (Not Using AI Tools).

Each card SHALL display: a primary value, a label, a sublabel (source description), and a status badge. Badge background and text colour SHALL follow the status colour mapping: `good` → green (`#16A34A` / `#DCFCE7`), `warn` → amber (`#D97706` / `#FEF3C7`), `bad` → red (`#DC2626` / `#FEE2E2`).

On desktop the strip SHALL be a single-row 4-column grid. On mobile it SHALL reflow to a 2×2 grid. Internal cell separators (vertical lines, `#E2E6EF`) SHALL be omitted at the start of each row (`i % 2 === 0` on mobile, `i === 0` on desktop).

All four values SHALL reflect the active period returned by `PeriodContext`. Count metrics (`at_risk_count`, `not_using_ai`) SHALL scale with period. Rate/fraction metrics (`focus_gte_60` expressed as `x/12`) SHALL represent the count of members meeting the threshold during the active period.

#### Scenario: Strip renders with API data

Given the API returns `at_risk_count=2`, `team_dev_time=14h`, `focus_gte_60=9/12`, `not_using_ai=5` for period `month`,
When Team View loads with period set to `month`,
Then the strip SHALL display all four cards with values `2`, `14h`, `9 / 12`, `5` respectively,
And each card SHALL show its status badge with the colour matching the `status` field from the API response.

#### Scenario: Period switch updates KPI values

Given Team View is displayed with period `month`,
When the user switches the global period selector to `quarter`,
Then the component SHALL call `getTeamKpis('quarter')`,
And the hero strip SHALL re-render with values returned for the `quarter` period,
And count metrics SHALL reflect the quarterly scale (approximately 3× the monthly baseline).

#### Scenario: Empty / loading state

Given the API call for team KPIs is in flight,
When the hero strip container renders,
Then each of the four card slots SHALL display a loading skeleton or blank state,
And no stale values from a previous period SHALL remain visible.

#### Scenario: Mobile layout reflows to 2×2 grid

Given the viewport width is below the mobile breakpoint (`isMobile === true`),
When Team View renders,
Then the KPI strip grid SHALL use `gridTemplateColumns: '1fr 1fr'`,
And cells at index 2 and 3 SHALL have a top border (`1px solid #E2E6EF`),
And cells at odd indices SHALL have a left border.

---

### Requirement: Attention Needed Section

The Team View SHALL render an "Attention Needed" card that surfaces team members whose metrics fall below defined thresholds. The following rules SHALL be evaluated per member using data from the active-period `TeamMember` response:

- `build_success_pct < 90` → alert reason: "Build success rate below target"
- `focus_time_pct < 60` → alert reason: "Focus time below 60% target"
- `ai_loc_share_pct < 10` → alert reason: "Low AI tool adoption"

A member may generate multiple alerts (one per failing metric). Each alert entry SHALL display: the member name and primary failing metric value in the title, a descriptive sentence in the body, a severity badge, and a "→ Open IC dashboard" link.

Severity SHALL be determined as follows:
- `bad` (red) — the metric value is more than 20% below the threshold (e.g. `focus_time_pct < 48` where threshold is 60; `build_success_pct < 72` where threshold is 90; `ai_loc_share_pct < 8` where threshold is 10).
- `warn` (amber) — the metric value is at or below the threshold but within 20% of it.

If no members fail any threshold, the Attention Needed section SHALL be hidden or SHALL display an empty state message ("All members are within acceptable ranges").

#### Scenario: Members below threshold appear with correct severity

Given the active-period member list includes Ben Clarke with `focus_time_pct=48` and `build_success_pct=72`,
When the Attention Needed section is evaluated,
Then Ben Clarke SHALL appear with at least two alert entries (one for focus time, one for build success),
And each entry for Ben Clarke SHALL carry a `bad` (red) severity badge because `48 < 48` (focus: 60 × 0.8 = 48, boundary is strict less than) and `72 < 72` (build: 90 × 0.8 = 72, boundary is strict less than),
And entries for members within 20% of the threshold (e.g. `focus_time_pct=55`) SHALL carry a `warn` (amber) badge.

#### Scenario: Alert link navigates to IC dashboard

Given an alert entry for member with `person_id=p4` is visible,
When the user clicks "→ Open IC dashboard" on that entry,
Then the screen SHALL dispatch `selectIcPerson('p4')` and call `navigateToScreen(screensetId, IC_DASHBOARD_SCREEN_ID)`.

#### Scenario: No members below threshold

Given all team members have `build_success_pct ≥ 90`, `focus_time_pct ≥ 60`, and `ai_loc_share_pct ≥ 10`,
When the Attention Needed section renders,
Then no alert entries SHALL be shown,
And the section SHALL either be hidden entirely or display a neutral confirmation message indicating all members meet the thresholds.

#### Scenario: Period switch re-evaluates alerts

Given Tom Sullivan has `focus_time_pct=55` (warn) in period `month`,
And Tom Sullivan's `focus_time_pct` in period `week` is `58` (still below 60),
When the user switches the global period to `week`,
Then the Attention Needed section SHALL re-evaluate using the `week` data,
And Tom Sullivan's alert entry SHALL remain visible with `warn` severity if `focus_time_pct=58`.

---

### Requirement: Members Table

The Team View SHALL render a "Team Members" card containing a data table with one row per team member returned by the active-period API response. The table SHALL include the following columns in order:

| Column | Field | Display |
|---|---|---|
| Name | `name` + `seniority` | Name in bold (12px), seniority below in muted text (10px) |
| Tasks | `tasks_closed` | Plain count |
| Bugs Fixed | `bugs_fixed` | Count, color-coded: `hi(bugs_fixed, 15, 8)` — ≥15 green, ≥8 amber, <8 red |
| Dev Time | `dev_time_h` | Value + `h` suffix; color-coded: `lo(dev_time_h, 14, 20)` — ≤14 green, ≤20 amber, >20 red |
| Pull Requests | `prs_merged` | Plain count |
| Build Success | `build_success_pct` | Percentage; color-coded: `hi(build_success_pct, 90, 80)` — ≥90 green, ≥80 amber, <80 red |
| Focus Time | `focus_time_pct` | Mini progress bar + percentage; bar fill and text color: `hi(focus_time_pct, 60, 50)` — ≥60 green, ≥50 amber, <50 red |
| AI Tools | `ai_tools` | Comma-separated tool names rendered as outline badges; `"—"` values excluded |
| AI LOC Share | `ai_loc_share_pct` | Percentage; color-coded: `hi(ai_loc_share_pct, 20, 10)` — ≥20 green, ≥10 amber, <10 red. Zero value shown as muted `0%` |

The entire row SHALL be clickable (cursor pointer, hover background `blue-50/40`). Clicking any row SHALL dispatch `selectIcPerson(person_id)` then call `navigateToScreen(screensetId, IC_DASHBOARD_SCREEN_ID)`.

A subheader hint "Click member to open IC dashboard" SHALL be shown at the top right of the card header.

#### Scenario: Table renders all period members

Given the API returns 12 members for period `month`,
When the Team Members table renders,
Then the table SHALL contain exactly 12 rows,
And each row SHALL display the member's `name`, `seniority`, and all numeric metrics for the `month` period.

#### Scenario: Row click navigates to IC dashboard

Given the members table is rendered with member Konrad Zuse (person_id=1),
When the user clicks anywhere on Konrad Zuse's row,
Then the router SHALL navigate to `/ic/1`.

#### Scenario: Color coding applied per thresholds

Given a member with `build_success_pct=72`, `focus_time_pct=48`, `ai_loc_share_pct=6`,
When the table renders that member's row,
Then the build success cell SHALL be red (`#DC2626`),
And the focus time bar and percentage SHALL be red (`#DC2626`),
And the AI LOC share cell SHALL be red (`#DC2626`).

#### Scenario: AI tools column renders badges

Given a member with `ai_tools="Cursor,CC"`,
When the table renders that member's row,
Then the AI Tools cell SHALL contain two outline badges: one labelled "Cursor" and one labelled "CC".

#### Scenario: AI tools column renders empty state

Given a member with `ai_tools="—"`,
When the table renders that member's row,
Then the AI Tools cell SHALL display a muted dash character and no badges.

#### Scenario: Period switch updates count columns

Given the members table shows `tasks_closed=5` for a member in period `month`,
When the user switches the global period to `quarter`,
Then the API SHALL be called with period `quarter`,
And the member's `tasks_closed` cell SHALL update to reflect the quarterly value (approximately 15).

#### Scenario: Mobile horizontal scroll

Given the viewport width is below the mobile breakpoint (`isMobile === true`),
When the Members table renders,
Then the card content container SHALL have `overflowX: 'auto'`,
And the table SHALL have `minWidth: 640px` to prevent column collapsing,
And the user SHALL be able to scroll horizontally to view all columns.

#### Scenario: Loading state

Given the API call for team members is in flight,
When the members table container renders,
Then a loading indicator SHALL be displayed in place of the table rows,
And no partial or stale row data SHALL be shown.

---

### Requirement: Bullet Chart Sections

The Team View SHALL render five collapsible sections, each containing one or more `BulletChart` components: **Task Delivery**, **Code & Quality**, **Estimation**, **AI Adoption**, and **Collaboration**.

Each section SHALL be implemented as a `CollapsibleSection` with `defaultOpen=false` (collapsed on initial load). The user SHALL be able to expand or collapse each section independently.

Each `BulletChart` SHALL display the team median value as the primary bar against a track showing the company-wide range. The median marker line SHALL be labelled **"Company median"** (not "Team median") in all bullet chart legends on Team View. This distinction is normative: Team View compares the team aggregate to the wider organisation.

Each section SHALL include a `Legend` component rendered once per section (or per subsection where applicable), showing: a narrow blue line swatch labelled "Company median" and a blue bar swatch labelled "Team".

Sections and their metrics SHALL be as follows:

**Task Delivery** — one card with three bullet rows:
- Tasks closed / developer (Jira · closed issues in sprint; range 0–15)
- Task Development Time (Jira · time in In Progress · lower is better; range 0h–48h)
- Task Reopen Rate (Jira · closed then reopened within 14 days · lower is better; range 0%–50%)

**Code & Quality** — one card with three bullet rows:
- Pull Requests merged / developer (Bitbucket · authored and merged; range 0–20)
- Build Success Rate (CI · passed ÷ total runs · target ≥90%; range 0%–100%)
- Pull Request Cycle Time (Bitbucket · PR opened → merged · lower is better; range 0h–72h)

On desktop, Task Delivery and Code & Quality SHALL render side by side in a two-column grid. On mobile they SHALL stack vertically.

**Estimation** — one full-width card with three groups in a three-column grid (one column per group on desktop, stacked on mobile):
- Group 1 · Time estimate accuracy: Within ±20% of estimate; Median overrun ratio
- Group 2 · Sprint scope: Scope Completion Rate; Scope Creep Rate
- Group 3 · Deadline (date-driven): On-time Delivery Rate; Avg Slip When Late

**AI Adoption** — one collapsible section with a two-column grid of bullet rows covering: active members (total and per tool: Cursor, Claude Code, Codex), Team AI LOC Share, Cursor Acceptance Rate, Claude Code Tool Acceptance.

**Collaboration** — one collapsible section with a three-column grid grouped by: Slack (Thread Participation, Message Engagement, DM Ratio), M365 (Teams Messages, Emails Sent, Files Shared), and Meetings (Meeting Hours, Zoom Calls, Meeting-Free Days).

#### Scenario: Sections start collapsed

Given Team View loads for the first time,
When the page renders,
Then all five collapsible sections SHALL be in the collapsed state,
And their content panels SHALL not be visible,
And the trigger rows SHALL display a "Collapsed" badge and a `▾` arrow indicator.

#### Scenario: Expanding a section reveals bullet charts

Given the AI Adoption section is collapsed,
When the user clicks the AI Adoption section trigger row,
Then the content panel SHALL animate into view,
And the Legend and all bullet chart rows SHALL be visible,
And the trigger row SHALL update to show an "Expanded" badge and a `▴` arrow.

#### Scenario: Bullet chart legend label is "Company median"

Given any bullet chart section on Team View is expanded,
When the Legend component renders,
Then the vertical line swatch label SHALL read "Company median",
And the bar swatch label SHALL read "Team",
And the label "Team median" SHALL NOT appear anywhere in bullet chart legends on Team View.

#### Scenario: Median marker position reflects company median

Given the Task Delivery section is expanded and the API returns company median for Task Development Time as 16h with a range of 0h–48h,
When the BulletChart for Task Development Time renders,
Then the median marker line SHALL be positioned at `(16 / 48) * 100 ≈ 33%` of the track width (`median_left_pct`),
And the footer center label SHALL read "Company median: 16h · lower = better".

#### Scenario: Period switch updates bullet chart values

Given the Task Delivery section is expanded and the Tasks closed / developer value is `5.3` for period `month`,
When the user switches the global period to `week`,
Then the API SHALL be called with period `week`,
And the Tasks closed / developer bullet row value SHALL update to reflect the weekly scale.

#### Scenario: Task Delivery and Code & Quality are side by side on desktop

Given the viewport is at desktop width (`isMobile === false`),
When Team View renders,
Then the Task Delivery card and the Code & Quality card SHALL render in a two-column grid (`gridTemplateColumns: '1fr 1fr'`),
And both cards SHALL be visible simultaneously without horizontal scrolling.

#### Scenario: Estimation section has three column groups

Given the Estimation section is expanded,
When the section renders on desktop,
Then its content SHALL use a three-column grid (`gridTemplateColumns: '1fr 1fr 1fr'`),
And each column SHALL display its group label (e.g. "1 · Time estimate accuracy") above its bullet rows.

---

### Requirement: Period Awareness

All data-bearing sections of Team View SHALL respond to the global period state from `PeriodContext`. The active period SHALL be sourced via `usePeriod()`. The period key `custom` SHALL map to `month` for all API calls (identical to how the prototype maps `custom → 'month'`).

Count metrics in the hero strip, members table, and bullet chart sections SHALL all reflect values pre-computed for the active period by the API. Rate and percentage metrics SHALL remain stable across periods (they represent averages, not totals).

Switching periods SHALL trigger a full data refresh on all three API endpoints (`getTeamKpis`, `getTeamMembers`, `getTeamBulletSections`) with the new period parameter. No client-side period scaling SHALL occur — all scaling is server-side.

When navigating to Team View from another screen, the screen SHALL load data for whichever period is currently active in `PeriodContext` — there is no period reset on navigation.

#### Scenario: Custom period maps to month

Given the global period is set to `custom` with a date range selected,
When Team View fetches its data,
Then all three API calls SHALL use `period='month'` as the period key,
And the displayed values SHALL be the monthly baseline values.

#### Scenario: Period persists across navigation

Given the user is on IC Dashboard with period set to `quarter`,
When the user navigates to Team View,
Then Team View SHALL immediately load data with `period='quarter'`,
And the hero strip and members table SHALL display quarterly values without requiring an additional period selection.

#### Scenario: Rate metrics do not change between periods

Given `build_success_pct=89%` and `focus_time_pct=68%` for a member in period `month`,
When the user switches to `quarter`,
Then the API SHALL return the same `build_success_pct` and `focus_time_pct` values (89% and 68% respectively),
And these values SHALL not be multiplied by the quarter scale factor.

---

### Requirement: View Mode Toggle

The NavBar on Team View SHALL include a Charts / Tiles view mode toggle (the `showViewToggle` prop). The active view mode SHALL be sourced from the global view mode context shared with IC Dashboard and Executive View.

When view mode is `chart`, bullet chart sections SHALL render `BulletChart` components using the full track visualization (bar + median line + range footer).

When view mode is `tile`, bullet chart sections SHALL render `BulletChart` components in tile mode: large numeric value, status badge, no track, no range labels.

The members table and hero KPI strip are not affected by the view mode toggle — they always render in their fixed layout regardless of mode.

#### Scenario: Default view mode is chart

Given Team View loads without any prior view mode selection,
When the page renders,
Then the view mode SHALL default to `chart`,
And bullet chart sections SHALL display track visualizations.

#### Scenario: Switching to tile mode

Given Team View is in `chart` mode and the AI Adoption section is expanded,
When the user clicks the Tiles toggle in the NavBar,
Then all expanded bullet chart sections SHALL immediately re-render in tile mode,
And each metric SHALL display as a tile card (large number, status badge, no bar track),
And the Legend component SHALL be hidden in tile mode.

#### Scenario: View mode persists on navigation

Given the user sets view mode to `tile` on IC Dashboard and then navigates to Team View,
When Team View renders,
Then the bullet chart sections on Team View SHALL render in `tile` mode,
And the NavBar toggle SHALL reflect `tile` as the active mode.

---

### Requirement: Responsive Layout

Team View SHALL implement a responsive layout. On mobile (`isMobile === true` via `useBreakpoint()`), the following adaptations SHALL apply:

- Hero KPI strip: 2×2 grid (see Hero KPI Strip requirement).
- Task Delivery and Code & Quality: stacked vertically in a single column.
- Estimation section: single-column stacked groups.
- AI Adoption bullet grid: single column.
- Collaboration bullet grid: single column (Slack, M365, Meetings stacked).
- Members table: horizontal scroll enabled with `minWidth: 640px` on the table element.

On desktop (`isMobile === false`), Task Delivery and Code & Quality render side by side; Estimation renders in three columns; AI Adoption renders in two columns; Collaboration renders in three columns.

Page layout padding, gap, and max-width SHALL follow the shared `PageLayout` spec: desktop `20px 20px 40px`, mobile `12px 12px 32px`; inter-section gap desktop `14px`, mobile `10px`.

#### Scenario: Desktop multi-column layout

Given the viewport is at desktop width,
When Team View renders all sections,
Then Task Delivery and Code & Quality SHALL be in a `1fr 1fr` two-column grid,
And Estimation's three groups SHALL be in a `1fr 1fr 1fr` three-column grid,
And Collaboration's three subsections SHALL be in a `1fr 1fr 1fr` grid.

#### Scenario: Mobile single-column stacking

Given the viewport is at mobile width (`isMobile === true`),
When Team View renders all sections,
Then Task Delivery and Code & Quality SHALL stack vertically with a `10px` gap,
And Estimation's groups SHALL stack vertically,
And Collaboration's subsections SHALL stack vertically,
And the members table SHALL have `overflowX: 'auto'` with `minWidth: 640px`.

# Capability Spec: ic-dashboard

**Capability ID:** `ic-dashboard`
**Screen IDs:** `IC_DASHBOARD_SCREEN_ID` (`'ic-dashboard'`), `MY_DASHBOARD_SCREEN_ID` (`'my-dashboard'`)
**Screenset:** `insight`
**Change:** `add-initial-insight-screens`

---

## Overview

The IC Dashboard is the individual contributor detail view. It is reached by:
- Clicking a team member's row or menu sub-item in Team View / sidebar (loads the selected person via `selectedPersonId` Redux state, navigates to `IC_DASHBOARD_SCREEN_ID`).
- Clicking "My Dashboard" in the menu (navigates to `MY_DASHBOARD_SCREEN_ID`, always loads the current user's own data regardless of `selectedPersonId`).

The page shows a person header inline in a header row (with period selector on the right), a hero KPI strip, always-visible bullet metric card sections (Task Delivery, Git Output, Code Quality), two collapsible charts (LOC Breakdown, Delivery Trends), two collapsible data sections (AI Dev Tools, Collaboration), and a privacy footer. A drill modal opens when the user clicks any bullet metric that carries a `drill_id`.

All data is period-aware. The period selector is rendered inside the screen's header row; changing it triggers a data reload.

### Person ID resolution

The screen determines `personId` as follows:
```ts
const personId = currentScreen === MY_DASHBOARD_SCREEN_ID
  ? currentUser.personId
  : selectedPersonId;  // from icDashboardSlice
```

`selectedPersonId` is set by `selectIcPerson(personId)` action before `navigateToScreen` is called. Menu items for IC Dashboard use the `'ic-dashboard::p2'` compound ID format; `Menu.tsx` emits `layout/menu/itemParam` which triggers the same `setSelectedPersonId` dispatch.

---

## ADDED Requirements

### Requirement: Person Header (inline)

The page SHALL display the person header **inline** in the screen's top header row — left side — alongside the period selector on the right. There is no separate header card; the avatar, name, and role line appear directly in the row.

The header row SHALL render:
- Left: circular avatar (36 × 36 px, background `#EFF6FF`, initials `fontSize 14px fontWeight 800 color #2563EB`) + name line (`fontSize 15px fontWeight 700 color #111827`) + role·seniority line (`fontSize 11px color #6B7280`).
- Right: `PeriodSelectorBar` (period tabs + optional custom range).

When `personId` resolves to no matching person in the API response and `loading` is false, the page SHALL show a "Person not found" empty state in place of all content below the header row.

#### Scenario: Known person renders header correctly

Given the user navigates to `/ic/alice-kim` and the API returns a person record with name "Alice Kim", role "Backend Developer", and seniority "Senior",
when the page loads,
then the avatar circle SHALL display "AK", the name line SHALL display "Alice Kim", and the role line SHALL display "Backend Developer · Senior".

#### Scenario: Role is read from API, not hardcoded

Given the API returns a person record with role "Frontend Engineer",
when the page renders the header,
then the role line SHALL display "Frontend Engineer · {seniority}" — not any hardcoded role string.

#### Scenario: Unknown personId shows not-found state

Given the user navigates to `/ic/unknown-person-999` and the API returns no matching person record,
when the page finishes loading,
then the page SHALL display a "Person not found" message and SHALL NOT render the KPI strip, bullet sections, charts, or footer.

---

### Requirement: KPI Hero Strip

Below the person header, the page SHALL render a KPI hero strip containing exactly five metrics in the following order:

| Position | `metric_key` | Label | Unit |
|---|---|---|---|
| 1 | `bugs_fixed` | Bugs Fixed | _(count — no unit suffix)_ |
| 2 | `clean_loc` | Clean LOC | _(count — no unit suffix)_ |
| 3 | `ai_loc_share` | AI LOC Share | `%` |
| 4 | `spec_lines` | Spec Lines | _(count — no unit suffix)_ |
| 5 | `focus_time_pct` | Focus Time | `%` |

Each KPI SHALL display: label, value, `delta` string (rendered verbatim), and a badge whose colour is controlled by `delta_type` (`good` → green, `warn`/`neutral` → amber, `bad` → red). The UI SHALL NOT generate arrow symbols from `delta_type` — the arrow and comparison text are part of the `delta` string supplied by the API.

Immediately below the KPI strip row, the page SHALL render a time-off notice banner. The banner SHALL display: number of days off, date range (e.g. "Jul 14–18"), a note that metrics reflect active working days, and a clickable "BambooHR" link. The banner background SHALL be `#FFFBEB` with a top border of `#FDE68A` and text colour `#92400E`. The banner SHALL appear even when days-off count is zero (the API controls whether it is shown; the UI renders it if the API includes it in the response).

The KPI strip SHALL reflect the active period. When the period changes, the strip values and deltas SHALL update to the new period's data.

#### Scenario: Five KPI metrics render in correct order

Given the API returns KPI data for the active person and period,
when the page loads,
then the strip SHALL render exactly five tiles in the order: Bugs Fixed, Clean LOC, AI LOC Share, Spec Lines, Focus Time.

#### Scenario: Delta badge colour reflects delta_type

Given a KPI row has `delta_type = "good"` and `delta = "↑ 5 vs Jun"`,
when the strip renders,
then the delta badge SHALL be green and SHALL display the string "↑ 5 vs Jun" verbatim.

#### Scenario: Time-off banner appears below strip

Given the API response includes a time-off notice of 5 days (Jul 14–18),
when the page renders,
then a banner SHALL appear below the KPI row containing "5 days off", "Jul 14–18", and a "BambooHR" link.

#### Scenario: KPI values update on period change

Given the page is displaying Month period data,
when the user switches the global period selector to Quarter,
then the KPI strip SHALL reload and display values and deltas for the Quarter period.

---

### Requirement: Bullet Metric Cards (always visible)

The page SHALL display three bullet metric card sections that are always expanded and NOT collapsible:

1. **Task Delivery** — metrics with `section = "task_delivery"`: `tasks_completed`, `task_dev_time`, `due_date_compliance`, `estimation_accuracy`, `task_reopen_rate`.
2. **Git Output** — metrics with `section = "git_output"`: `commits`, `prs_created`, `prs_merged`, `clean_loc`.
3. **Code Quality** — metrics with `section = "code_quality"`: `reviews_given`, `rework_ratio`, `build_success`, `pr_cycle_time`, `pickup_time`, `bugs_fixed`, `bug_reopen_rate`.

Task Delivery and Git Output SHALL be laid out side-by-side in a two-column grid on non-mobile viewports; on mobile they SHALL stack to a single column. Code Quality SHALL use a three-column grid on non-mobile viewports; on mobile it SHALL collapse to a single column.

In chart mode, each bullet metric SHALL render as a BulletChart with a horizontal track. The legend label "Team median" SHALL be shown above the bullet sections (via the chart-mode legend in each card). In tile mode, each metric renders as a compact tile card.

Each BulletChart instance SHALL render:

- **Header row**: metric label (left) and formatted value (right), with sublabel below the label in the format `"{source} · {definition} · {personName}"`.
- **Track**: `height 24px`, background `#F0F2F7`, `borderRadius 6px`. Contains a value bar starting from the left edge (`bar_left_pct = 0`) with width equal to `bar_width_pct`% of track width and colour from `status` (`good → #16A34A`, `warn → #D97706`, `bad → #DC2626`). A median marker line (`width 2px`, `#2563EB` at 50% opacity, overflowing track by 3px top and bottom) positioned at `median_left_pct`%.
- **Footer**: three labels — `range_min` (left), `median_label` (centre), `range_max` (right), `fontSize 9px`, `color #9CA3AF`.
- If `drill_id` is non-empty, the value in the header SHALL render with `borderBottom: 1px dotted #2563EB` and cursor pointer; clicking it opens the drill modal.
- A period suffix (e.g. `/ mo`) SHALL be appended after the unit for count metrics only. The suffix SHALL be suppressed when the unit is `%`, `×`, `h`, `avg replies`, or `avg`.

#### Scenario: Task Delivery and Git Output are side-by-side on desktop

Given the viewport is non-mobile and view mode is chart,
when the page renders,
then Task Delivery and Git Output cards SHALL appear in a two-column row, with Code Quality spanning full width below them in a three-column internal grid.

#### Scenario: All three sections are always visible without a toggle

Given the page has loaded successfully,
when the user has not interacted with any control,
then Task Delivery, Git Output, and Code Quality sections SHALL all be visible without requiring the user to expand them.

#### Scenario: Bullet bar colour reflects status

Given a metric has `status = "warn"`,
when the BulletChart renders in chart mode,
then the value bar SHALL be amber (`#D97706`) and the header value SHALL be amber.

#### Scenario: Median line position matches median_left_pct

Given a metric has `median_left_pct = 40`,
when the BulletChart track renders,
then the median marker line SHALL be positioned at 40% from the left edge of the track.

#### Scenario: Period suffix shown for count metric, suppressed for percentage

Given the active period is Month and a metric has `unit = ""` (count),
when the BulletChart renders in chart mode,
then `/ mo` SHALL appear after the value. Given a different metric in the same period has `unit = "%"`, no period suffix SHALL appear.

#### Scenario: Drill trigger on value click

Given a bullet metric has a non-empty `drill_id`,
when the user clicks the metric value in chart mode,
then the drill modal SHALL open for that `drill_id`.

#### Scenario: No drill trigger on metric with empty drill_id

Given a bullet metric has `drill_id = ""`,
when the user clicks the metric value,
then no modal SHALL open and no navigation SHALL occur.

#### Scenario: Tile mode renders compact cards without track

Given the view mode toggle is set to Tiles,
when the bullet sections render,
then each metric SHALL display as a compact tile with a large value, label, and status badge — with no horizontal track or range labels.

---

### Requirement: LOC Breakdown Chart (collapsible)

Below the always-visible bullet sections, the page SHALL render a "LOC Breakdown" collapsible section. The section SHALL start collapsed on initial page load. When expanded, the section SHALL display a stacked bar chart with three series:

| Series | Colour |
|---|---|
| AI LOC | Light blue |
| Code LOC | Blue |
| Spec Lines | Purple |

The chart subtitle SHALL read: "Bitbucket · lines added per period · AI-assisted vs manual vs spec/config".

The x-axis granularity SHALL follow the Chart Granularity Logic:

| Active period | x-axis granularity | x-axis labels |
|---|---|---|
| Week | Daily (5 points) | Mon, Tue, Wed, Thu, Fri |
| Month | Weekly (4 points) | Week-start dates (e.g. Jun 2, Jun 9, Jun 16, Jun 23) |
| Quarter | Monthly (3 points) | 3-letter month abbreviations (e.g. Jun, Jul, Aug) |
| Year | Quarterly (2 points) | Quarter labels (e.g. Q2 2026, Q3 2026) |
| Custom | Weekly (4 points) | Same as Month |

When the period is Week, the weekly total for each series SHALL be distributed across 5 weekdays using fixed weights: Mon 15%, Tue 22%, Wed 24%, Thu 21%, Fri 18%. Values SHALL be `Math.round(total * weight)`.

#### Scenario: Section starts collapsed on page load

Given the user navigates to the IC Dashboard,
when the page finishes loading,
then the LOC Breakdown section header SHALL be visible but the chart content SHALL be hidden.

#### Scenario: Section expands on header click

Given the LOC Breakdown section is collapsed,
when the user clicks the section header,
then the chart SHALL become visible displaying the stacked bar chart.

#### Scenario: Week period shows 5 daily bars

Given the LOC Breakdown section is expanded and the active period is Week,
when the chart renders,
then the x-axis SHALL show exactly 5 bars labelled Mon, Tue, Wed, Thu, Fri.

#### Scenario: Month period shows 4 weekly bars

Given the LOC Breakdown section is expanded and the active period is Month,
when the chart renders,
then the x-axis SHALL show exactly 4 bars with week-start date labels.

#### Scenario: Quarter period shows 3 monthly bars

Given the LOC Breakdown section is expanded and the active period is Quarter,
when the chart renders,
then the x-axis SHALL show exactly 3 bars labelled with 3-letter month abbreviations.

#### Scenario: Year period shows 2 quarterly bars

Given the LOC Breakdown section is expanded and the active period is Year,
when the chart renders,
then the x-axis SHALL show exactly 2 bars labelled with quarter identifiers (e.g. Q2 2026, Q3 2026).

#### Scenario: Custom period uses weekly granularity

Given the LOC Breakdown section is expanded and the active period is Custom,
when the chart renders,
then the x-axis SHALL show 4 weekly bars, identical in granularity to the Month period.

#### Scenario: Three series are colour-coded and stacked

Given the section is expanded,
when the chart renders,
then each bar SHALL contain three stacked segments in the correct colours: AI LOC (light blue), Code LOC (blue), Spec Lines (purple).

---

### Requirement: Delivery Trends Chart (collapsible)

Below the LOC Breakdown section, the page SHALL render a "Delivery Trends" collapsible section. The section SHALL start collapsed on initial page load. When expanded, the section SHALL display a multi-line chart with three series:

| Series | Colour |
|---|---|
| Commits | Blue |
| PRs Merged | Purple |
| Tasks Done | Green |

The chart subtitle SHALL read: "Jira + Bitbucket · activity counts · Commits, PRs and Tasks are independent signals — not directly comparable".

The x-axis granularity SHALL follow the same Chart Granularity Logic as the LOC Breakdown Chart (see table in the LOC Breakdown requirement). The same Week daily-distribution weights SHALL apply when the period is Week.

#### Scenario: Section starts collapsed on page load

Given the user navigates to the IC Dashboard,
when the page finishes loading,
then the Delivery Trends section content SHALL be hidden.

#### Scenario: Section expands on header click

Given the Delivery Trends section is collapsed,
when the user clicks the section header,
then the multi-line chart SHALL become visible.

#### Scenario: Three line series render with correct colours

Given the section is expanded and the active period is Month,
when the chart renders,
then three lines SHALL be visible: Commits in blue, PRs Merged in purple, Tasks Done in green.

#### Scenario: Week period shows 5 daily data points

Given the section is expanded and the active period is Week,
when the chart renders,
then the x-axis SHALL show 5 points labelled Mon through Fri, with values derived from weekly totals using the fixed day-weight distribution.

#### Scenario: Period change updates chart granularity

Given the Delivery Trends chart is expanded and the active period is Month,
when the user switches the global period to Quarter,
then the chart x-axis SHALL change to 3 monthly data points without requiring the user to re-expand the section.

---

### Requirement: AI Dev Tools Section (collapsible)

Below the Delivery Trends section, the page SHALL render an "AI Dev Tools & AI Chat" collapsible section. The section SHALL start collapsed on initial page load.

When expanded in chart mode, the section SHALL render a two-column layout:

- **Left column** — headed "CURSOR · {source}" with Cursor metrics (`cursor_completions`, `cursor_agents`, `cursor_lines`), then a sub-heading "CLAUDE CODE · Enterprise Admin API" with Claude Code metrics (`cc_sessions`, `cc_lines`, `cc_tool_accept`).
- **Right column** — headed "AI LOC SHARE · Cursor + Claude Code" with the `ai_loc_share2` metric, then a sub-heading "AI CHAT · Claude Web · ChatGPT" with chat metrics (`claude_web`, `chatgpt`).

Column headings SHALL be rendered in `fontSize 10px`, `fontWeight 700`, `color #6B7280`.

In chart mode, a legend row SHALL appear above the two columns showing "Team median" (with the median line indicator) and "Your result · color = vs target" (with a gradient bar indicator).

When expanded in tile mode, all AI tools metrics SHALL render in a flat grid (no two-column split) at `repeat(3, 1fr)` on non-mobile viewports.

All metrics in this section SHALL use the BulletChart component with the same rendering rules as the always-visible sections.

#### Scenario: Section starts collapsed on page load

Given the user navigates to the IC Dashboard,
when the page finishes loading,
then the AI Dev Tools section content SHALL be hidden.

#### Scenario: Chart mode shows two-column split with sub-headings

Given the AI Dev Tools section is expanded and view mode is chart,
when the layout renders,
then the left column SHALL contain Cursor metrics under "CURSOR" heading and Claude Code metrics under "CLAUDE CODE · Enterprise Admin API" heading; the right column SHALL contain `ai_loc_share2` under "AI LOC SHARE" heading and chat metrics under "AI CHAT · Claude Web · ChatGPT" heading.

#### Scenario: Tile mode shows flat grid, no column split

Given the AI Dev Tools section is expanded and view mode is Tiles,
when the layout renders,
then all AI tools metrics SHALL be displayed in a flat multi-column tile grid with no left/right column separation.

#### Scenario: Legend row appears in chart mode only

Given the section is expanded in chart mode,
when the layout renders,
then a legend row showing "Team median" and "Your result · color = vs target" SHALL be visible above the two-column layout. Given the view mode switches to Tiles, the legend row SHALL NOT be shown.

---

### Requirement: Collaboration Section (collapsible)

Below the AI Dev Tools section, the page SHALL render a "Collaboration" collapsible section. The section SHALL start collapsed on initial page load.

When expanded in chart mode, the section SHALL render a three-column layout. Each column has an uppercase heading and contains BulletChart instances:

| Column | Heading | Metrics (`metric_key` prefix or explicit list) |
|---|---|---|
| Left | SLACK | `slack_threads`, `slack_engage`, `slack_dm` |
| Centre | M365 | `m365_emails`, `m365_teams`, `m365_files` |
| Right | Meetings · M365 · Zoom | `meeting_hours`, `zoom_calls`, `meeting_free` |

Column headings SHALL be `fontSize 11px`, `fontWeight 700`, `color #6B7280`, `textTransform uppercase`, `letterSpacing 0.5`.

When expanded in tile mode, all collaboration metrics SHALL render in a flat grid at `repeat(3, 1fr)` on non-mobile viewports — no three-column sub-division.

On mobile viewports, both chart and tile modes SHALL use a single-column layout.

#### Scenario: Section starts collapsed on page load

Given the user navigates to the IC Dashboard,
when the page finishes loading,
then the Collaboration section content SHALL be hidden.

#### Scenario: Chart mode shows three labelled columns

Given the Collaboration section is expanded and view mode is chart,
when the layout renders,
then three columns SHALL be visible with headings SLACK, M365, and "Meetings · M365 · Zoom", each containing the correct metrics.

#### Scenario: Tile mode shows flat grid

Given the Collaboration section is expanded and view mode is Tiles,
when the layout renders,
then all collaboration metrics SHALL appear in a flat tile grid with no column grouping.

#### Scenario: Mobile collapses to single column in chart mode

Given the viewport is mobile and the Collaboration section is expanded in chart mode,
when the layout renders,
then all metrics SHALL stack in a single column.

---

### Requirement: View Mode Toggle

The NavBar SHALL include a Charts/Tiles toggle control. The selected view mode SHALL apply simultaneously to all bullet sections on the IC Dashboard: Task Delivery, Git Output, Code Quality, AI Dev Tools, and Collaboration.

Switching from chart to tile mode SHALL affect all bullet sections at once. No section-level view toggle SHALL exist.

The toggle state SHALL persist during the user's session (it SHALL NOT reset when the user switches periods or navigates away and returns).

#### Scenario: Toggle switches all sections simultaneously

Given the view mode is chart and all bullet sections are rendered,
when the user clicks the Tiles toggle,
then every bullet section (Task Delivery, Git Output, Code Quality, AI Dev Tools, Collaboration) SHALL switch to tile rendering simultaneously — not one at a time.

#### Scenario: Charts mode restores track layout for all sections

Given the view mode is Tiles,
when the user clicks the Charts toggle,
then all bullet sections SHALL revert to chart mode with horizontal tracks.

#### Scenario: Period change does not reset view mode

Given the user has set the view mode to Tiles,
when the user changes the global period selector,
then the view mode SHALL remain Tiles after the data reloads.

---

### Requirement: Drill Modal

Clicking a bullet metric value (in chart mode) or a bullet tile (in tile mode) where `drill_id` is non-empty SHALL open the Drill Modal as an overlay.

The modal SHALL display:

1. **Header**: metric title, source badge (background colour from `srcColor`), and the pre-formatted value string.
2. **Filter bar**: the query string (`filter`) used to fetch the underlying data, shown for transparency.
3. **Data table**: columns defined per `drill_id` (see table below), rows from the API response.
4. **Footer**: "Open all in {source} ↗" link and the row count.

The modal SHALL grow to fit content with no internal scroll. It SHALL close when the user clicks the backdrop or the ✕ button in the header.

The full list of drill IDs and their column schemas:

| `drill_id` | Title | Source | Columns |
|---|---|---|---|
| `tasks-completed` | Tasks Completed | Jira | Task, Story Points, Dev Time, Closed |
| `cycle-time` | Task Development Time | Jira | Task, Story Points, Dev Time, Status |
| `task-reopen` | Reopened Tasks | Jira | Task, Reason, Reopened, Resolved |
| `commits` | Commits | Bitbucket | Commit, Repository, +LOC, -LOC, Date |
| `pull-requests` | Pull Requests | Bitbucket | PR, Repository, Status, Cycle Time |
| `reviews` | Code Reviews Given | Bitbucket | PR, Author, Outcome, Time to Review |
| `builds` | Build Results | Bitbucket | Build, Branch, Status, Duration |
| `bugs-fixed` | Bugs Fixed | Jira | Bug, Priority, Fix Time, Closed |

Metrics with no `drill_id` (empty string) SHALL NOT open a modal when clicked.

#### Scenario: Clicking a drillable metric opens the modal

Given the bullet metric `tasks_completed` has `drill_id = "tasks-completed"`,
when the user clicks its value in chart mode,
then the Drill Modal SHALL open showing title "Tasks Completed", source badge "Jira", and a table with columns Task, Story Points, Dev Time, Closed.

#### Scenario: Modal displays correct columns per drill_id

Given the user clicks the `commits` metric (drill_id = "commits"),
when the modal opens,
then the data table SHALL have columns: Commit, Repository, +LOC, -LOC, Date.

#### Scenario: Modal displays the filter query string

Given a drill modal is open,
when the user views the filter bar,
then the filter query string from the API response SHALL be rendered verbatim.

#### Scenario: Modal closes on backdrop click

Given the Drill Modal is open,
when the user clicks outside the modal panel on the backdrop,
then the modal SHALL close and return the user to the dashboard.

#### Scenario: Modal closes on ✕ button click

Given the Drill Modal is open,
when the user clicks the ✕ button in the modal header,
then the modal SHALL close.

#### Scenario: Footer link includes source name

Given the Drill Modal is open for a Jira drill,
when the user views the footer,
then the footer SHALL display "Open all in Jira ↗" as a link.

#### Scenario: Clicking a non-drillable metric does not open the modal

Given a bullet metric has an empty `drill_id`,
when the user clicks it (in either chart or tile mode),
then no modal SHALL appear.

#### Scenario: Tile mode drill opens modal on tile click

Given the view mode is Tiles and the `bugs_fixed` metric has `drill_id = "bugs-fixed"`,
when the user clicks the tile,
then the Drill Modal SHALL open with title "Bugs Fixed" and the correct column schema.

---

### Requirement: Period Awareness

All data displayed on the IC Dashboard — KPI strip values, bullet metric values and bar positions, chart series data — SHALL reflect the active period from the global period selector.

When the period changes, the screen SHALL reload all data (KPI strip, bullet metrics, chart data) for the new period. The drill modal, if open, SHALL close before the reload completes so it does not display stale data.

Bullet metric values are pre-computed per period by the API; the client SHALL NOT aggregate bullet values client-side. Chart series aggregation follows the Chart Granularity Logic described in the LOC Breakdown and Delivery Trends requirements.

The Custom period SHALL map to the Month bucket for bullet metrics and to weekly chart granularity for charts.

#### Scenario: Switching period updates all bullet values

Given the IC Dashboard is showing Week data,
when the user switches the global period to Month,
then all bullet metric values, bar widths, median positions, and statuses SHALL update to Month data.

#### Scenario: Switching period updates KPI strip

Given the IC Dashboard is showing Week data,
when the user switches the global period to Quarter,
then all five KPI strip values and their delta strings SHALL update to Quarter data.

#### Scenario: Custom period maps to Month for bullet values

Given the active period is Custom,
when the bullet metrics render,
then the displayed values SHALL be the same as the Month bucket values.

#### Scenario: Open drill modal closes on period change

Given the Drill Modal is open,
when the user changes the global period selector,
then the modal SHALL close before the page reloads with new period data.

#### Scenario: Period state persists across navigation

Given the user sets the global period to Quarter on the IC Dashboard and then navigates to Team View and back,
when the IC Dashboard reloads,
then it SHALL show Quarter data without reverting to the default period.

---

### Requirement: Privacy Footer

At the bottom of the IC Dashboard page, below all content sections, the page SHALL render a privacy notice in the following exact text:

> This is your personal dashboard. Your manager sees a summary — not the full detail shown here.

The footer SHALL be centred, `fontSize 10px`, `color #9CA3AF`, with top padding of `8px`. It SHALL be visible on all viewport sizes.

#### Scenario: Privacy footer appears at the bottom of the page

Given the IC Dashboard has loaded for a known person,
when the user scrolls to the bottom,
then the privacy footer SHALL be the last element in the content area.

#### Scenario: Privacy footer text is exact

Given the IC Dashboard is rendered,
when the user reads the footer,
then it SHALL display exactly: "This is your personal dashboard. Your manager sees a summary — not the full detail shown here."

#### Scenario: Privacy footer is not shown on not-found state

Given the `personId` is unknown and the "Person not found" state is displayed,
when the page renders,
then the privacy footer SHALL NOT be shown.

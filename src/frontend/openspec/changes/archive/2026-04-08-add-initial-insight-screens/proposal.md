## Why

The `insight` screenset currently contains only scaffolding/demo screens (documentation, dashboard, speed, uikit). The Virtuoso prototype defines the actual product — engineering analytics dashboards for three user roles: executives, team managers, and individual contributors. These screens need to be implemented as production-quality HAI3 screens so the product can be used and iterated on.

## What Changes

- Add `executiveView` screen: org-level engineering KPIs, radar chart (team health), bar chart (metrics by team), teams overview table with status badges; accessible to `executive` role only
- Add `teamView` screen: team member roster with clickable rows, "Attention Needed" alert cards, bullet charts for task delivery / code quality / estimation / AI adoption / collaboration — all with median comparison; accessible to `team_lead` and `executive`
- Add `icDashboard` screen: individual contributor detail view accessed from teamView or sidebar; KPI hero strip, bullet metric cards (task delivery, git output, code quality), LOC breakdown stacked bar chart, delivery trends chart, AI dev tools section (Cursor + Claude Code), collaboration section (Slack, M365, meetings); accessible to all roles
- Add `my-dashboard` screen: same as `icDashboard` but always loads the current user's own data — used by `team_lead` and `ic` roles via "My Dashboard" in the sidebar
- Add role system (`executive` / `team_lead` / `ic`) with dynamic sidebar menu rebuilt on every role switch via `setMenuItems`
- Add `currentUser` domain: `currentUserSlice`, `currentUserEvents`, `currentUserEffects`, `currentUserActions` (with 4 demo users for role switching)
- Add `RoleSwitcher` composite in sidebar bottom — dark-themed role/user switcher for demo purposes
- Add org hierarchy (`OrgTeam` with recursive `subTeams`) in `currentUserEffects`; Executive sees all teams and their members as nested sidebar items; team leads see their own team members as clickable sidebar items
- Add `InsightApiService` domain methods for all three screens with mock data (mirroring the CSV/JSON data from the prototype)
- Add new screen IDs to `ids.ts`; register all screens and `currentUserSlice` in `insightScreenset.tsx`
- Upgrade screenset `category` from `Drafts` to `Production`
- Remove HAI3 app-shell header from `Layout.tsx`; each screen merges identity/title and period controls into its own header row

## Capabilities

### New Capabilities

- `executive-view`: Org-level engineering dashboard — KPI summary cards, radar chart for org health, bar chart comparing key metrics across teams, teams overview table with status (good/warn/bad)
- `team-view`: Team manager dashboard — hero KPI strip, attention-needed alerts with severity, members table (clickable → IC dashboard), bullet charts for task delivery, code quality, estimation accuracy, AI adoption, collaboration metrics
- `ic-dashboard`: Individual contributor dashboard — person header, KPI strip with time-off notice, bullet metric cards in sections (task delivery, git output, code quality), collapsible LOC breakdown chart, delivery trends chart, AI dev tools split view (Cursor / Claude Code / AI chat), collaboration split view (Slack / M365 / meetings), drill modal for metric detail

### Modified Capabilities

- `Layout`: HAI3 app-shell header removed; `RoleSwitcher` added to sidebar bottom
- `Menu`: updated to support recursive nested items, compound `screenId::param` IDs, and Redux-based active item highlighting
- `insightScreenset.tsx`: now registers `currentUserSlice`, `periodSlice`, and domain slices for all three screens; `MY_DASHBOARD_SCREEN_ID` added; category promoted to `Production`

## Impact

- `src/screensets/insight/ids.ts` — add `EXECUTIVE_VIEW_SCREEN_ID`, `TEAM_VIEW_SCREEN_ID`, `IC_DASHBOARD_SCREEN_ID`, `MY_DASHBOARD_SCREEN_ID`
- `src/screensets/insight/insightScreenset.tsx` — register all screens + `currentUserSlice`; change category to `Production`
- `src/screensets/insight/types/index.ts` — add `UserRole`, `CurrentUser`, `TeamViewData` (with `teamName`), all screen data shapes
- `src/screensets/insight/api/mocks.ts` — export `TEAM_MEMBERS_MONTH`; add `teamName` to team view mocks; per-person IC dashboard mock entries
- `src/screensets/insight/slices/` — add `currentUserSlice`, `icDashboardSlice` (with `selectedPersonId`), `teamViewSlice` (with `teamName`), `executiveViewSlice`, `periodSlice`
- `src/screensets/insight/events/` — add `currentUserEvents`, `icDashboardEvents` (with `PersonSelected` + `layout/menu/itemParam`)
- `src/screensets/insight/effects/` — add `currentUserEffects` (with `ORG` hierarchy, `buildMenu`, `teamToMenuItem`)
- `src/screensets/insight/actions/` — add `currentUserActions` (with `MOCK_USERS`, `switchUser`), `icDashboardActions` (with `selectIcPerson`)
- `src/screensets/insight/screens/executive-view/` — new screen + components
- `src/screensets/insight/screens/team-view/` — new screen + components (role-based self-filtering, `selectIcPerson` navigation)
- `src/screensets/insight/screens/ic-dashboard/` — new screen + components (dual screen IDs, inline `PersonHeader`)
- New shared uikit composites: `BulletChart`, `KpiStrip`, `MetricCard`, `CollapsibleSection`, `LocStackedBar`, `DeliveryTrends`, `DrillModal`, `RoleSwitcher`, `PeriodSelectorBar`, `ViewModeToggle`
- `src/app/layout/Layout.tsx` — remove `<Header />`; add `<RoleSwitcher />` in sidebar bottom
- `src/app/layout/Menu.tsx` — recursive item renderer; `activeParam` from Redux; compound `::` ID handling
- Dependencies: `recharts`

---

## Metrics Reference

All metric labels, sections, units and data sources as defined in the prototype. These must be reproduced exactly in API mock data and UI labels.

### IC Dashboard — KPI Strip (hero row, 5 metrics)

| metric_key | Label | Unit | Source |
|---|---|---|---|
| `bugs_fixed` | Bugs Fixed | _(empty)_ | Jira |
| `clean_loc` | Clean LOC | _(empty)_ | Bitbucket |
| `ai_loc_share` | AI LOC Share | `%` | Cursor + Claude Code |
| `spec_lines` | Spec Lines | _(empty)_ | Jira |
| `focus_time_pct` | Focus Time | `%` | Calendar / M365 |

> Unit is an empty string for count metrics — the value field already contains the formatted number (e.g. `"23"`, `"12k"`). Do not set unit to `"count"` — it would render as `"23 count"` in the UI.

Each KPI has a `delta` string (e.g. `"+5"`, `"+8%"`, `"≈"`) and a `delta_type` field. The `delta_type` controls **badge colour only** — the arrow symbol and comparison text are part of the `delta` string itself, supplied by the API. `delta_type` values: `good` → green badge, `warn` / `neutral` → amber badge, `bad` → red badge. All five compare vs prior period or vs team median; the comparison label is embedded in the delta string (e.g. `"↑ 8% vs median 19%"`).

---

### IC Dashboard — Bullet Metrics

Bullet charts show: value vs team range (min/max) with a median marker. Status: `good` / `warn` / `bad`.
Clicking a metric with a `drill_id` opens the drill modal.

#### Section: Task Delivery (`task_delivery`)

| metric_key | Label | Sublabel (data source · definition) | Unit | Drill |
|---|---|---|---|---|
| `tasks_completed` | Tasks Completed | Jira · closed issues in sprint | count | `tasks-completed` |
| `task_dev_time` | Task Development Time | Jira · time in In Progress state · lower = better | h | `cycle-time` |
| `due_date_compliance` | Due Date Compliance | Jira · tasks closed by due date | % | — |
| `estimation_accuracy` | Estimation Accuracy | Jira · tasks within ±20% of original estimate | × | — |
| `task_reopen_rate` | Task Reopen Rate | Jira · closed then reopened within 14 days · lower = better | % | `task-reopen` |

#### Section: Git Output (`git_output`)

| metric_key | Label | Sublabel | Unit | Drill |
|---|---|---|---|---|
| `commits` | Commits Created | Bitbucket · commits authored | count | `commits` |
| `prs_created` | Pull Requests Created | Bitbucket · PRs authored | count | `pull-requests` |
| `prs_merged` | Pull Requests Merged | Bitbucket · authored and merged | count | — |
| `clean_loc` | Clean LOC | Bitbucket · lines added · excl. spec/config | count | — |

#### Section: Code Quality (`code_quality`)

| metric_key | Label | Sublabel | Unit | Drill |
|---|---|---|---|---|
| `reviews_given` | Reviews Given | Bitbucket · PRs reviewed | count | `reviews` |
| `rework_ratio` | Rework Ratio | Bitbucket · lines changed in follow-up commits · lower = better | _(empty — % is embedded in the value string, e.g. `"12%"`)_ | — |
| `build_success` | Build Success Rate | CI · passed ÷ total runs · target ≥90% | % | `builds` |
| `pr_cycle_time` | Pull Request Cycle Time | Bitbucket · PR opened → merged · lower = better | h | `pull-requests` |
| `pickup_time` | Pickup Time | Bitbucket · PR opened → first review · lower = better | h | — |
| `bugs_fixed` | Bugs Fixed | Jira · bug-type issues closed | count | `bugs-fixed` |
| `bug_reopen_rate` | Bug Reopen Rate | Jira · bugs reopened · lower = better | % | — |

#### Section: AI Tools (`ai_tools`)

Split into three subsections: **Cursor**, **Claude Code**, **AI Chat**.

**Cursor**

| metric_key | Label | Sublabel | Unit | Drill |
|---|---|---|---|---|
| `cursor_completions` | Completions Accepted | Cursor · inline suggestions accepted | count | — |
| `cursor_agents` | Agent Requests | Cursor · agent mode requests sent | count | — |
| `cursor_lines` | Lines Accepted | Cursor · lines of code accepted | count | — |

**Claude Code**

| metric_key | Label | Sublabel | Unit | Drill |
|---|---|---|---|---|
| `cc_sessions` | Sessions | Anthropic Enterprise API · coding sessions | count | — |
| `cc_lines` | Lines Added | Anthropic Enterprise API · lines added by Claude Code | count | — |
| `cc_tool_accept` | Tool Acceptance | Anthropic Enterprise API · tool calls accepted ÷ offered | % | — |
| `ai_loc_share2` | AI LOC Share | Cursor + Claude Code · accepted lines ÷ clean LOC | % | — |

> **Note on `ai_loc_share` vs `ai_loc_share2`**: both exist and are distinct. `ai_loc_share` appears in the **KPI strip** (hero row) as a high-level summary metric. `ai_loc_share2` is the **bullet metric** inside the AI Tools section with a full track visualization. They represent the same underlying measurement but live in different API shapes (KpiRow vs BulletMetric) and different UI contexts. The `2` suffix avoids a key collision in the flat data model of the prototype.

**AI Chat**

| metric_key | Label | Sublabel | Unit | Drill |
|---|---|---|---|---|
| `claude_web` | Claude Web Messages | Claude Web · messages sent | count | — |
| `chatgpt` | ChatGPT Messages | ChatGPT · messages sent | count | — |

#### Section: Collaboration (`collab`)

Split into three subsections: **Slack**, **M365**, **Meetings**.

**Slack**

| metric_key | Label | Sublabel | Unit | Drill |
|---|---|---|---|---|
| `slack_threads` | Thread Participation | Slack · replies to others' threads | replies | — |
| `slack_engage` | Message Engagement | Slack · avg replies per thread | avg replies | — |
| `slack_dm` | DM Ratio | Slack · DMs ÷ all messages · lower = more open | % | — |

**M365**

| metric_key | Label | Sublabel | Unit | Drill |
|---|---|---|---|---|
| `m365_emails` | Emails Sent | M365 · emails sent · lower = better for ICs | /mo | — |
| `m365_teams` | Teams Messages | Microsoft Teams · all channels sent | /mo | — |
| `m365_files` | Files Shared | M365 · files shared in Teams/SharePoint | /mo | — |

**Meetings**

| metric_key | Label | Sublabel | Unit | Drill |
|---|---|---|---|---|
| `meeting_hours` | Meeting Hours | Zoom · meeting duration + M365 audioDuration · lower = more focus time | h/mo | — |
| `zoom_calls` | Zoom Calls | Zoom API · calls attended | /mo | — |
| `meeting_free` | Meeting-Free Days | Zoom · days with no meetings + M365 · higher = better | days | — |

---

### IC Dashboard — Charts

**LOC Stacked Bar** (`LocStackedBar`)
Stacked bar chart, series: `AI LOC` (light blue), `Code LOC` (blue), `Spec Lines` (purple).
Granularity adapts to period: Week → daily (Mon–Fri), Month → weekly, Quarter → monthly, Year → quarterly.

**Delivery Trends** (`DeliveryTrends`)
Multi-line chart, series: `Commits` (blue), `PRs Merged` (purple), `Tasks Done` (green).
Same granularity logic as LOC chart.

---

### IC Dashboard — Drill Modal

Opened by clicking a metric with a `drill_id`. Shows: title, value, source badge, filter string (query used), data table, "Open all in {source}" link.

| drill_id | Title | Source | Columns |
|---|---|---|---|
| `tasks-completed` | Tasks Completed | Jira | Task, Story Points, Dev Time, Closed |
| `cycle-time` | Task Development Time | Jira | Task, Story Points, Dev Time, Status |
| `task-reopen` | Reopened Tasks | Jira | Task, Reason, Reopened, Resolved |
| `commits` | Commits | Bitbucket | Commit, Repository, +LOC, -LOC, Date |
| `pull-requests` | Pull Requests | Bitbucket | PR, Repository, Status, Cycle Time |
| `reviews` | Code Reviews Given | Bitbucket | PR, Author, Outcome, Time to Review |
| `builds` | Build Results | Bitbucket | Build, Branch, Status, Duration |
| `bugs-fixed` | Bugs Fixed | Jira | Bug, Priority, Fix Time, Closed |

---

### Team View — Members Table

One row per team member. Columns:

| Field | Label | Unit | Notes |
|---|---|---|---|
| `name` | Name | — | Clickable → IC Dashboard |
| `seniority` | Seniority | — | Junior / Middle / Senior / Lead |
| `tasks_closed` | Tasks | count | Scaled by period |
| `bugs_fixed` | Bugs | count | Scaled by period |
| `dev_time_h` | Dev Time | h | Avg per task |
| `prs_merged` | PRs | count | Scaled by period |
| `build_success_pct` | Build % | % | Pass rate |
| `focus_time_pct` | Focus % | % | ≥60% = green |
| `ai_tools` | AI Tools | — | Comma-separated tool names |
| `ai_loc_share_pct` | AI LOC % | % | AI-generated share |

---

### Executive View — Teams Table

One row per team. Columns:

| Field | Label | Unit | Threshold |
|---|---|---|---|
| `team_name` | Team | — | — |
| `headcount` | Headcount | count | — |
| `tasks_closed` | Tasks Closed | count | — |
| `bugs_fixed` | Bugs Fixed | count | — |
| `build_success_pct` | Build % | % | ≥90% green |
| `focus_time_pct` | Focus % | % | ≥60% green |
| `ai_adoption_pct` | AI Adoption | % | ≥60% green |
| `ai_loc_share_pct` | AI LOC % | % | — |
| `pr_cycle_time_h` | PR Cycle | h | — |
| `status` | Status | good/warn/bad | Badge |

Org-level KPI cards: **Teams at Risk** (count with warn/bad status), **Avg Build Success** (%), **Avg AI Adoption** (%), **Avg Focus Time** (%).

Radar chart axes: Build Success, AI Adoption, Focus Time, Bug Resolution, PR Cycle.

---

### Period Granularity

All data is period-aware. The period selector supports: **Week**, **Month**, **Quarter**, **Year**, **Custom** (date range picker).

Chart granularity: Week → daily points (Mon–Fri), Month → weekly points, Quarter → monthly points, Year → quarterly points.

Bullet metric values are pre-computed per period in the API response (not derived client-side). Count metrics (tasks, bugs, commits, PRs, LOC) scale with period length; rate/percentage metrics (build %, focus %, AI LOC share) remain stable.

---

## Business Logic

### Bullet Chart Rendering

Each bullet metric from the API carries pre-computed display fields:

| Field | Description |
|---|---|
| `value` | Formatted display value (e.g. `"8"`, `"11h"`, `"88%"`) |
| `unit` | Unit suffix appended to value in UI |
| `range_min` / `range_max` | Left/right labels on the chart track |
| `median` | Raw numeric median value — used server-side to compute `median_left_pct` and `status`; also needed client-side if recalculation is required |
| `median_label` | Human-readable median label shown at center-bottom of the track (e.g. `"Median: 7.6"`, `"Target 0.9–1.3×"`) |
| `bar_left_pct` | Left offset of the value bar as % of track width — **always `0`**: the bar always starts from the left edge of the track |
| `bar_width_pct` | Width of the value bar as % of track width |
| `median_left_pct` | Position of the median marker line as % of track width |
| `status` | `good` / `warn` / `bad` — controls bar color and status badge |
| `drill_id` | Non-empty string → metric is clickable, opens drill modal |

---

### BulletChart — Visual Anatomy

#### Chart mode (default)

```
┌─ Header ──────────────────────────────────────────────────┐
│ label (12px, #6B7280, weight 500)      value (14px, bold) │
│ sublabel (9px, #9CA3AF, weight 400)    unit (10px, #9CA3AF)│
│                                        period suffix (9px) │
└───────────────────────────────────────────────────────────┘
┌─ Track (height 24px, bg #F0F2F7, borderRadius 6px) ───────┐
│  │← median line (2px wide, #2563EB 50% opacity,           │
│  │  overflows track: top -3px, bottom -3px)                │
│  ████████ value bar (height 12px, top 6px, borderRadius 4px│
│           color from status, animated width 0.5s ease)     │
└───────────────────────────────────────────────────────────┘
┌─ Footer (marginTop 3px, fontSize 9px, #9CA3AF) ───────────┐
│ range_min              median_label              range_max  │
└───────────────────────────────────────────────────────────┘
```

**Header details:**
- Label and value are on the same row, space-between, aligned to baseline
- Label: `fontSize 12px`, `color #6B7280`, `fontWeight 500`, `lineHeight 1.4`
- Sublabel: on a new line below label, `fontSize 9px`, `color #9CA3AF`, `fontWeight 400`; format: `"{source} · {definition} · by {personName}"`
- Value: `fontSize 14px`, `fontWeight 800`, `color #111827`
- If `drill_id` is non-empty: value has `borderBottom: 1px dotted #2563EB`, cursor pointer; clicking opens drill modal
- Unit: `fontSize 10px`, `color #9CA3AF`
- Period suffix: `fontSize 9px`, `color #B0B8C8`; only shown for count metrics (not %, ×, h, avg)

**Track details:**
- Container: `height 24px`, `background #F0F2F7`, `borderRadius 6px`, `position relative`
- Median line: `position absolute`, `width 2px`, `background #2563EB`, `opacity 0.5`, `borderRadius 2px`, `top -3px bottom -3px` (overflows track by 3px each side)
- Value bar: `position absolute`, `top 6px`, `height 12px`, `borderRadius 4px`, `transition: width 0.5s ease`; color from status

**Footer details:**
- Three labels in a flex row, `justifyContent space-between`, `marginTop 3px`, `fontSize 9px`, `color #9CA3AF`
- Left: `range_min`, Center: `median_label`, Right: `range_max`

---

#### Tile mode

```
┌─ Tile (bg #F0F2F7, borderRadius 8px, padding 12px 14px) ──┐
│ label (10px, #6B7280, weight 500)                          │
│ value (22px, weight 800, #111827, letterSpacing -0.5)      │
│   unit (11px, #9CA3AF)  period suffix (9px, #9CA3AF)       │
│ ┌─ badge (borderRadius 10px, padding 1px 6px) ────────┐    │
│ │ ↑/→/↓  median_label   (10px, weight 600)            │    │
│ └─────────────────────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────┘
```

- Entire tile is clickable if `drill_id` is non-empty
- Status badge: arrow from `STATUS_ARROW` map (`good → ↑`, `warn → →`, `bad → ↓`), then `median_label` text; background and color from status maps
- No track, no range labels in tile mode

---

#### Period suffix logic

Suffix appended after unit for count metrics only. Suppressed when unit contains `%`, `×`, `avg replies`, `avg`, or `h`:

| Period | Suffix |
|---|---|
| week | `/ wk` |
| month | `/ mo` |
| quarter | `/ qtr` |
| year | `/ yr` |

---

**Bar position formula** (computed server-side):
```
bar_left_pct    = 0   (always — bar starts from the left edge)
bar_width_pct   = (value - range_min) / (range_max - range_min) * 100
median_left_pct = (median - range_min) / (range_max - range_min) * 100
```
For "lower is better" metrics, the bar still grows left-to-right; status inversion is handled by the `status` field.

**Status color mapping:**
- `good` → `#16A34A` (green), background `#DCFCE7`
- `warn` → `#D97706` (amber), background `#FEF3C7`
- `bad` → `#DC2626` (red), background `#FEE2E2`

**Sublabel format** — rendered below the metric label:
- Template: `"{source} · {definition} · {person name}"`
- Example: `"Jira · closed issues in sprint · Alice Kim"`

---

### Status Thresholds

The `status` field is determined server-side per metric relative to the **team median**. Thresholds are expressed as a percentage of the median, not absolute values.

| Metric type | good | warn | bad |
|---|---|---|---|
| Higher-is-better (tasks, PRs, reviews, LOC, build %, focus %) | value ≥ 100% of median | value ≥ 70% and < 100% of median | value < 70% of median |
| Lower-is-better (dev time, cycle time, reopen rate, rework ratio, DM ratio) | value ≤ 100% of median | value > 100% and ≤ 130% of median | value > 130% of median |
| Range-based (estimation accuracy target 0.9–1.3×) | within target range | ±20% outside range | >20% outside range |

The green/amber/red boundary is always anchored to the median — there are no hardcoded absolute thresholds. If the team median shifts, all statuses recalculate accordingly.

Executive team `status` field rolls up from the team's worst individual metric status.

---

### KPI Strip Delta Logic

Each KPI in the hero strip has two fields:
- `delta` — a pre-formatted string from the API containing the arrow symbol and comparison text (e.g. `"↑ 5 vs Jun"`, `"+8% vs median 19%"`, `"≈"`). The UI renders this string verbatim; it does **not** generate arrows from `delta_type`.
- `delta_type` — controls badge **colour only**: `good` → green, `warn` / `neutral` → amber, `bad` → red.

There is no separate `vs_label` field — the comparison baseline is embedded in the `delta` string itself.

---

### Drill Modal

Triggered by clicking any bullet metric where `drill_id` is non-empty. The modal shows:
1. **Header**: metric title, source badge (colored by tool), value
2. **Filter bar**: the query string used to fetch the data (shown for transparency)
3. **Table**: rows of underlying records (tasks, PRs, commits, bugs, builds, reviews)
4. **Footer**: "Open all in {source} ↗" link + row count

Modal sizing: grows to fit content with no internal scroll. Closes on backdrop click or ✕ button.

---

### Chart Granularity Logic

Charts always show one level of granularity **finer** than the selected period. The period selector controls both the bullet metric values and the chart x-axis resolution.

#### Period → Chart resolution mapping

| Period selected | Chart x-axis | Labels | Source data |
|---|---|---|---|
| **Week** | Daily (5 points) | `Mon Tue Wed Thu Fri` | Last weekly bucket, split by day weights |
| **Month** | Weekly (4 points) | `Jun 2`, `Jun 9`, `Jun 16`, `Jun 23` | Last 4 weekly rows |
| **Quarter** | Monthly (3 points) | `Jun`, `Jul`, `Aug` | Weekly rows grouped by month prefix |
| **Year** | Quarterly (exactly 2 points) | `Q2 2026`, `Q3 2026` | Monthly totals grouped into quarters |
| **Custom** | Weekly (4 points) | Same as Month | Treated as Month granularity |

#### Week → Daily distribution

When period = **Week**, the weekly total for each series is split across 5 weekdays using fixed weights:

```
Mon: 15%  Tue: 22%  Wed: 24%  Thu: 21%  Fri: 18%
```

Values are `Math.round(total * weight)`. This preserves the weekly total (±1 due to rounding).

#### Month → Weekly aggregation

Last 4 rows from the weekly time-series table. Each row uses its `week_label` (e.g. `"Jun 23"`) as the x-axis label.

#### Quarter → Monthly aggregation

Weekly rows are grouped by the month name prefix of `week_label` (e.g. `"Jun 9"` → `"Jun"`). All values within the same month are summed. Result: one point per month, labeled with the 3-letter month abbreviation.

#### Year → Quarterly aggregation

All weekly rows are summed into a single total, then split into two synthetic quarters using fixed ratios:
- Q2: 40% of total
- Q3: 60% of total

Labels: `"Q2 2026"`, `"Q3 2026"`. This is a prototype approximation; in production these would be actual quarterly API buckets.

#### Custom date range

Treated identically to **Month** — shows last 4 weekly buckets regardless of the selected date range. In production, the API would return weekly buckets within the selected date range.

#### Bullet metrics and period

Bullet metric `value` fields are pre-computed per period by the API and stored separately (one row per `metric_key × period`). The client filters by `period` key: `week`, `month`, `quarter`, `year`. Custom maps to `month`. No client-side aggregation of bullet values occurs — only chart series are aggregated client-side in the prototype.

#### Global scope of the period selector

The period selector (Week / Month / Quarter / Year tabs + Custom calendar) is a **global, app-wide control** located in the NavBar. It is persistent across navigation and affects every screen simultaneously:

- **IC Dashboard** — KPI strip values, all bullet metric values and their bar positions/statuses, LOC Stacked Bar chart, Delivery Trends chart
- **Team View** — hero KPI strip, all member table count columns (tasks, bugs, PRs), all bullet charts in the collapsible sections
- **Executive View** — all team table count columns, org-level KPI cards, bar chart, radar chart

> **Production vs prototype difference**: the prototype's `ExecutiveView.tsx` does not call `usePeriod()` and shows static data regardless of the selected period. In the production screenset, Executive View **must** be period-aware — all API calls must include the active period parameter, and all count values must reflect the selected period.

When the user switches from Month to Quarter on the IC Dashboard and then navigates to Team View, Team View opens already showing quarterly data — no reset occurs.

The **Custom date range** calendar is also global. Selecting a custom range sets the period to `Custom` across all screens. The selected range is displayed in the NavBar date button on every page. Cancelling the custom range reverts to the previously active standard period (Month by default).

There is exactly one period state in the application at any time. All API calls on all screens must include the active period (or date range) as a parameter.

---

### Period Scaling for Count Metrics

The API returns values pre-scaled for each period. Scale factors relative to month baseline:

| Period | Count scale factor |
|---|---|
| week | ÷ 4 (approx) |
| month | 1× (baseline) |
| quarter | 3× |
| year | 12× |

Rate and percentage metrics (`build_success_pct`, `focus_time_pct`, `ai_loc_share_pct`, `due_date_compliance`, `estimation_accuracy`) are **not** scaled — they represent averages, not totals.

---

### IC Dashboard Layout Logic

**Section grouping** — bullet metrics are grouped into collapsible `MetricCard` sections:
1. Task Delivery (`task_delivery`) — always expanded
2. Git Output (`git_output`) — always expanded
3. Code Quality (`code_quality`) — always expanded
4. AI Dev Tools (`ai_tools`) — split into two columns: left = Cursor + Claude Code metrics, right = AI LOC Share + AI Chat
5. Collaboration (`collab`) — split into three columns: Slack · M365 · Meetings

**AI Tools column split:**
- Left: `cursor_completions`, `cursor_agents`, `cursor_lines`, `cc_sessions`, `cc_lines`, `cc_tool_accept`
- Right: `ai_loc_share2`, `claude_web`, `chatgpt`

**Collaboration column split:**
- Slack: all `metric_key` starting with `slack_`
- M365: all `metric_key` starting with `m365_`
- Meetings: `meeting_hours`, `zoom_calls`, `meeting_free`

**Collapsible sections default state** — all `CollapsibleSection` components start **collapsed** (hidden) when the page loads. The user must expand them manually. Sections: LOC Breakdown, Delivery Trends, AI Dev Tools & AI Chat, Collaboration.
The three `MetricCard` sections (Task Delivery, Git Output, Code Quality) are not collapsible — they are always visible.

**Chart section subtitles** — displayed below the section title in smaller muted text:
- LOC Breakdown: `"Bitbucket · lines added per period · AI-assisted vs manual vs spec/config"`
- Delivery Trends: `"Jira + Bitbucket · activity counts · Commits, PRs and Tasks are independent signals — not directly comparable"`

**View mode toggle** — NavBar has Charts / Tiles toggle:
- `chart` mode: bullet track visualization
- `tile` mode: large number + status badge, no track

---

### IC Dashboard — Person Header

Displays above the KPI strip. Fields:
- **Avatar**: circle with first two initials of the person's name (e.g. `"AK"` for Alice Kim), blue background `#EFF6FF`, blue text `#2563EB`
- **Name**: full name from API
- **Role line**: `"{role} · {seniority}"` (e.g. `"Backend Developer · Senior"`)

> **Production vs prototype difference**: the role string `"Backend Developer"` is hardcoded in the prototype. In production it must come from the person's profile in the API response.

---

### Team View Hero KPI Strip

Four summary cards at the top of Team View (`at_risk_count`, `team_dev_time`, `focus_gte_60`, `not_using_ai`). Response shape matches `team_kpis.csv`: `metric_key`, `label`, `value`, `unit`, `status`, `section`.

> **Production vs prototype difference**: the prototype hardcodes these four values in JSX. In the production screenset they must come from the API and respect the active period parameter.

---

### Bullet Chart Legend Labels by Screen

The median marker line label differs intentionally between screens:

| Screen | Median line label | Rationale |
|---|---|---|
| IC Dashboard | `"Team median"` | IC is compared against their own team |
| Team View | `"Company median"` | Team aggregate is compared against the wider organisation |

This is not a bug — comparing a team against itself would be meaningless at the team level.

---

### Team View Attention Needed

The "Attention Needed" alert section surfaces team members whose metrics are below threshold. Rules:
- `build_success_pct < 90` → alert: "Build success rate below target"
- `focus_time_pct < 60` → alert: "Focus time below 60% target"
- `ai_loc_share_pct < 10` → alert: "Low AI tool adoption"

Severity: `bad` (red) for >20% below threshold, `warn` (amber) otherwise.

---

### Executive View Thresholds

Color coding in the teams table:
- `build_success_pct`: ≥90% → green, else amber
- `focus_time_pct`: ≥60% → green, else amber
- `ai_adoption_pct`: ≥60% → green, else amber

Org-level KPI card for "Teams at Risk": count of teams where `status` is `warn` or `bad`.

---

## Routing & Navigation

| Route | Screen | Notes |
|---|---|---|
| `/` | Redirect → `/ic/1` | Default entry point, hardcoded to person_id=1 |
| `/ic/:personId` | IC Dashboard | `personId` filters team_members data |
| `/team/:teamId` | Team View | `teamId` param present but **unused** — always shows the same team |
| `/executive` | Executive View | No params |

> **Production note**: `teamId` must be wired to the API in production. The prototype always renders a single hardcoded team regardless of the URL.

There is no in-app back button. Navigation between screens uses the NavBar links. Team View → IC Dashboard is via member row click (`navigate('/ic/{person_id}')`).

---

## Page Layout & Typography

Applies to all three screens via `PageLayout`:

| Property | Value |
|---|---|
| Background | `#F0F2F7` |
| Font family | `-apple-system, BlinkMacSystemFont, 'Inter', sans-serif` |
| Base font size | `13px` |
| Line height | `1.5` |
| Content max-width | Mobile: 100%, Tablet: 95%, Desktop: 80% |
| Content padding | Mobile: `12px 12px 32px`, Desktop: `20px 20px 40px` |
| Gap between sections | Mobile: `10px`, Desktop: `14px` |

---

## KPI Strip — Full Schema & Visuals

### KpiRow API shape

| Field | Type | Description |
|---|---|---|
| `period` | string | `week` / `month` / `quarter` / `year` |
| `metric_key` | string | Identifier |
| `label` | string | Display label |
| `value` | string | Formatted value (e.g. `"23"`, `"12k"`, `"68"`) |
| `unit` | string | Unit suffix (e.g. `"%"`) — empty for counts |
| `sublabel` | string | Source description shown below label |
| `delta` | string | Pre-formatted comparison string (verbatim from API) |
| `delta_type` | string | `good` / `warn` / `bad` / `neutral` — badge colour only |

### KpiRow sublabels (exact strings from prototype data)

| metric_key | sublabel |
|---|---|
| `bugs_fixed` | `Jira · bug-type issues closed in sprint` |
| `clean_loc` | `Bitbucket · lines added in diffs · excl. spec/config` |
| `ai_loc_share` | `Cursor + Claude Code · accepted lines ÷ clean LOC` |
| `spec_lines` | `Bitbucket · lines added in diffs · OpenAPI · YAML · config` |
| `focus_time_pct` | `Zoom · meeting duration + M365 · meetingInteractedCount` |

### KpiRow delta strings by period

| metric_key | week | month | quarter | year |
|---|---|---|---|---|
| `bugs_fixed` | `↑ 1 vs last week` | `↑ 5 vs Jun` | `↑ 12 vs Q1` | `↑ 31 vs last year` |
| `clean_loc` | `↑ 10%` | `↑ 8%` | `↑ 6%` | `↑ 11%` |
| `ai_loc_share` | `↑ median 19%` | `↑ median 19%` | `↑ median 19%` | `↑ median 19%` |
| `spec_lines` | `≈ median` | `≈ median` | `≈ median` | `≈ median` |
| `focus_time_pct` | `≥ 60% target` | `≥ 60% target` | `≥ 60% target` | `≥ 60% target` |

### KpiCell visual spec

- **Value**: `20px`, `font-weight 800`, `tracking-tight`, `#111827`
- **Unit**: rendered as `<sup>`, `11px`, `font-weight 600` (appears superscript next to value)
- **Period suffix**: `9px`, `font-weight normal`, `#9CA3AF`, space before
- **Label**: `11px`, `font-weight 600`, `#111827`
- **Sublabel**: `10px`, `#9CA3AF`
- **Delta badge**: `10px`, `font-weight 700`, `rounded-full`, `px-1.5 py-0.5`; colour from `delta_type`
- **Separator**: vertical line `bg-insight-border` between cells; on mobile omitted at start of each row (`i % 2 === 0`)
- **`plain` prop**: when `true`, KpiStrip renders without a Card wrapper — used on IC Dashboard where the strip shares a container div with the time-off notice banner

---

## MetricCard — Visual Spec

Card with title header, optional legend, and a grid of BulletChart instances.

**Header (chart mode):**
- Title: `11px`, `font-weight 700`, `uppercase`, `letter-spacing 0.6px`, `#6B7280`
- Subtitle top-right: `"vs team range"`, `10px`, `#9CA3AF`

**Legend (chart mode only):**
- Blue line swatch (2px × 11px, `rgba(37,99,235,0.5)`) → label `"Team median"`
- Gradient bar swatch (18px × 5px, gradient `#16A34A → #D97706 → #DC2626`) → label `"Your result · color = vs target"`
- Font: `9px`, `#9CA3AF`

**Column layout (chart mode):**
Metrics distributed **round-robin** across columns by index: metric `i` goes to column `i % columns`. For 7 metrics across 3 columns: col0=[0,3,6], col1=[1,4], col2=[2,5]. Gap between columns: `14px`. Gap between metrics within a column: `16px`.

**Tile mode:**
- No legend, no "vs team range" subtitle
- Fixed **3-column grid** regardless of `columns` prop
- Tile gap: `8px`

---

## CollapsibleSection — Visual Spec

Card with a clickable trigger row and animated content panel.

**Trigger row:**
- Full width, `px-4 py-3`, `bg-white`, hover `bg-muted/30` (transition)
- Left: title (`13px`, `font-weight 600`, `#111827`) + optional subtitle below (`10px`, `#9CA3AF`, `margin-top 2px`)
- Right: badge `"Expanded"` / `"Collapsed"` (secondary variant, `10px`, `bg-insight-bg`) + `▴` / `▾` arrow (`10px`)

**Content panel:**
- `border-t border-insight-border`, `bg-white`
- Default state: **collapsed** (`defaultOpen=false`). All sections in the prototype use the default.

---

## `scale` — Custom Range Multiplier

The `PeriodContext` exposes a `scale` value alongside `period` and `dateRange`:

```
scale = (to - from) in days / 30
```

Standard period scales: Week ≈ 0.23, Month = 1.0, Quarter ≈ 3.0, Year ≈ 12.0. For Custom ranges, `scale` is proportional to the selected day count.

> In the prototype `scale` is computed but not consumed — bullet metric values come from pre-computed CSV rows. In production, `scale` can be used for client-side estimation when the API returns monthly baseline values and the selected range is Custom.

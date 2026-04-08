# Spec: analytics-api (v1 — source-traced)

**Service:** Analytics API
**Base path:** `/api/v1/analytics`
**Auth:** `Authorization: Bearer <token>` + `X-Tenant-ID: <tenantId>` required

---

## Overview

Pre-aggregated, screen-ready view endpoints. The backend computes all visual positioning
(`bar_left_pct`, `median_left_pct`), statuses, medians, and thresholds from ClickHouse
Silver/Gold tables. The frontend renders data as received — no client-side aggregation.

Chart data (`locTrend`, `deliveryTrend`) is returned at the granularity of the requested
period: daily points for `week`, weekly for `month`, monthly for `quarter`, quarterly for
`year`. The frontend renders `label` as the x-axis key directly.

**Data availability:** Fields from missing/pending sources return `null` (numbers),
`""` (strings), or `[]` (arrays). The `data_availability` envelope in each response
lets the frontend show "not configured" states instead of misleading zeros.

---

## Source Legend

| Tag | Source | Silver Table | Status |
|---|---|---|---|
| `[git]` | GitHub / Bitbucket Cloud / Bitbucket Server | `class_git_commits`, `class_git_pull_requests`, `class_git_pull_requests_reviewers` | ✓ available |
| `[ai-code]` | Claude Team (Code), Cursor | `class_ai_dev_usage` | ✓ available |
| `[hr]` | BambooHR | `class_people` | ✓ available |
| `[comms]` | Zoom, M365, Slack | `class_comms_events` | ✓ available |
| `[tasks]` | Jira | `class_tasks` (TBD) | ⚠ pending — PR #62 |
| `[ci]` | GitHub Actions | no connector yet | ❌ missing |
| `[identity]` | Identity Service (MariaDB) | `org_units`, `persons` | ✓ available |
| `[hr-leave]` | BambooHR leave_requests | `bronze_bamboohr.leave_requests` | ✓ available |

---

## Shared Types

```ts
type PeriodValue = 'week' | 'month' | 'quarter' | 'year';

type DataAvailability = {
  git:   'available' | 'no-connector' | 'syncing';
  tasks: 'available' | 'no-connector' | 'syncing';
  ci:    'available' | 'no-connector' | 'syncing';
  comms: 'available' | 'no-connector' | 'syncing';
  hr:    'available' | 'no-connector' | 'syncing';
  ai:    'available' | 'no-connector' | 'syncing';
};

/**
 * BulletMetric — benchmark bar for an individual IC metric.
 * All visual positions are precomputed from org-unit distribution (P5/P50/P95).
 */
type BulletMetric = {
  period:           PeriodValue;
  section:          string;
  metric_key:       string;     // see metric_key catalog below
  label:            string;
  sublabel?:        string;
  value:            string;
  unit:             string;
  range_min:        string;     // [varies] P5 of org-unit distribution
  range_max:        string;     // [varies] P95 of org-unit distribution
  median:           string;     // [varies] P50 of org-unit distribution
  median_label:     string;
  bar_left_pct:     number;     // precomputed: (value - range_min) / (range_max - range_min) * 100
  bar_width_pct:    number;     // precomputed: fixed or proportional width
  median_left_pct:  number;     // precomputed: (median - range_min) / (range_max - range_min) * 100
  status:           'good' | 'warn' | 'bad';
  drill_id:         string;     // empty string if no drill available
};

type BulletSection = {
  id:       string;
  title:    string;
  metrics:  BulletMetric[];
};

type DrillRow = Record<string, string | number>;

type DrillData = {
  title:    string;
  source:   string;
  srcClass: string;
  value:    string;
  filter:   string;
  columns:  string[];
  rows:     DrillRow[];
};
```

---

## Executive View

### `GET /views/executive?period={period}`

**Response:** `ExecViewData`

```ts
type ExecColumnThreshold = {
  metric_key: string;
  threshold:  number;   // green if value >= threshold, amber otherwise
};

type ExecViewConfig = {
  column_thresholds: ExecColumnThreshold[];
  // stored in Analytics API MariaDB; returned with each view response
};

/**
 * ExecTeamRow — one row in the teams table.
 *
 * Source annotations:
 *   team_id / team_name     [identity]  org_units.id / org_units.name
 *   headcount               [hr]        COUNT(class_people) WHERE org_unit AND status='active'
 *   tasks_closed            [tasks]     COUNT(class_tasks) WHERE done AND type!='Bug'  ← ⚠ pending
 *   bugs_fixed              [tasks]     COUNT(class_tasks) WHERE done AND type='Bug'   ← ⚠ pending
 *   build_success_pct       [ci]        successful_runs / total_runs * 100             ← ❌ missing → null
 *   focus_time_pct          [comms]     (work_h - meeting_h) / work_h * 100
 *   ai_adoption_pct         [ai-code]   active_ai_users / headcount * 100
 *   ai_loc_share_pct        [ai-code]   SUM(lines_added) / total_loc * 100
 *   pr_cycle_time_h         [git]       AVG(merged_at - created_at) in hours
 *   status                  backend     computed from ExecViewConfig.column_thresholds
 */
type ExecTeamRow = {
  team_id:            string;
  team_name:          string;
  headcount:          number;
  tasks_closed:       number | null;   // null when [tasks] not configured
  bugs_fixed:         number | null;   // null when [tasks] not configured
  build_success_pct:  number | null;   // null when [ci] not configured
  focus_time_pct:     number;
  ai_adoption_pct:    number;
  ai_loc_share_pct:   number;
  pr_cycle_time_h:    number;
  status:             'good' | 'warn' | 'bad';
};

/**
 * OrgKpis — org-wide KPI summary strip.
 *
 *   avgBuildSuccess    [ci]     AVG(build_success_pct) across teams   ← ❌ missing → null
 *   avgAiAdoption      [ai-code] AVG(ai_adoption_pct) across teams
 *   avgFocus           [comms]  AVG(focus_time_pct) across teams
 *   bugResolutionScore [tasks]  bugs_fixed / (bugs_opened + 1) * 100  ← ⚠ pending → null
 *   prCycleScore       [git]    score derived from AVG(pr_cycle_time_h) vs threshold
 */
type OrgKpis = {
  avgBuildSuccess:    number | null;
  avgAiAdoption:      number;
  avgFocus:           number;
  bugResolutionScore: number | null;
  prCycleScore:       number;
};

type ExecViewData = {
  teams:              ExecTeamRow[];
  orgKpis:            OrgKpis;
  config:             ExecViewConfig;
  data_availability:  DataAvailability;
};
```

**Errors:** `401`, `403` → `ProblemDetail`

---

## Team View

### `GET /views/team?period={period}`

**Response:** `TeamViewData`

```ts
type TeamKpi = {
  metric_key:   string;
  label:        string;
  value:        string;
  unit:         string;
  sublabel?:    string;
  chipLabel?:   string;
  status:       'good' | 'warn' | 'bad';
  section:      string;
};

/**
 * TeamMember — one row in the members table.
 *
 * Source annotations:
 *   person_id           [identity]  persons.id
 *   name                [hr]        class_people.display_name
 *   seniority           [hr]        class_people.custom_str_attrs['seniority'] or job_title mapping
 *   tasks_closed        [tasks]     COUNT(class_tasks WHERE done AND type!='Bug')  ← ⚠ pending
 *   bugs_fixed          [tasks]     COUNT(class_tasks WHERE done AND type='Bug')   ← ⚠ pending
 *   dev_time_h          [comms]     work_h - SUM(meeting duration from class_comms_events)
 *   prs_merged          [git]       COUNT(class_git_pull_requests WHERE merged)
 *   build_success_pct   [ci]        successful_runs / total_runs * 100             ← ❌ missing → null
 *   focus_time_pct      [comms]     (work_h - meeting_h) / work_h * 100
 *   ai_tools            [ai-code]   ARRAY_AGG(DISTINCT source FROM class_ai_dev_usage)
 *   ai_loc_share_pct    [ai-code]   SUM(lines_added) / total_loc * 100
 *   trend_label         [git]       rolling comparison of prs_merged across prior periods
 */
type TeamMember = {
  person_id:          string;
  period:             PeriodValue;
  name:               string;
  seniority:          string;
  tasks_closed:       number;         // 0 when [tasks] not configured
  bugs_fixed:         number;         // 0 when [tasks] not configured
  dev_time_h:         number;
  prs_merged:         number;
  build_success_pct:  number | null;  // null when [ci] not configured
  focus_time_pct:     number;
  ai_tools:           string[];
  ai_loc_share_pct:   number;
  trend_label?:       string;
};

/**
 * AlertThreshold — drives Attention Needed section.
 * JSON-serializable; no functions. UI generates description text from metric_key + values.
 */
type AlertThreshold = {
  metric_key: string;
  trigger:    number;   // member is at-risk if value < trigger
  bad:        number;   // severity 'bad' if value < bad, else 'warn'
  reason:     string;   // short label e.g. "Focus time below 60% target"
};

type ColumnThreshold = {
  metric_key:       string;
  good:             number;
  warn:             number;
  higher_is_better: boolean;
};

type TeamViewConfig = {
  alert_thresholds:   AlertThreshold[];   // drives Attention Needed
  column_thresholds:  ColumnThreshold[];  // drives MembersTable column coloring
};

/**
 * TeamViewData
 *
 * KPI derivation note:
 *   at_risk_count, focus_gte_60, not_using_ai — computed by backend from
 *   member data using config.alert_thresholds. Frontend does NOT recompute.
 */
type TeamViewData = {
  teamName:           string;
  teamKpis:           TeamKpi[];
  members:            TeamMember[];
  bulletSections:     BulletSection[];
  config:             TeamViewConfig;
  data_availability:  DataAvailability;
};
```

**Errors:** `401`, `403` → `ProblemDetail`

---

## IC Dashboard

### `GET /views/ic/{personId}?period={period}`

**Response:** `IcDashboardData`

```ts
type PersonData = {
  person_id:  string;   // [identity]
  name:       string;   // [hr] class_people.display_name
  role:       string;   // [identity] user role from Identity Service
  seniority:  string;   // [hr] class_people.custom_str_attrs['seniority']
};

/**
 * IcKpi — hero strip KPI card.
 *
 * metric_key catalog:
 *   loc               [git]      SUM(additions - deletions FROM class_git_commits)
 *   ai_loc_share_pct  [ai-code]  SUM(lines_added) / loc * 100 FROM class_ai_dev_usage
 *   prs_merged        [git]      COUNT(class_git_pull_requests WHERE merged)
 *   pr_cycle_time_h   [git]      AVG(merged_at - created_at) in hours
 *   focus_time_pct    [comms]    (work_h - meeting_h) / work_h * 100
 *   tasks_closed      [tasks]    COUNT(class_tasks WHERE done AND type!='Bug')  ⚠ pending
 *   bugs_fixed        [tasks]    COUNT(class_tasks WHERE done AND type='Bug')   ⚠ pending
 *   build_success_pct [ci]       —                                              ❌ missing
 *   ai_sessions       [ai-code]  SUM(session_count FROM class_ai_dev_usage)
 *
 *   delta: vs. previous period (same duration), formatted as "+12%" / "-3h"
 *   delta_type: 'good' when improvement, 'bad' when regression, 'neutral' otherwise
 */
type IcKpi = {
  period:       PeriodValue;
  metric_key:   string;
  label:        string;
  value:        string;
  unit:         string;
  sublabel:     string;
  description?: string;
  delta:        string;
  delta_type:   'good' | 'warn' | 'bad' | 'neutral';
};

type TimeOffNotice = {
  days:         number;   // [hr-leave] SUM(days FROM bronze_bamboohr.leave_requests)
  dateRange:    string;   // formatted date range
  bambooHrUrl:  string;   // deep link to BambooHR profile
};

/**
 * Chart data — granularity matches period:
 *   week    → 5 daily points    (Mon, Tue, Wed, Thu, Fri)
 *   month   → 4 weekly points   (W1, W2, W3, W4)
 *   quarter → 3 monthly points  (Jan, Feb, Mar)
 *   year    → 4 quarterly points (Q1, Q2, Q3, Q4)
 *
 * Frontend renders label as x-axis key. No client-side aggregation.
 */
type LocDataPoint = {
  label:    string;
  aiLoc:    number;   // [ai-code] SUM(lines_added FROM class_ai_dev_usage) per bucket
  codeLoc:  number;   // [git]     SUM(additions - deletions FROM class_git_commits) per bucket
  specLines: number;  // ❌ no source — always 0 in v1
};

type DeliveryDataPoint = {
  label:      string;
  commits:    number;   // [git]    COUNT(class_git_commits) per bucket
  prsMerged:  number;   // [git]    COUNT(class_git_pull_requests WHERE merged) per bucket
  tasksDone:  number;   // [tasks]  COUNT(class_tasks WHERE done) per bucket  ⚠ pending → 0
};

type IcChartsData = {
  locTrend:       LocDataPoint[];
  deliveryTrend:  DeliveryDataPoint[];
};

type IcDashboardData = {
  person:             PersonData;
  kpis:               IcKpi[];
  bulletMetrics:      BulletMetric[];
  charts:             IcChartsData;
  timeOffNotice:      TimeOffNotice | null;
  drills:             Record<string, DrillData>;
  data_availability:  DataAvailability;
};
```

**Errors:** `401`, `403`, `404` (person not found) → `ProblemDetail`

---

### `GET /views/ic/{personId}/drill/{drillId}`

Returns drill-down table for an IC metric.

**Response:** `DrillData`

**Errors:** `401`, `403`, `404` → `ProblemDetail`

---

## Team Drill

### `GET /views/team/drill/{drillId}?period={period}`

Returns drill-down table for a team-level metric.

**Response:** `DrillData`

**Errors:** `401`, `403`, `404` → `ProblemDetail`

---

## Metric Key Catalog

| metric_key | Display label | Unit | Source tag | v1 status |
|---|---|---|---|---|
| `loc` | Lines of Code | LOC | `[git]` | ✓ |
| `ai_loc_share_pct` | AI LOC Share | % | `[ai-code]` | ✓ |
| `ai_sessions` | AI Sessions | sessions | `[ai-code]` | ✓ |
| `prs_merged` | PRs Merged | PRs | `[git]` | ✓ |
| `pr_cycle_time_h` | PR Cycle Time | h | `[git]` | ✓ |
| `pr_review_time_h` | PR Review Time | h | `[git]` | ✓ |
| `focus_time_pct` | Focus Time | % | `[comms]` | ✓ |
| `dev_time_h` | Dev Time | h | `[comms]` | ✓ |
| `headcount` | Headcount | people | `[hr]` | ✓ |
| `tasks_closed` | Tasks Closed | tasks | `[tasks]` | ⚠ pending |
| `bugs_fixed` | Bugs Fixed | bugs | `[tasks]` | ⚠ pending |
| `build_success_pct` | Build Success | % | `[ci]` | ❌ missing |
| `at_risk_count` | At Risk | people | computed | ✓ |
| `focus_gte_60` | Focus ≥ 60% | people / total | computed | ✓ |
| `not_using_ai` | Not Using AI | people | computed | ✓ |
| `ai_adoption_pct` | AI Adoption | % | `[ai-code]` | ✓ |

---

## Frontend Implementation

- `src/screensets/insight/api/insightApiService.ts` — all endpoints
- `src/screensets/insight/api/mocks/` — mock fixtures by screen
- `src/screensets/insight/types/index.ts` — all TypeScript types

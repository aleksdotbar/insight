# Spec: analytics-api

**Service:** Analytics API
**Base path:** `/api/v1/analytics`
**Auth:** `Authorization: Bearer <token>` + `X-Tenant-ID: <tenantId>` required

---

## Overview

Pre-aggregated, screen-ready view endpoints. The backend computes all visual positioning
(`bar_left_pct`, `median_left_pct`, etc.), statuses, medians, and thresholds.
The frontend renders data as received — no client-side aggregation or approximation.

Chart data (`locTrend`, `deliveryTrend`) is returned at the granularity of the requested
period: daily points for `week`, weekly for `month`, monthly for `quarter`, quarterly for `year`.
The frontend charts render the points directly with their `label` as the x-axis key.

---

## Shared Types

```ts
type PeriodValue = 'week' | 'month' | 'quarter' | 'year';

type BulletMetric = {
  period: PeriodValue;
  section: string;
  metric_key: string;
  label: string;
  sublabel?: string;
  value: string;
  unit: string;
  range_min: string;
  range_max: string;
  median: string;
  median_label: string;
  bar_left_pct: number;    // precomputed: position of bar start (0–100)
  bar_width_pct: number;   // precomputed: width of bar (0–100)
  median_left_pct: number; // precomputed: position of median marker (0–100)
  status: 'good' | 'warn' | 'bad';
  drill_id: string;        // empty string if no drill available
};

type BulletSection = {
  id: string;
  title: string;
  metrics: BulletMetric[];
};

type DrillRow = Record<string, string | number>;

type DrillData = {
  title: string;
  source: string;
  srcClass: string;
  value: string;
  filter: string;
  columns: string[];
  rows: DrillRow[];
};
```

---

## Executive View

### `GET /views/executive?period={period}`

**Response:** `ExecViewData`

```ts
type ExecColumnThreshold = {
  metric_key: string;
  threshold: number;   // single pass/fail cutoff (green if ≥ threshold, amber otherwise)
};

type ExecViewConfig = {
  column_thresholds: ExecColumnThreshold[];
};

type ExecTeamRow = {
  team_id: string;
  team_name: string;
  headcount: number;
  tasks_closed: number;
  bugs_fixed: number;
  build_success_pct: number;
  focus_time_pct: number;
  ai_adoption_pct: number;
  ai_loc_share_pct: number;
  pr_cycle_time_h: number;
  status: 'good' | 'warn' | 'bad';
};

type OrgKpis = {
  avgBuildSuccess: number;
  avgAiAdoption: number;
  avgFocus: number;
  bugResolutionScore: number;
  prCycleScore: number;
};

type ExecViewData = {
  teams: ExecTeamRow[];
  orgKpis: OrgKpis;
  config: ExecViewConfig;
};
```

**Errors:** `401`, `403` → `ProblemDetail`

---

## Team View

### `GET /views/team?period={period}`

**Response:** `TeamViewData`

```ts
type TeamKpi = {
  metric_key: string;
  label: string;
  value: string;
  unit: string;
  sublabel?: string;
  chipLabel?: string;
  status: 'good' | 'warn' | 'bad';
  section: string;
};

type TeamMember = {
  person_id: string;
  period: PeriodValue;
  name: string;
  seniority: string;
  tasks_closed: number;
  bugs_fixed: number;
  dev_time_h: number;
  prs_merged: number;
  build_success_pct: number;
  focus_time_pct: number;
  ai_tools: string[];
  ai_loc_share_pct: number;
  trend_label?: string;   // optional: e.g. "3 months declining" — only set when meaningful
};

/**
 * AlertThreshold — drives the Attention Needed section.
 * All fields are plain data (JSON-serializable).
 * The frontend generates description text from metric_key + member values.
 */
type AlertThreshold = {
  metric_key: string;
  trigger: number;   // member is at-risk if value < trigger
  bad: number;       // severity is 'bad' if value < bad, else 'warn'
  reason: string;    // short label, e.g. "Focus time below 60% target"
};

type ColumnThreshold = {
  metric_key: string;
  good: number;            // green if value meets this bound
  warn: number;            // amber if value meets this bound but not good
  higher_is_better: boolean;
};

type TeamViewConfig = {
  alert_thresholds: AlertThreshold[];   // which metrics trigger Attention Needed alerts
  column_thresholds: ColumnThreshold[]; // how to color member table columns
};

type TeamViewData = {
  teamName: string;
  teamKpis: TeamKpi[];       // hero strip KPIs — all values precomputed by backend
  members: TeamMember[];
  bulletSections: BulletSection[];
  config: TeamViewConfig;
};
```

**KPI derivation note:** `at_risk_count`, `focus_gte_60`, and `not_using_ai` KPI values
are computed by the backend from member data using `config.alert_thresholds`.
The frontend does not recompute them.

**Errors:** `401`, `403` → `ProblemDetail`

---

## IC Dashboard

### `GET /views/ic/{personId}?period={period}`

**Response:** `IcDashboardData`

```ts
type PersonData = {
  person_id: string;
  name: string;
  role: string;
  seniority: string;
};

type IcKpi = {
  period: PeriodValue;
  metric_key: string;
  label: string;
  value: string;
  unit: string;
  sublabel: string;
  description?: string;
  delta: string;
  delta_type: 'good' | 'warn' | 'bad' | 'neutral';
};

type TimeOffNotice = {
  days: number;
  dateRange: string;
  bambooHrUrl: string;
};

/**
 * Chart data points — granularity matches period:
 *   week    → daily points (Mon, Tue, Wed, Thu, Fri)
 *   month   → weekly points (W1, W2, W3, W4)
 *   quarter → monthly points (Jan, Feb, Mar)
 *   year    → quarterly points (Q1, Q2, Q3, Q4)
 * The frontend renders label as x-axis key — no client-side aggregation.
 */
type LocDataPoint = {
  label: string;
  aiLoc: number;
  codeLoc: number;
  specLines: number;
};

type DeliveryDataPoint = {
  label: string;
  commits: number;
  prsMerged: number;
  tasksDone: number;
};

type IcChartsData = {
  locTrend: LocDataPoint[];
  deliveryTrend: DeliveryDataPoint[];
};

type IcDashboardData = {
  person: PersonData;
  kpis: IcKpi[];
  bulletMetrics: BulletMetric[];
  charts: IcChartsData;
  timeOffNotice: TimeOffNotice | null;
  drills: Record<string, DrillData>;
};
```

**Errors:** `401`, `403`, `404` (person not found) → `ProblemDetail`

---

### `GET /views/ic/{personId}/drill/{drillId}`

Returns drill-down table data for an IC metric.

**Response:** `DrillData`

**Errors:** `401`, `403`, `404` → `ProblemDetail`

---

## Team Drill

### `GET /views/team/drill/{drillId}?period={period}`

Returns drill-down table data for a team-level metric.

**Response:** `DrillData`

**Errors:** `401`, `403`, `404` → `ProblemDetail`

---

## Frontend Implementation

- `src/screensets/insight/api/insightApiService.ts` — all endpoints above
- `src/screensets/insight/api/mocks/` — mock fixtures by screen
- `src/screensets/insight/types/index.ts` — all TypeScript types

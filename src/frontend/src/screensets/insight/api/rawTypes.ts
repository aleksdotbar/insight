/**
 * Raw API response types
 *
 * TypeScript interfaces for the snake_case rows returned by ClickHouse via
 * the Analytics API.  These mirror the backend column names exactly.
 *
 * The transform layer (transforms.ts) maps these into the UI types defined
 * in ../types/index.ts.
 */

// ---------------------------------------------------------------------------
// Executive View  (POST /metrics/{EXEC_SUMMARY}/query)
// ---------------------------------------------------------------------------

export type RawExecSummaryRow = {
  org_unit_id: string;
  org_unit_name: string;
  headcount: number;
  tasks_closed: number | null;
  bugs_fixed: number | null;
  build_success_pct: number | null;
  // focus_time_pct is non-null only when silver.class_focus_metrics has a row
  // for the org; otherwise the exec_summary view emits NULL. Same for the AI
  // columns when no cursor rows exist for the org/day (LEFT JOIN miss).
  focus_time_pct: number | null;
  ai_adoption_pct: number | null;
  ai_loc_share_pct: number | null;
  // Bitbucket PR ingestion not wired — always NULL today.
  pr_cycle_time_h: number | null;
};

// ---------------------------------------------------------------------------
// IC KPIs  (POST /metrics/{IC_KPIS}/query)
// ---------------------------------------------------------------------------

export type RawIcAggregateRow = {
  person_id: string;
  // loc/prs_merged/pr_cycle_time_h: Bitbucket diffstat + PR ingestion not
  // wired; insight.ic_kpis emits NULL (see CH migration 20260422100000).
  loc: number | null;
  ai_loc_share_pct: number;
  prs_merged: number | null;
  pr_cycle_time_h: number | null;
  focus_time_pct: number;
  tasks_closed: number;
  bugs_fixed: number;
  build_success_pct: number | null;
  ai_sessions: number;
};

// ---------------------------------------------------------------------------
// Team Members  (POST /metrics/{TEAM_MEMBER}/query)
// ---------------------------------------------------------------------------

export type RawTeamMemberRow = {
  person_id: string;
  display_name: string;
  seniority: string;
  supervisor_email: string | null;
  tasks_closed: number;
  bugs_fixed: number;
  // dev_time_h/focus_time_pct/ai_loc_share_pct: upstream source may be
  // missing; insight.team_member emits NULL (CH migration 20260422150000).
  // prs_merged: Bitbucket PR ingestion not wired (CH migration 20260423120000).
  dev_time_h: number | null;
  prs_merged: number | null;
  build_success_pct: number | null;
  focus_time_pct: number | null;
  ai_tools: string[];
  ai_loc_share_pct: number | null;
};

// ---------------------------------------------------------------------------
// Charts  (POST /metrics/{IC_CHART_LOC}/query)
// ---------------------------------------------------------------------------

export type RawLocTrendRow = {
  date_bucket: string;
  ai_loc: number;
  code_loc: number;
  // spec extractor not wired — insight.ic_chart_loc emits NULL.
  spec_lines: number | null;
};

// ---------------------------------------------------------------------------
// Charts  (POST /metrics/{IC_CHART_DELIVERY}/query)
// ---------------------------------------------------------------------------

export type RawDeliveryTrendRow = {
  date_bucket: string;
  commits: number;
  // Bitbucket PR ingestion not wired — insight.ic_chart_delivery emits NULL.
  prs_merged: number | null;
  tasks_done: number;
};

// ---------------------------------------------------------------------------
// Bullet Aggregates  (POST /metrics/{*_BULLET_*}/query)
// ---------------------------------------------------------------------------

export type RawBulletAggregateRow = {
  metric_key: string;
  value: number;
  median: number | null;
  range_min: number | null;
  range_max: number | null;
};

// ---------------------------------------------------------------------------
// Time Off  (POST /metrics/{IC_TIMEOFF}/query)
// ---------------------------------------------------------------------------

export type RawTimeOffRow = {
  days: number;
  date_range: string;
  bamboo_hr_url: string;
};

// ---------------------------------------------------------------------------
// Drill Detail  (POST /metrics/{IC_DRILL}/query)
// ---------------------------------------------------------------------------

export type RawDrillRow = {
  title: string;
  source: string;
  src_class: string;
  value: string;
  filter: string;
  columns: string[];
  rows: Array<Record<string, string | number>>;
};

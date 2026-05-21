/** POST /metrics/{EXEC_SUMMARY}/query */
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

/** POST /metrics/{IC_KPIS}/query */
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

/** POST /metrics/{TEAM_MEMBER}/query */
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

/** POST /metrics/{IC_CHART_LOC}/query */
export type RawLocTrendRow = {
  date_bucket: string;
  ai_loc: number;
  code_loc: number;
  // spec extractor not wired — insight.ic_chart_loc emits NULL.
  spec_lines: number | null;
};

/** POST /metrics/{IC_CHART_DELIVERY}/query */
export type RawDeliveryTrendRow = {
  date_bucket: string;
  commits: number;
  // Bitbucket PR ingestion not wired — insight.ic_chart_delivery emits NULL.
  prs_merged: number | null;
  tasks_done: number;
};

/** POST /metrics/{*_BULLET_*}/query */
export type RawBulletAggregateRow = {
  metric_key: string;
  value: number;
  median: number | null;
  range_min: number | null;
  range_max: number | null;
};

/** POST /metrics/{IC_TIMEOFF}/query */
export type RawTimeOffRow = {
  days: number;
  date_range: string;
  bamboo_hr_url: string;
};

/** POST /metrics/{IC_DRILL}/query */
export type RawDrillRow = {
  title: string;
  source: string;
  src_class: string;
  value: string;
  filter: string;
  columns: string[];
  rows: Array<Record<string, string | number>>;
};

/**
 * POST /metrics/{CRM_KPIS}/query — sales-rep hero strip + pacing band source.
 * CH JSON encodes UInt aggregates as strings, Float64 as numbers; coerce in transforms.
 */
export type RawCrmKpisRow = {
  person_id: string;
  deals_opened: number | string | null;
  deals_closed: number | string | null;
  deals_won: number | string | null;
  deals_value_closed: number | string | null;
  comms_count: number | string | null;
  pipeline_count: number | string | null;
  pipeline_value: number | string | null;
};

/** POST /metrics/{CRM_CHART_FLOW}/query — weekly deal-flow series. */
export type RawCrmFlowRow = {
  date_bucket: string;
  metric_date: string;
  opened: number | string;
  closed: number | string;
  won: number | string;
};

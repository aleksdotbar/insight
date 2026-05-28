export type PeriodValue = "week" | "month" | "quarter" | "year";
export type CustomRange = { from: string; to: string };
export type ViewMode = "chart" | "tile";
export interface PeriodState {
  period: PeriodValue;
  customRange: CustomRange | null;
  scale: number;
}

export interface ODataParams {
  $filter?: string;
  $orderby?: string;
  $top?: number;
  $select?: string;
  $skip?: string;
}
export interface ODataResponse<T> {
  items: T[];
  page_info: { has_next: boolean; cursor: string | null };
}
export interface ProblemFieldViolation {
  field: string;
  description?: string;
  reason?: string;
}
export interface ProblemContext {
  resource_type?: string;
  resource_name?: string;
  field_violations?: ProblemFieldViolation[];
}
export interface ProblemDetails {
  type: string;
  title: string;
  status: number;
  detail: string;
  context?: ProblemContext;
}
export type ThresholdLevel = "good" | "warning" | "critical";
export type Thresholds = Record<string, ThresholdLevel>;

export type ConnectorAvailability = "available" | "no-connector" | "syncing";
export interface ConnectorStatus {
  id: string;
  name: string;
  status: ConnectorAvailability;
}
export type DataAvailability = {
  git: ConnectorAvailability;
  tasks: ConnectorAvailability;
  ci: ConnectorAvailability;
  comms: ConnectorAvailability;
  hr: ConnectorAvailability;
  ai: ConnectorAvailability;
};

export interface ExecColumnThreshold {
  metric_key: string;
  threshold: number;
}
export interface ExecViewConfig {
  column_thresholds: ExecColumnThreshold[];
}
export interface ExecTeamRow {
  team_id: string;
  team_name: string;
  headcount: number;
  tasks_closed: number | null;
  bugs_fixed: number | null;
  build_success_pct: number | null;
  focus_time_pct: number | null;
  ai_adoption_pct: number | null;
  ai_loc_share_pct: number | null;
  pr_cycle_time_h: number | null;
  status: "good" | "warn" | "bad";
}
export interface OrgKpis {
  avgBuildSuccess: number | null;
  avgAiAdoption: number | null;
  avgFocus: number | null;
}
export interface ExecViewData {
  teams: ExecTeamRow[];
  orgKpis: OrgKpis;
  config: ExecViewConfig;
}

export interface TeamKpi {
  metric_key: string;
  label: string;
  value: string;
  unit: string;
  sublabel?: string;
  chipLabel?: string;
  description?: string;
  status: "good" | "warn" | "bad";
  section: string;
}
export interface TeamMember {
  person_id: string;
  period: PeriodValue;
  name: string;
  seniority: string;
  supervisor_email: string | null;
  tasks_closed: number;
  bugs_fixed: number;
  dev_time_h: number | null;
  prs_merged: number | null;
  build_success_pct: number | null;
  focus_time_pct: number | null;
  ai_tools: string[];
  ai_loc_share_pct: number | null;
}
export interface BulletMetric {
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
  bar_left_pct: number;
  bar_width_pct: number;
  median_left_pct: number;
  status: "good" | "warn" | "bad" | "unavailable";
  drill_id: string;
}
export interface BulletSection {
  id: string;
  title: string;
  metrics: BulletMetric[];
}
export interface AlertThreshold {
  metric_key: string;
  trigger: number;
  bad: number;
  reason: string;
}
export interface ColumnThreshold {
  metric_key: string;
  good: number;
  warn: number;
  higher_is_better: boolean;
}
export interface TeamViewConfig {
  alert_thresholds: AlertThreshold[];
  column_thresholds: ColumnThreshold[];
}
export interface TeamViewData {
  teamName: string;
  teamKpis: TeamKpi[];
  members: TeamMember[];
  bulletSections: BulletSection[];
  config: TeamViewConfig;
}

export interface PersonData {
  person_id: string;
  name: string;
  role: string;
  seniority: string;
}
export interface IcKpi {
  period: PeriodValue;
  metric_key: string;
  label: string;
  value: string | null;
  raw_value: number | null;
  unit: string;
  sublabel: string;
  description?: string;
  delta: string;
  delta_type: "good" | "warn" | "bad" | "neutral";
}
export interface TimeOffNotice {
  days: number;
  dateRange: string;
  bambooHrUrl: string;
}
export interface LocDataPoint {
  label: string;
  aiLoc: number;
  codeLoc: number;
  specLines: number | null;
}
export interface DeliveryDataPoint {
  label: string;
  commits: number;
  prsMerged: number | null;
  tasksDone: number;
}
export interface IcChartsData {
  locTrend: LocDataPoint[];
  deliveryTrend: DeliveryDataPoint[];
}
export interface DrillRow {
  [key: string]: string | number;
}
export interface DrillData {
  title: string;
  source: string;
  srcClass: string;
  value: string;
  filter: string;
  columns: string[];
  rows: DrillRow[];
}
export interface IcDashboardData {
  kpis: IcKpi[];
  bulletMetrics: BulletMetric[];
  charts: IcChartsData;
  timeOffNotice: TimeOffNotice | null;
  drills: Record<string, DrillData>;
}

export interface CrmKpis {
  dealsOpened: number;
  dealsClosed: number;
  dealsWon: number;
  dealsValueClosed: number;
  commsCount: number;
  pipelineCount: number;
  pipelineValue: number;
}

export interface CrmFlowPoint {
  label: string;
  opened: number;
  closed: number;
  won: number;
}

/** Lightweight projection of the C# PersonResponse shape. */
export interface IdentityPerson {
  person_id: string;
  email: string;
  display_name: string;
  first_name?: string;
  last_name?: string;
  department?: string;
  division?: string;
  job_title?: string;
  status?: string;
  parent_email?: string | null;
  parent_id?: string | null;
  parent_person_id?: string | null;
  supervisor_email?: string | null;
  supervisor_name?: string | null;
  subordinates: IdentityPerson[];
}

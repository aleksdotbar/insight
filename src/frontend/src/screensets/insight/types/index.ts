/**
 * insight Types
 * Shared type definitions for this screenset
 */

export type UserRole = 'executive' | 'team_lead' | 'ic';

export interface CurrentUser {
  personId: string;
  name: string;
  role: UserRole;
  teamId: string;
}

export type ChartDataPoint = {
  date: string;
  aiLoc: number;
  commitLoc: number;
};

export type StatCard = {
  key: string;
  value: string;
  sub: string;
};

export type BottomMetric = {
  key: string;
  value: string;
};

export type PeriodSplit = {
  label: string;
  value: number;
};

export type DashboardData = {
  stats: StatCard[];
  chartData: ChartDataPoint[];
  bottomMetrics: BottomMetric[];
  periodSplits: PeriodSplit[];
  periodDelta: number;
};


export type SpeedData = {
  value: number;
  min: number;
  max: number;
  unit: string;
  label: string;
};

// Period
export type PeriodValue = 'week' | 'month' | 'quarter' | 'year';
export type CustomRange = { from: string; to: string };
export type ViewMode = 'chart' | 'tile';
export interface PeriodState {
  period: PeriodValue;
  customRange: CustomRange | null;
  scale: number;
}

// Executive View
export interface ExecTeamRow {
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
}
export interface OrgKpis {
  avgBuildSuccess: number;
  avgAiAdoption: number;
  avgFocus: number;
  bugResolutionScore: number;
  prCycleScore: number;
}
export interface ExecViewData {
  teams: ExecTeamRow[];
  orgKpis: OrgKpis;
}

// Team View
export interface TeamKpi {
  metric_key: string;
  label: string;
  value: string;
  unit: string;
  sublabel?: string;
  chipLabel?: string;
  status: 'good' | 'warn' | 'bad';
  section: string;
}
export interface TeamMember {
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
  status: 'good' | 'warn' | 'bad';
  drill_id: string;
}
export interface BulletSection {
  id: string;
  title: string;
  metrics: BulletMetric[];
}
export interface TeamViewData {
  teamName: string;
  teamKpis: TeamKpi[];
  members: TeamMember[];
  bulletSections: BulletSection[];
}

// IC Dashboard
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
  value: string;
  unit: string;
  sublabel: string;
  delta: string;
  delta_type: 'good' | 'warn' | 'bad' | 'neutral';
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
  specLines: number;
}
export interface DeliveryDataPoint {
  label: string;
  commits: number;
  prsMerged: number;
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
  person: PersonData;
  kpis: IcKpi[];
  bulletMetrics: BulletMetric[];
  charts: IcChartsData;
  timeOffNotice: TimeOffNotice | null;
  drills: Record<string, DrillData>;
}

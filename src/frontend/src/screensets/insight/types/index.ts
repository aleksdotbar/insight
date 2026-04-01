/**
 * insight Types
 * Shared type definitions for this screenset
 */

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

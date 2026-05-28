function hashStr(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++) {
    h = ((h << 5) - h + s.charCodeAt(i)) | 0;
  }
  return Math.abs(h);
}

function rng(seed: number): () => number {
  let s = seed || 1;
  return () => {
    s = (s * 9301 + 49297) % 233280;
    return s / 233280;
  };
}

export interface HistogramBin {
  metric_key: string;
  bin: number;
  bin_end: number;
  count: number;
}

export function mockHistogramBins(
  personId: string,
  metricKey: string,
  periodDays = 30,
): HistogramBin[] {
  const scale = periodDays / 30;
  const r = rng(hashStr(`${personId}|${metricKey}|hist`));
  const binCount = 8;
  const bins: HistogramBin[] = [];
  const max = 50 + Math.floor(r() * 50);
  const step = Math.ceil(max / binCount);
  for (let i = 0; i < binCount; i++) {
    bins.push({
      metric_key: metricKey,
      bin: i * step,
      bin_end: (i + 1) * step,
      count: Math.max(1, Math.round((2 + r() * 14) * scale)),
    });
  }
  return bins;
}

export interface PeerCohortStat {
  metric_key: string;
  p25: number;
  p50: number;
  p75: number;
  min: number;
  max: number;
  n: number;
}

const COHORT_METRICS = [
  "bugs_fixed",
  "ai_loc_share",
  "focus_time_pct",
  "tasks_closed",
  "prs_merged",
  "pr_cycle_time_h",
  "ai_sessions",
  "tasks_completed",
  "task_dev_time",
  "task_reopen_rate",
  "due_date_compliance",
  "estimation_accuracy",
  "bugs_to_task_ratio",
  "mean_time_to_resolution",
  "flow_efficiency",
  "pickup_time",
  "commits",
  "prs_created",
  "clean_loc",
  "pr_size",
  "merge_rate",
  "lines_per_commit",
  "commits_per_active_day",
  "prs_per_dev",
  "build_success",
  "pr_cycle_time",
  "active_ai_members",
  "cursor_active",
  "cc_active",
  "team_ai_loc",
  "cursor_acceptance",
  "cc_tool_acceptance",
  "cursor_lines",
  "cc_lines",
  "ai_loc_share2",
  "slack_messages_sent",
  "slack_active_days",
  "slack_dm_ratio",
  "m365_emails_sent",
  "m365_emails_received",
  "m365_teams_chats",
  "meeting_hours",
  "meetings_count",
  "meeting_free",
];

const NON_SCALING_COHORT_KEYS = new Set<string>([
  "build_success",
  "focus_time_pct",
  "ai_loc_share",
  "ai_loc_share2",
  "merge_rate",
  "due_date_compliance",
  "task_reopen_rate",
  "estimation_accuracy",
  "bugs_to_task_ratio",
  "flow_efficiency",
  "cursor_acceptance",
  "cc_tool_acceptance",
  "slack_dm_ratio",
  "pr_cycle_time",
  "pr_cycle_time_h",
  "mean_time_to_resolution",
  "task_dev_time",
  "pickup_time",
  "pr_size",
]);

export function mockPeerCohortStats(
  seed: string,
  periodDays = 30,
): PeerCohortStat[] {
  const scale = periodDays / 30;
  return COHORT_METRICS.map((metric_key) => {
    const r = rng(hashStr(`${seed}|${metric_key}`));
    const base = 10 + r() * 90;
    const spread = 5 + r() * 15;
    const shouldScale = !NON_SCALING_COHORT_KEYS.has(metric_key);
    const factor = shouldScale ? scale : 1;
    return {
      metric_key,
      p25: Math.round((base - spread) * factor * 10) / 10,
      p50: Math.round(base * factor * 10) / 10,
      p75: Math.round((base + spread) * factor * 10) / 10,
      min: Math.round((base - spread * 2) * factor * 10) / 10,
      max: Math.round((base + spread * 2) * factor * 10) / 10,
      n: 12,
    };
  });
}

export interface KpiPeerMedianRow {
  kpi_key: string;
  p50: number;
  n: number;
}

const KPI_PEER_BASELINES: Record<string, { base: number; spread: number; scaling: boolean }> = {
  bugs_fixed:      { base: 4,   spread: 2,  scaling: true },
  ai_loc_share:    { base: 22,  spread: 8,  scaling: false },
  focus_time_pct:  { base: 60,  spread: 12, scaling: false },
  tasks_closed:    { base: 10,  spread: 4,  scaling: true },
  prs_merged:      { base: 6,   spread: 3,  scaling: true },
  pr_cycle_time_h: { base: 22,  spread: 8,  scaling: false },
  ai_sessions:     { base: 28,  spread: 10, scaling: true },
};

export function mockKpiPeerMedians(
  seed: string,
  periodDays = 30,
): KpiPeerMedianRow[] {
  const scale = periodDays / 30;
  return Object.entries(KPI_PEER_BASELINES).map(([kpi_key, def]) => {
    const r = rng(hashStr(`${seed}|kpi-median|${kpi_key}`));
    const factor = def.scaling ? scale : 1;
    const value = def.base + (r() - 0.5) * 2 * def.spread;
    return {
      kpi_key,
      p50: Math.max(0, Math.round(value * factor * 10) / 10),
      n: 6,
    };
  });
}

const SECTION_TREND_SERIES: Record<string, { key: string; base: number; spread: number }[]> = {
  code_quality: [
    { key: "pr_cycle_time", base: 22, spread: 12 },
    { key: "build_success", base: 88, spread: 8 },
  ],
  ai_adoption: [
    { key: "cc_lines", base: 180, spread: 90 },
    { key: "cursor_lines", base: 240, spread: 120 },
  ],
};

export interface SectionTrendLongRow {
  metric_date: string;
  section_id: string;
  series_key: string;
  value: number;
}

export function mockSectionTrend(
  personId: string,
  sectionId: string,
  periodDays: number,
): SectionTrendLongRow[] {
  const defs = SECTION_TREND_SERIES[sectionId];
  if (!defs) return [];
  const days = Math.max(7, Math.min(periodDays, 90));
  const out: SectionTrendLongRow[] = [];
  const today = new Date();
  for (let i = days - 1; i >= 0; i--) {
    const d = new Date(today);
    d.setDate(d.getDate() - i);
    const metric_date = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
    for (const def of defs) {
      const r = rng(hashStr(`${personId}|${sectionId}|${def.key}|${i}`));
      const v = def.base + (r() - 0.5) * 2 * def.spread;
      out.push({
        metric_date,
        section_id: sectionId,
        series_key: def.key,
        value: Math.max(0, Math.round(v * 10) / 10),
      });
    }
  }
  return out;
}

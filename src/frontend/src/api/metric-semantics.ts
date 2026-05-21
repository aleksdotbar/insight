export type MetricSemantics = {
  metric_key: string;
  unit: string;
  higher_is_better: boolean;
  /** Status threshold — values from here onwards are "good". */
  good: number;
  /** Status threshold — below "good" but not yet "bad". */
  warn: number;
  /** Team-view AttentionNeeded alert config, when applicable. */
  alert?: {
    trigger: number;
    bad: number;
    reason: string;
  };
};

export const METRIC_SEMANTICS = {
  build_success_pct: {
    metric_key: 'build_success_pct',
    unit: '%',
    higher_is_better: true,
    good: 90,
    warn: 80,
    alert: { trigger: 90, bad: 80, reason: 'Build success rate below 90% target' },
  },
  focus_time_pct: {
    metric_key: 'focus_time_pct',
    unit: '%',
    higher_is_better: true,
    good: 60,
    warn: 50,
    alert: { trigger: 60, bad: 48, reason: 'Focus time below 60% target' },
  },
  ai_adoption_pct: {
    metric_key: 'ai_adoption_pct',
    unit: '%',
    higher_is_better: true,
    good: 60,
    warn: 40,
  },
  ai_loc_share_pct: {
    metric_key: 'ai_loc_share_pct',
    unit: '%',
    higher_is_better: true,
    good: 20,
    warn: 10,
    alert: { trigger: 10, bad: 8, reason: 'Low AI tool adoption' },
  },
  dev_time_h: {
    metric_key: 'dev_time_h',
    unit: 'h',
    higher_is_better: false,
    good: 14,
    warn: 20,
  },
  bugs_fixed: {
    metric_key: 'bugs_fixed',
    unit: '',
    higher_is_better: true,
    good: 15,
    warn: 8,
  },
} as const satisfies Record<string, MetricSemantics>;

export type MetricSemanticsKey = keyof typeof METRIC_SEMANTICS;

// Fractions of headcount — chip status scales with team size instead of
// using a fixed absolute rule that collapses for teams larger than a few
// people.
export const TEAM_HEALTH_THRESHOLDS = {
  /** From this fraction onward, and below `badPct` → status is 'warn'. */
  warnPct: 0.10,
  /** From this fraction onward → status is 'bad'. */
  badPct: 0.25,
} as const;

/**
 * Translate a count of affected members into a status chip, scaled against
 * team size. `count===0` is always 'good' so an all-healthy team still reads
 * 'good' regardless of how small the team is.
 */
export function teamHealthStatus(
  count: number,
  teamSize: number,
): 'good' | 'warn' | 'bad' {
  if (count <= 0 || teamSize <= 0) return 'good';
  const pct = count / teamSize;
  if (pct >= TEAM_HEALTH_THRESHOLDS.badPct) return 'bad';
  if (pct >= TEAM_HEALTH_THRESHOLDS.warnPct) return 'warn';
  return 'good';
}

/** Shared status evaluator — same rule on every screen. */
export function evaluateStatus(
  value: number,
  sem: Pick<MetricSemantics, 'good' | 'warn' | 'higher_is_better'>,
): 'good' | 'warn' | 'bad' {
  if (sem.higher_is_better) {
    if (value >= sem.good) return 'good';
    if (value >= sem.warn) return 'warn';
    return 'bad';
  }
  if (value <= sem.good) return 'good';
  if (value <= sem.warn) return 'warn';
  return 'bad';
}

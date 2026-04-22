/**
 * Metric Semantics — single source of truth for metric thresholds and
 * directionality across Executive, Team, and IC views.
 *
 * Previously, the same metric was defined with divergent good/warn/alert
 * thresholds in three places (EXEC_VIEW_CONFIG.column_thresholds,
 * TEAM_VIEW_CONFIG.alert_thresholds, TEAM_VIEW_CONFIG.column_thresholds),
 * which meant the same value could show "warn" on one screen and "good" on
 * another. This module consolidates them.
 *
 * Status rules:
 *   higher_is_better=true  → value >= good: good; >= warn: warn; else bad.
 *   higher_is_better=false → value <= good: good; <= warn: warn; else bad.
 *
 * Alert rules (for Team View AttentionNeeded):
 *   An alert fires when value is worse than the `alert.trigger` level, and
 *   becomes severe below `alert.bad`. Omit `alert` for metrics that do not
 *   participate in team-level alerts.
 */

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

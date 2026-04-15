/**
 * View configuration objects
 *
 * Static config used by action files to derive computed KPIs and apply thresholds.
 * These were previously in fixture files but are runtime config, not test data.
 */

import type {
  ExecViewConfig,
  TeamViewConfig,
  TeamKpi,
  PeriodValue,
} from '../types';

// ---------------------------------------------------------------------------
// Executive View Config
// ---------------------------------------------------------------------------

export const EXEC_VIEW_CONFIG: ExecViewConfig = {
  column_thresholds: [
    { metric_key: 'build_success_pct', threshold: 90 },
    { metric_key: 'focus_time_pct',    threshold: 60 },
    { metric_key: 'ai_adoption_pct',   threshold: 60 },
  ],
};

// ---------------------------------------------------------------------------
// Team View Config -- thresholds configurable by backend / team lead
// ---------------------------------------------------------------------------

export const TEAM_VIEW_CONFIG: TeamViewConfig = {
  alert_thresholds: [
    { metric_key: 'build_success_pct', trigger: 90, bad: 80, reason: 'Build success rate below 90% target' },
    { metric_key: 'focus_time_pct',    trigger: 60, bad: 48, reason: 'Focus time below 60% target' },
    { metric_key: 'ai_loc_share_pct',  trigger: 10, bad: 8,  reason: 'Low AI tool adoption' },
  ],
  column_thresholds: [
    { metric_key: 'bugs_fixed',        good: 15, warn: 8,  higher_is_better: true },
    { metric_key: 'dev_time_h',        good: 14, warn: 20, higher_is_better: false },
    { metric_key: 'build_success_pct', good: 90, warn: 80, higher_is_better: true },
    { metric_key: 'focus_time_pct',    good: 60, warn: 50, higher_is_better: true },
    { metric_key: 'ai_loc_share_pct',  good: 20, warn: 10, higher_is_better: true },
  ],
};

// ---------------------------------------------------------------------------
// Team KPIs by period (4 entries: week, month, quarter, year)
// ---------------------------------------------------------------------------

export const TEAM_KPIS_BY_PERIOD: Record<PeriodValue, TeamKpi[]> = {
  week: [
    { metric_key: 'at_risk_count', label: 'At Risk', value: '1', unit: '', sublabel: 'Declining across 2+ metrics this week', chipLabel: 'Needs attention', status: 'warn', section: 'overview' },
    { metric_key: 'team_dev_time', label: 'Task Dev Time', value: '13h', unit: '', sublabel: 'Team median \u00b7 Task Delivery', chipLabel: '\u2193 vs company 16h', status: 'good', section: 'overview' },
    { metric_key: 'focus_gte_60', label: 'Focus \u226560%', value: '10 / 12', unit: '', sublabel: '2 members below target', chipLabel: 'On track', status: 'good', section: 'overview' },
    { metric_key: 'not_using_ai', label: 'Not Using AI Tools', value: '6', unit: '', sublabel: 'No AI activity logged this week', chipLabel: 'Action needed', status: 'bad', section: 'overview' },
  ],
  month: [
    { metric_key: 'at_risk_count', label: 'At Risk', value: '2', unit: '', sublabel: 'Declining across 2+ metrics for 2+ months', chipLabel: 'Needs attention', status: 'bad', section: 'overview' },
    { metric_key: 'team_dev_time', label: 'Task Dev Time', value: '14h', unit: '', sublabel: 'Team median \u00b7 Task Delivery', chipLabel: '\u2193 vs company 16h', status: 'good', section: 'overview' },
    { metric_key: 'focus_gte_60', label: 'Focus \u226560%', value: '9 / 12', unit: '', sublabel: '3 members below target', chipLabel: 'Monitor closely', status: 'warn', section: 'overview' },
    { metric_key: 'not_using_ai', label: 'Not Using AI Tools', value: '5', unit: '', sublabel: 'No AI activity logged this month', chipLabel: 'Action needed', status: 'bad', section: 'overview' },
  ],
  quarter: [
    { metric_key: 'at_risk_count', label: 'At Risk', value: '3', unit: '', sublabel: 'Declining across 2+ metrics for 2+ quarters', chipLabel: 'Needs attention', status: 'bad', section: 'overview' },
    { metric_key: 'team_dev_time', label: 'Task Dev Time', value: '15h', unit: '', sublabel: 'Team median \u00b7 Task Delivery', chipLabel: '\u2191 vs company 16h', status: 'warn', section: 'overview' },
    { metric_key: 'focus_gte_60', label: 'Focus \u226560%', value: '8 / 12', unit: '', sublabel: '4 members below target', chipLabel: 'Monitor closely', status: 'warn', section: 'overview' },
    { metric_key: 'not_using_ai', label: 'Not Using AI Tools', value: '4', unit: '', sublabel: 'No AI activity logged this quarter', chipLabel: 'Action needed', status: 'bad', section: 'overview' },
  ],
  year: [
    { metric_key: 'at_risk_count', label: 'At Risk', value: '2', unit: '', sublabel: 'Declining across 2+ metrics over the year', chipLabel: 'Needs attention', status: 'bad', section: 'overview' },
    { metric_key: 'team_dev_time', label: 'Task Dev Time', value: '14h', unit: '', sublabel: 'Team median \u00b7 Task Delivery \u00b7 annual avg', chipLabel: '\u2193 vs company 16h', status: 'good', section: 'overview' },
    { metric_key: 'focus_gte_60', label: 'Focus \u226560%', value: '9 / 12', unit: '', sublabel: '3 members below target \u00b7 annual avg', chipLabel: 'Monitor closely', status: 'warn', section: 'overview' },
    { metric_key: 'not_using_ai', label: 'Not Using AI Tools', value: '5', unit: '', sublabel: 'No AI activity logged this year', chipLabel: 'Action needed', status: 'bad', section: 'overview' },
  ],
};

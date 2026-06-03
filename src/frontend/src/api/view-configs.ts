/**
 * Catalog-driven view-config hooks (Refs #79, follow-up to #66/#78).
 *
 * Replaces the previous module-level `EXEC_VIEW_CONFIG` / `TEAM_VIEW_CONFIG`
 * consts. Threshold values are sourced from `useCatalog()` under the
 * `view_configs.<bare_key>` wire prefix; compile-in defaults survive only
 * as the fallback rows surfaced by `useCatalog` when the API is unreachable
 * (per DESIGN §3.3 transitional-fallback policy).
 *
 * Catalog rows with `schema_status='error'` are omitted from the returned
 * threshold lists so the rule never fires for a broken column — the
 * downstream consumers (`MembersTable`, `TeamsTable`, `OrgKpiCards`,
 * `AttentionNeeded`) read the policy lookup as "no threshold => neutral
 * coloring, no alert", which is the intended render per the wave-1 contract.
 */

import { useMemo } from 'react';

import { useCatalog } from '@/api/use-catalog';
import type {
  ExecViewConfig,
  TeamViewConfig,
  TeamKpi,
  PeriodValue,
} from '@/types/insight';

const VIEW_CONFIG_PREFIX = 'view_configs.';

const EXEC_COLUMN_KEYS = ['build_success_pct', 'focus_time_pct', 'ai_adoption_pct'] as const;
const TEAM_ALERT_KEYS = ['build_success_pct', 'focus_time_pct', 'ai_loc_share_pct'] as const;
const TEAM_COLUMN_KEYS = [
  'bugs_fixed',
  'dev_time_h',
  'build_success_pct',
  'focus_time_pct',
  'ai_loc_share_pct',
] as const;

export function useExecViewConfig(): ExecViewConfig {
  const { data } = useCatalog();
  return useMemo(() => {
    const byKey = indexByMetricKey(data?.metrics ?? []);
    return {
      column_thresholds: EXEC_COLUMN_KEYS.flatMap((key) => {
        const m = byKey.get(`${VIEW_CONFIG_PREFIX}${key}`);
        if (!m || m.schema_status === 'error') return [];
        return [{ metric_key: key, threshold: m.thresholds.good }];
      }),
    };
  }, [data]);
}

export function useTeamViewConfig(): TeamViewConfig {
  const { data } = useCatalog();
  return useMemo(() => {
    const byKey = indexByMetricKey(data?.metrics ?? []);
    return {
      alert_thresholds: TEAM_ALERT_KEYS.flatMap((key) => {
        const m = byKey.get(`${VIEW_CONFIG_PREFIX}${key}`);
        if (!m || m.schema_status === 'error') return [];
        const t = m.thresholds;
        if (t.alert_trigger === undefined || t.alert_bad === undefined) return [];
        return [
          {
            metric_key: key,
            trigger: t.alert_trigger,
            bad: t.alert_bad,
            // alert_reason is optional on the wire; fall through to a
            // generic message when the backend omits it.
            reason: t.alert_reason ?? `${key} below target`,
          },
        ];
      }),
      column_thresholds: TEAM_COLUMN_KEYS.flatMap((key) => {
        const m = byKey.get(`${VIEW_CONFIG_PREFIX}${key}`);
        if (!m || m.schema_status === 'error') return [];
        return [
          {
            metric_key: key,
            good: m.thresholds.good,
            warn: m.thresholds.warn,
            higher_is_better: m.higher_is_better,
          },
        ];
      }),
    };
  }, [data]);
}

function indexByMetricKey<T extends { metric_key?: string }>(
  metrics: T[],
): Map<string, T> {
  const out = new Map<string, T>();
  for (const m of metrics) {
    if (m.metric_key && !out.has(m.metric_key)) out.set(m.metric_key, m);
  }
  return out;
}

// Structural metadata only. value / sublabel / chipLabel / status are
// computed by `useTeamKpis` from the actual members for the period — when
// no members come back, useTeamKpis returns [] and TeamHeroStrip renders
// <ComingSoon>. We never substitute hardcoded numbers.
const TEAM_KPI_TEMPLATES: TeamKpi[] = [
  {
    metric_key: 'at_risk_count',
    label: 'At Risk',
    value: '',
    unit: '',
    description:
      'Members whose key metrics (tasks, focus, build, AI adoption) are declining across two or more dimensions.',
    chipLabel: 'Needs attention',
    status: 'warn',
    section: 'overview',
  },
  {
    metric_key: 'team_dev_time',
    label: 'Task Dev Time',
    value: '',
    unit: '',
    description:
      'Team median time a task spends in In Progress state. Lower means tasks flow through development faster.',
    chipLabel: '',
    status: 'good',
    section: 'overview',
  },
  {
    metric_key: 'focus_gte_60',
    label: 'Focus ≥60%',
    value: '',
    unit: '',
    description:
      "Members with 60%+ of their work time in uninterrupted 60-minute+ blocks. Fewer interruptions enables deeper, more effective work.",
    chipLabel: 'On track',
    status: 'good',
    section: 'overview',
  },
  {
    metric_key: 'not_using_ai',
    label: 'Not Using AI Tools',
    value: '',
    unit: '',
    description:
      'Members with no AI tool activity (Cursor, Claude Code, Codex) logged in the selected period.',
    chipLabel: 'Action needed',
    status: 'bad',
    section: 'overview',
  },
];

const TEAM_KPIS_BY_PERIOD: Record<PeriodValue, TeamKpi[]> = {
  week: TEAM_KPI_TEMPLATES,
  month: TEAM_KPI_TEMPLATES,
  quarter: TEAM_KPI_TEMPLATES,
  year: TEAM_KPI_TEMPLATES,
};

/**
 * Period-keyed team-KPI templates. Currently identical across periods, but
 * exposed as a hook so a future catalog-driven override (label / sublabel /
 * description) has an obvious entry point without rewriting consumers.
 */
export function useTeamKpisByPeriod(period: PeriodValue): TeamKpi[] {
  return useMemo(
    () => TEAM_KPIS_BY_PERIOD[period] ?? TEAM_KPIS_BY_PERIOD.month,
    [period],
  );
}

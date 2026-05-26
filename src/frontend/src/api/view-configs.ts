import type {
  ExecViewConfig,
  TeamViewConfig,
  TeamKpi,
  PeriodValue,
} from '@/types/insight';
import { METRIC_SEMANTICS } from './metric-semantics';

// Derived from METRIC_SEMANTICS — adjust a threshold there and every view
// picks it up.

export const EXEC_VIEW_CONFIG: ExecViewConfig = {
  column_thresholds: (['build_success_pct', 'focus_time_pct', 'ai_adoption_pct'] as const).map(
    (key) => ({ metric_key: key, threshold: METRIC_SEMANTICS[key].good }),
  ),
};

export const TEAM_VIEW_CONFIG: TeamViewConfig = {
  alert_thresholds: (['build_success_pct', 'focus_time_pct', 'ai_loc_share_pct'] as const)
    .flatMap((key) => {
      const alert = METRIC_SEMANTICS[key].alert;
      return alert ? [{ metric_key: key, ...alert }] : [];
    }),
  column_thresholds: (
    ['bugs_fixed', 'dev_time_h', 'build_success_pct', 'focus_time_pct', 'ai_loc_share_pct'] as const
  ).map((key) => {
    const sem = METRIC_SEMANTICS[key];
    return {
      metric_key: key,
      good: sem.good,
      warn: sem.warn,
      higher_is_better: sem.higher_is_better,
    };
  }),
};

// Structural metadata only. value / sublabel / chipLabel / status are
// computed by `deriveTeamKpis` from the actual members for the period — when
// no members come back, deriveTeamKpis returns [] and TeamHeroStrip renders
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
    label: 'Focus \u226560%',
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

export const TEAM_KPIS_BY_PERIOD: Record<PeriodValue, TeamKpi[]> = {
  week:    TEAM_KPI_TEMPLATES,
  month:   TEAM_KPI_TEMPLATES,
  quarter: TEAM_KPI_TEMPLATES,
  year:    TEAM_KPI_TEMPLATES,
};

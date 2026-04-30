/**
 * Bullet + IC KPI descriptor catalog.
 *
 * Only structural metadata lives here (label, sublabel, unit, section,
 * drill_id, higher_is_better, format) plus status thresholds (`good` /
 * `warn`) that drive bullet color on the FE.
 *
 * Numeric distribution defaults (range_min / range_max / median) were
 * previously extracted from hand-crafted mock fixtures and used as
 * fallbacks when the backend omitted them. Those fallbacks lied about
 * real data and are gone: when the backend doesn't supply distribution,
 * the bullet is marked `status:'unavailable'` and renders ComingSoon
 * inline instead of drawing a fake bar.
 *
 * `good` / `warn` are FE operational thresholds (policy), not data.
 * Tenant-specific overrides should eventually flow through a config API
 * — see the TODO in `metricSemantics.ts`.
 */

import type { BulletMetric } from '../types';
import type { RawIcAggregateRow } from './rawTypes';

// ---------------------------------------------------------------------------
// Bullet metric definitions
// ---------------------------------------------------------------------------

export type BulletThresholdDef = {
  metric_key: string;
  section: string;
  label: string;
  /**
   * Sublabel shown on IC view — describes the person's own value.
   * Use plain "period total" / "daily avg" without aggregation hints.
   */
  sublabel: string;
  /**
   * Optional sublabel override for Team view. Team-view value is
   * `avg(per-person v_period)`, so honest sublabels should make that
   * explicit. Falls back to `sublabel` if unset.
   */
  teamSublabel?: string;
  /**
   * Static unit label (e.g. '%', 'h', 'tasks'). Member-scale metrics
   * (active_ai_members, cursor_active, ...) leave this empty — their
   * denominator is team headcount, injected at transform time.
   */
  unit: string;
  drill_id: string;
  higher_is_better: boolean;
  /**
   * Opt-in flag for metrics whose value represents "N members out of team".
   * The transform layer replaces `unit` with `/ ${teamSize}` and scales
   * range_max to team size (falling back to `unavailable` when teamSize is
   * unknown, e.g. IC Dashboard context).
   */
  isMemberScale?: boolean;
  /** Status threshold — values from here onwards are "good". */
  good: number;
  /** Below "good" but not yet "bad". */
  warn: number;
};

/**
 * Sub-grouping within a section. UI composition picks metrics by group name
 * (via `filterBulletsByLayoutGroup`) instead of hand-maintained metric_key
 * arrays sprinkled through screen components. Group names are scoped by
 * section — they only need to be unique within one section.
 *
 * Only sections that use sub-grouping need entries here: `estimation`
 * (3 groups), `ai_adoption` (7 groups), `collaboration` (3 groups).
 * `task_delivery`, `git_output`, `code_quality` don't sub-group — the
 * screen just renders all metrics in the section — so their metric_keys
 * are intentionally absent.
 */
export const BULLET_LAYOUT_GROUPS: Record<string, string> = {
  // --- estimation card sub-groups ---
  estimation_accuracy: 'estimate_accuracy',
  overrun_ratio:       'estimate_accuracy',
  scope_completion:    'sprint_scope',
  scope_creep:         'sprint_scope',
  on_time_delivery:    'deadline',
  avg_slip:            'deadline',

  // --- ai_adoption sub-groups ---
  active_ai_members:  'ai_members',
  cursor_active:      'ai_members',
  cc_active:          'ai_members',
  codex_active:       'ai_members',
  team_ai_loc:        'ai_team_output',
  cursor_acceptance:  'ai_acceptance',
  cc_tool_acceptance: 'ai_acceptance',
  cc_tool_accept:     'ai_acceptance',
  cursor_lines:       'ai_cursor_detail',
  cursor_agents:      'ai_cursor_detail',
  cursor_completions: 'ai_cursor_detail',
  cc_lines:           'ai_cc_detail',
  cc_sessions:        'ai_cc_detail',
  chatgpt:            'ai_web',
  claude_web:         'ai_web',
  ai_loc_share2:      'ai_loc_share',

  // --- collaboration sub-groups ---
  slack_messages_sent:        'slack',
  slack_channel_posts:        'slack',
  slack_active_days:          'slack',
  slack_msgs_per_active_day:  'slack',
  slack_dm_ratio:             'slack',
  m365_active_days:           'm365',
  m365_emails_sent:           'm365',
  m365_emails_received:       'm365',
  m365_emails_read:           'm365',
  m365_teams_chats:           'm365',
  m365_files_engaged:         'm365',
  m365_files_shared_internal: 'm365',
  m365_files_shared_external: 'm365',
  meeting_hours:              'meetings',
  meetings_count:             'meetings',
  teams_meeting_hours:        'meetings',
  zoom_meeting_hours:         'meetings',
  teams_meetings:             'meetings',
  zoom_meetings:              'meetings',
  meeting_free:               'meetings',
};

/**
 * Filter a list of bullet metrics by their layout group (see
 * BULLET_LAYOUT_GROUPS). Helper so screen components don't need to import
 * the map directly and open-code the filter.
 */
export function filterBulletsByLayoutGroup(
  metrics: BulletMetric[],
  group: string,
): BulletMetric[] {
  // Order by BULLET_DEFS position so display matches the canonical layout
  // regardless of API response ordering (CH GROUP BY is unordered).
  const filtered = metrics.filter((m) => BULLET_LAYOUT_GROUPS[m.metric_key] === group);
  const orderIndex = new Map(BULLET_DEFS.map((d, i) => [d.metric_key, i]));
  return filtered.sort(
    (a, b) =>
      (orderIndex.get(a.metric_key) ?? Number.MAX_SAFE_INTEGER) -
      (orderIndex.get(b.metric_key) ?? Number.MAX_SAFE_INTEGER),
  );
}

/**
 * Team-level bullet metric definitions.
 *
 * Sections match the IDs used by the Team View bullet sections:
 *   task_delivery, code_quality, estimation, ai_adoption, collaboration
 *
 * Aggregation semantics noted in each sublabel ("period total" / "daily avg"
 * / "any activity") must match the inner aggregation the backend applies for
 * that metric_key (see analytics-api seed migration). Team-view `value` is
 * the outer avg across people; IC-view `value` is the person's own inner
 * result compared against their team's distribution.
 */
export const BULLET_DEFS: BulletThresholdDef[] = [
  // --- task_delivery ---
  { metric_key: 'tasks_completed',    section: 'task_delivery', label: 'Tasks Closed / Developer',           sublabel: 'Jira \u00b7 closed issues \u00b7 per developer · period total', unit: 'tasks',  drill_id: 'team-tasks',     higher_is_better: true,  good: 5,   warn: 3   },
  { metric_key: 'task_dev_time',      section: 'task_delivery', label: 'Task Development Time',              sublabel: 'Jira \u00b7 time in dev statuses \u00b7 per-task median \u00b7 lower = better', unit: 'h', drill_id: 'team-dev-time', higher_is_better: false, good: 15,  warn: 22  },
  { metric_key: 'task_reopen_rate',   section: 'task_delivery', label: 'Task Reopen Rate',                   sublabel: 'Jira \u00b7 reopen events \u00f7 closures (this period) \u00b7 lower = better', unit: '%',      drill_id: 'team-reopen',    higher_is_better: false, good: 5,   warn: 10  },
  { metric_key: 'due_date_compliance', section: 'task_delivery', label: 'Due Date Compliance',               sublabel: 'Jira \u00b7 tasks closed by due date',                                 unit: '%',      drill_id: '',               higher_is_better: true,  good: 72,  warn: 55  },
  // estimation_accuracy: backend computes 100 - |100 - ratio|, higher=better.
  { metric_key: 'estimation_accuracy', section: 'task_delivery', label: 'Estimation Accuracy',               sublabel: 'Jira \u00b7 how close estimate matches actual time',                     unit: '%',     drill_id: '',               higher_is_better: true,  good: 80,  warn: 50  },
  // worklog_logging_accuracy: same symmetric folding (raw daily ratio = worklog seconds \u00f7 dev-status seconds, folded around 100). NULL when either side missing.
  { metric_key: 'worklog_logging_accuracy', section: 'task_delivery', label: 'Worklog Logging Accuracy',     sublabel: 'Jira \u00b7 worklog logged \u00f7 time in dev statuses \u00b7 100 = on target \u00b7 requires both', unit: '%', drill_id: '',          higher_is_better: true,  good: 80,  warn: 50  },
  { metric_key: 'bugs_to_task_ratio',   section: 'task_delivery', label: 'Bugs / Tasks Closed',              sublabel: 'Jira \u00b7 bug-type issues \u00f7 total closed \u00b7 lower = better',  unit: '%',     drill_id: '',               higher_is_better: false, good: 20,  warn: 35  },
  { metric_key: 'mean_time_to_resolution', section: 'task_delivery', label: 'Mean Time to Resolution',       sublabel: 'Jira \u00b7 close \u2212 create \u00b7 lower = better',                  unit: 'd',     drill_id: '',               higher_is_better: false, good: 7,   warn: 14  },
  { metric_key: 'stale_in_progress',    section: 'task_delivery', label: 'Stale In-Progress',                sublabel: 'Jira \u00b7 open issues untouched > 14 days \u00b7 snapshot, as of today',                     unit: 'tasks', drill_id: '',               higher_is_better: false, good: 0,   warn: 2   },
  // Flow Efficiency: per-task ratio of dev-status time to total lifetime
  // (created \u2192 final close), period median per person.
  { metric_key: 'flow_efficiency',       section: 'task_delivery', label: 'Flow Efficiency',                  sublabel: 'Jira \u00b7 time in dev statuses \u00f7 lifetime \u00b7 per-task median \u00b7 higher = better',     unit: '%',     drill_id: '',               higher_is_better: true,  good: 40,  warn: 20  },
  // Pickup Time: created \u2192 first dev-status entry, per-task median.
  { metric_key: 'pickup_time',           section: 'task_delivery', label: 'Pickup Time',                      sublabel: 'Jira \u00b7 created \u2192 first dev status \u00b7 per-task median \u00b7 lower = better',           unit: 'd',     drill_id: '',               higher_is_better: false, good: 3,   warn: 7   },

  // --- git_output ---
  { metric_key: 'commits',            section: 'git_output',    label: 'Commits Authored',                   sublabel: 'Bitbucket \u00b7 commits authored \u00b7 period total',                 unit: 'count',  drill_id: '',               higher_is_better: true,  good: 30,  warn: 10  },

  // --- code_quality ---
  { metric_key: 'prs_per_dev',        section: 'code_quality',  label: 'Pull Requests Merged / Developer',   sublabel: 'Bitbucket \u00b7 authored and merged \u00b7 per developer · period total', unit: '',  drill_id: 'team-prs',       higher_is_better: true,  good: 6,   warn: 3   },
  { metric_key: 'build_success',      section: 'code_quality',  label: 'Build Success Rate',                 sublabel: 'CI \u00b7 passed \u00f7 total runs \u00b7 target \u226590%',            unit: '%',      drill_id: 'team-build',     higher_is_better: true,  good: 90,  warn: 80  },
  { metric_key: 'pr_cycle_time',      section: 'code_quality',  label: 'Pull Request Cycle Time',            sublabel: 'Bitbucket \u00b7 PR opened \u2192 merged \u00b7 lower = better',       unit: 'h',      drill_id: 'team-pr-cycle',  higher_is_better: false, good: 22,  warn: 28  },
  { metric_key: 'bugs_fixed',         section: 'code_quality',  label: 'Bugs Fixed',                         sublabel: 'Jira \u00b7 bug-type issues closed \u00b7 period total',                unit: 'count',  drill_id: 'team-bugs',      higher_is_better: true,  good: 3,   warn: 1   },

  // --- estimation ---
  // Placeholders — backend does not yet emit an estimation bullet view. Until it does, these
  // metrics will always render as `unavailable` (no range from backend).
  { metric_key: 'overrun_ratio',       section: 'estimation',   label: 'Median overrun ratio',               sublabel: 'Jira \u00b7 actual \u00f7 estimated \u00b7 lower = better',            unit: '\u00d7', drill_id: '',               higher_is_better: false, good: 1.5, warn: 2   },
  { metric_key: 'scope_completion',    section: 'estimation',   label: 'Scope Completion Rate',              sublabel: 'Jira \u00b7 tasks done \u00f7 committed at sprint start',              unit: '%',      drill_id: '',               higher_is_better: true,  good: 75,  warn: 60  },
  { metric_key: 'scope_creep',         section: 'estimation',   label: 'Scope Creep Rate',                   sublabel: 'Jira \u00b7 added mid-sprint \u00f7 original count \u00b7 lower = better', unit: '%',  drill_id: '',               higher_is_better: false, good: 19,  warn: 30  },
  { metric_key: 'on_time_delivery',    section: 'estimation',   label: 'On-time Delivery Rate',              sublabel: 'Jira \u00b7 closed by due date',                                       unit: '%',      drill_id: '',               higher_is_better: true,  good: 70,  warn: 55  },
  { metric_key: 'avg_slip',            section: 'estimation',   label: 'Avg Slip When Late',                 sublabel: 'Jira \u00b7 days past due date \u00b7 lower = better',                 unit: 'd',      drill_id: '',               higher_is_better: false, good: 3.1, warn: 4.5 },

  // --- ai_adoption ---
  // *_active: inner max over the period — 1 if any day was active, else 0. Denominator is
  // team headcount, injected by the transform layer from the loaded members list.
  { metric_key: 'active_ai_members',  section: 'ai_adoption',   label: 'Active members',                     sublabel: 'Cursor \u00b7 Claude Code \u00b7 Codex \u00b7 any activity this period', unit: '',     drill_id: 'team-ai-active', higher_is_better: true,  isMemberScale: true, good: 6,   warn: 3   },
  { metric_key: 'cursor_active',      section: 'ai_adoption',   label: 'Cursor \u2014 active members',       sublabel: 'Cursor \u00b7 any activity this period',                               unit: '',     drill_id: '',               higher_is_better: true,  isMemberScale: true, good: 5,   warn: 3   },
  { metric_key: 'cc_active',          section: 'ai_adoption',   label: 'Claude Code \u2014 active members',  sublabel: 'Anthropic Enterprise API \u00b7 any activity this period',             unit: '',     drill_id: '',               higher_is_better: true,  isMemberScale: true, good: 3,   warn: 1   },
  { metric_key: 'codex_active',       section: 'ai_adoption',   label: 'Codex \u2014 active members',        sublabel: 'OpenAI API \u00b7 any activity this period',                           unit: '',     drill_id: '',               higher_is_better: true,  isMemberScale: true, good: 2,   warn: 1   },
  { metric_key: 'team_ai_loc',        section: 'ai_adoption',   label: 'Team AI Accepted Lines',             sublabel: 'Cursor + Claude Code \u00b7 accepted lines \u00b7 period total',       unit: 'lines', drill_id: 'team-ai-loc',    higher_is_better: true,  good: 1000, warn: 300 },
  { metric_key: 'cursor_acceptance',  section: 'ai_adoption',   label: 'Cursor Acceptance Rate',             sublabel: 'Cursor \u00b7 accepted \u00f7 shown completions \u00b7 daily avg',     unit: '%',      drill_id: '',               higher_is_better: true,  good: 55,  warn: 35  },
  { metric_key: 'cc_tool_acceptance', section: 'ai_adoption',   label: 'Claude Code Tool Acceptance',        sublabel: 'Anthropic Enterprise API \u00b7 accepted \u00f7 offered \u00b7 daily avg', unit: '%', drill_id: '',               higher_is_better: true,  good: 60,  warn: 40  },
  // Backend sometimes ships cc_tool_accept without the "ance" suffix — alias to the same pretty name.
  { metric_key: 'cc_tool_accept',     section: 'ai_adoption',   label: 'Claude Code Tool Acceptance',        sublabel: 'Anthropic Enterprise API \u00b7 accepted \u00f7 offered \u00b7 daily avg', unit: '%', drill_id: '',               higher_is_better: true,  good: 60,  warn: 40  },
  { metric_key: 'cursor_lines',       section: 'ai_adoption',   label: 'Cursor Accepted Lines',              sublabel: 'Cursor \u00b7 lines accepted from AI suggestions \u00b7 period total', unit: 'lines', drill_id: '',             higher_is_better: true,  good: 100, warn: 30  },
  { metric_key: 'cursor_agents',      section: 'ai_adoption',   label: 'Cursor Agent Interactions',          sublabel: 'Cursor \u00b7 agent-mode actions \u00b7 period total',                 unit: '',      drill_id: '',             higher_is_better: true,  good: 10,  warn: 3   },
  { metric_key: 'cursor_completions', section: 'ai_adoption',   label: 'Cursor Completions',                 sublabel: 'Cursor \u00b7 inline completions offered \u00b7 period total',         unit: '',      drill_id: '',             higher_is_better: true,  good: 30,  warn: 10  },
  { metric_key: 'cc_lines',           section: 'ai_adoption',   label: 'Claude Code Accepted Lines',         sublabel: 'Anthropic Enterprise API \u00b7 accepted lines \u00b7 period total',    unit: 'lines', drill_id: '',             higher_is_better: true,  good: 50,  warn: 10  },
  { metric_key: 'cc_sessions',        section: 'ai_adoption',   label: 'Claude Code Sessions',               sublabel: 'Anthropic Enterprise API \u00b7 sessions \u00b7 period total',         unit: '',      drill_id: '',             higher_is_better: true,  good: 4,   warn: 1   },
  { metric_key: 'chatgpt',            section: 'ai_adoption',   label: 'ChatGPT Activity',                   sublabel: 'ChatGPT Team \u00b7 interactions \u00b7 period total',                  unit: '',      drill_id: '',             higher_is_better: true,  good: 10,  warn: 0   },
  { metric_key: 'claude_web',         section: 'ai_adoption',   label: 'Claude.ai Activity',                 sublabel: 'Claude.ai web \u00b7 interactions \u00b7 period total',                 unit: '',      drill_id: '',             higher_is_better: true,  good: 10,  warn: 0   },
  { metric_key: 'ai_loc_share2',      section: 'ai_adoption',   label: 'AI Code Acceptance',                       sublabel: 'Cursor + Claude Code \u00b7 accepted \u00f7 clean LOC \u00b7 daily avg', unit: '%',    drill_id: '',             higher_is_better: true,  good: 14,  warn: 8   },

  // --- collaboration ---
  { metric_key: 'slack_messages_sent',        section: 'collaboration', label: 'Messages Sent',          sublabel: 'Slack \u00b7 messages sent \u00b7 period total',                               teamSublabel: 'Slack \u00b7 messages sent \u00b7 avg per person \u00b7 period total',              unit: 'messages',     drill_id: '',   higher_is_better: true,  good: 100, warn: 40  },
  { metric_key: 'slack_channel_posts',        section: 'collaboration', label: 'Channel Posts',          sublabel: 'Slack \u00b7 posts in public channels \u00b7 period total',                    teamSublabel: 'Slack \u00b7 posts in public channels \u00b7 avg per person \u00b7 period total',   unit: 'messages',     drill_id: '',   higher_is_better: true,  good: 25,  warn: 10  },
  { metric_key: 'slack_active_days',          section: 'collaboration', label: 'Active Days',            sublabel: 'Slack \u00b7 days with any messages \u00b7 period total',                      teamSublabel: 'Slack \u00b7 days with any messages \u00b7 avg per person \u00b7 period total',     unit: 'days',         drill_id: '',   higher_is_better: true,  good: 15,  warn: 8   },
  { metric_key: 'slack_msgs_per_active_day',  section: 'collaboration', label: 'Messages per Active Day', sublabel: 'Slack \u00b7 messages \u00f7 active days \u00b7 daily avg',                        teamSublabel: 'Slack \u00b7 messages \u00f7 active days \u00b7 avg per person \u00b7 daily avg',        unit: 'messages/day', drill_id: '',   higher_is_better: true,  good: 10,  warn: 4   },
  { metric_key: 'slack_dm_ratio',             section: 'collaboration', label: 'DM Ratio',               sublabel: 'Slack \u00b7 DMs \u00f7 all messages \u00b7 daily avg \u00b7 lower = more open',         teamSublabel: 'Slack \u00b7 DMs \u00f7 all messages \u00b7 avg per person \u00b7 daily avg \u00b7 lower = more open', unit: '%', drill_id: '', higher_is_better: false, good: 30,  warn: 50  },
  { metric_key: 'm365_active_days',           section: 'collaboration', label: 'Active Days',            sublabel: 'M365 \u00b7 days with any sent / chat / file activity \u00b7 period total',     teamSublabel: 'M365 \u00b7 days with any sent / chat / file activity \u00b7 avg per person \u00b7 period total', unit: 'days',     drill_id: '',   higher_is_better: true,  good: 18,  warn: 10  },
  { metric_key: 'm365_emails_sent',           section: 'collaboration', label: 'Emails Sent',            sublabel: 'M365 \u00b7 emails sent \u00b7 period total',                                   teamSublabel: 'M365 \u00b7 emails sent \u00b7 avg per person \u00b7 period total',                  unit: 'emails',   drill_id: '',   higher_is_better: true,  good: 30,  warn: 5   },
  { metric_key: 'm365_emails_received',       section: 'collaboration', label: 'Emails Received',        sublabel: 'M365 \u00b7 inbox volume \u00b7 period total',                                  teamSublabel: 'M365 \u00b7 inbox volume \u00b7 avg per person \u00b7 period total',                 unit: 'emails',   drill_id: '',   higher_is_better: true,  good: 100, warn: 30  },
  { metric_key: 'm365_emails_read',           section: 'collaboration', label: 'Emails Read',            sublabel: 'M365 \u00b7 emails read \u00b7 period total',                                   teamSublabel: 'M365 \u00b7 emails read \u00b7 avg per person \u00b7 period total',                  unit: 'emails',   drill_id: '',   higher_is_better: true,  good: 200, warn: 50  },
  { metric_key: 'm365_teams_chats',           section: 'collaboration', label: 'Teams Chats',            sublabel: 'Microsoft Teams \u00b7 DMs and group chats \u00b7 period total',                teamSublabel: 'Microsoft Teams \u00b7 DMs and group chats \u00b7 avg per person \u00b7 period total', unit: 'messages', drill_id: '',   higher_is_better: true,  good: 50,  warn: 20  },
  { metric_key: 'm365_files_engaged',         section: 'collaboration', label: 'Files Engaged',          sublabel: 'M365 \u00b7 files viewed or edited \u00b7 period total',                        teamSublabel: 'M365 \u00b7 files viewed or edited \u00b7 avg per person \u00b7 period total',       unit: 'files',    drill_id: '',   higher_is_better: true,  good: 40,  warn: 15  },
  { metric_key: 'm365_files_shared_internal', section: 'collaboration', label: 'Files Shared (Internal)', sublabel: 'M365 \u00b7 files shared inside org \u00b7 period total',                       teamSublabel: 'M365 \u00b7 files shared inside org \u00b7 avg per person \u00b7 period total',     unit: 'files',    drill_id: '',   higher_is_better: true,  good: 6,   warn: 2   },
  { metric_key: 'm365_files_shared_external', section: 'collaboration', label: 'Files Shared (External)', sublabel: 'M365 \u00b7 files shared outside org \u00b7 period total \u00b7 governance signal', teamSublabel: 'M365 \u00b7 files shared outside org \u00b7 avg per person \u00b7 period total \u00b7 governance signal', unit: 'files', drill_id: '', higher_is_better: true, good: 1, warn: 0 },
  { metric_key: 'meeting_hours',              section: 'collaboration', label: 'Meeting Hours',          sublabel: 'Teams + Zoom \u00b7 longest modality per meeting \u00b7 period total',          teamSublabel: 'Teams + Zoom \u00b7 longest modality per meeting \u00b7 avg per person \u00b7 period total', unit: 'h', drill_id: '', higher_is_better: false, good: 40,  warn: 80  },
  { metric_key: 'meetings_count',             section: 'collaboration', label: 'Meetings Attended',      sublabel: 'Teams + Zoom \u00b7 distinct meetings joined \u00b7 period total',              teamSublabel: 'Teams + Zoom \u00b7 distinct meetings joined \u00b7 avg per person \u00b7 period total',     unit: 'meetings', drill_id: '', higher_is_better: true, good: 40, warn: 10 },
  { metric_key: 'teams_meeting_hours',        section: 'collaboration', label: 'Teams Meeting Hours',    sublabel: 'Microsoft Teams \u00b7 longest modality per meeting \u00b7 period total',       teamSublabel: 'Microsoft Teams \u00b7 longest modality per meeting \u00b7 avg per person \u00b7 period total', unit: 'h', drill_id: '', higher_is_better: false, good: 30, warn: 60 },
  { metric_key: 'zoom_meeting_hours',         section: 'collaboration', label: 'Zoom Meeting Hours',     sublabel: 'Zoom \u00b7 longest modality per meeting \u00b7 period total',                  teamSublabel: 'Zoom \u00b7 longest modality per meeting \u00b7 avg per person \u00b7 period total', unit: 'h', drill_id: '', higher_is_better: false, good: 20, warn: 40 },
  { metric_key: 'teams_meetings',             section: 'collaboration', label: 'Teams Meetings Attended', sublabel: 'Microsoft Teams \u00b7 distinct meetings joined \u00b7 period total',          teamSublabel: 'Microsoft Teams \u00b7 distinct meetings joined \u00b7 avg per person \u00b7 period total', unit: 'meetings', drill_id: '', higher_is_better: true, good: 25, warn: 5 },
  { metric_key: 'zoom_meetings',              section: 'collaboration', label: 'Zoom Meetings Attended',  sublabel: 'Zoom \u00b7 distinct meetings joined \u00b7 period total',                     teamSublabel: 'Zoom \u00b7 distinct meetings joined \u00b7 avg per person \u00b7 period total', unit: 'meetings', drill_id: '', higher_is_better: true, good: 15, warn: 3 },
  { metric_key: 'meeting_free',               section: 'collaboration', label: 'Meeting-Free Days',      sublabel: 'Teams + Zoom \u00b7 days with any record but no meeting time \u00b7 period total', teamSublabel: 'Teams + Zoom \u00b7 days with any record but no meeting time \u00b7 avg per person \u00b7 period total', unit: 'days', drill_id: '', higher_is_better: true, good: 4, warn: 2 },
];

// ---------------------------------------------------------------------------
// IC KPI definitions
// ---------------------------------------------------------------------------

/**
 * `raw_field` is constrained to numeric keys of `RawIcAggregateRow` so
 * typos (e.g. mis-spelled column names) fail at compile time instead of
 * producing NaN at runtime.
 */
type NumericIcAggregateField = {
  [K in keyof RawIcAggregateRow]: RawIcAggregateRow[K] extends number | null ? K : never;
}[keyof RawIcAggregateRow];

export type IcKpiDef = {
  metric_key: string;
  raw_field: NumericIcAggregateField;
  label: string;
  unit: string;
  sublabel: string;
  description: string;
  higher_is_better: boolean;
  format: 'integer' | 'decimal1' | 'percent' | 'hours';
};

/**
 * IC-level KPI definitions. The transform layer reads `raw_field` from the
 * backend row and formats per `format`. When the backend returns null for a
 * source that isn't ingested yet (e.g. Bitbucket diffstat for Clean LOC), the
 * transform emits `value:null` and KpiStrip renders ComingSoon in the cell.
 */
export const IC_KPI_DEFS: IcKpiDef[] = [
  { metric_key: 'bugs_fixed',      raw_field: 'bugs_fixed',       label: 'Bugs Fixed',     unit: '',  sublabel: 'Jira',                 description: 'Bug-type Jira issues closed in the selected period. Reflects quality contribution and team reliability.',                                          higher_is_better: true,  format: 'integer'  },
  { metric_key: 'ai_loc_share',    raw_field: 'ai_loc_share_pct', label: 'AI Code Acceptance',   unit: '%', sublabel: 'Cursor + Claude Code', description: 'Share of authored lines accepted from AI suggestions (Cursor + Claude Code). Reflects how much AI tooling contributes to actual output.',       higher_is_better: true,  format: 'percent'  },
  { metric_key: 'focus_time_pct',  raw_field: 'focus_time_pct',   label: 'Focus Time',     unit: '%', sublabel: 'Calendar / M365',      description: 'Share of work time spent in uninterrupted 60-minute+ blocks. Higher means fewer context switches and more deep work.',                          higher_is_better: true,  format: 'percent'  },
  { metric_key: 'tasks_closed',    raw_field: 'tasks_closed',     label: 'Tasks Closed',   unit: '',  sublabel: 'Jira',                 description: 'Jira tasks moved to Done in the selected period. Direct measure of delivery throughput.',                                                      higher_is_better: true,  format: 'integer'  },
  { metric_key: 'prs_merged',      raw_field: 'prs_merged',       label: 'PRs Merged',     unit: '',  sublabel: 'Bitbucket',            description: 'Pull requests authored and merged. Source not ingested yet — cell shows ComingSoon until Bitbucket PR ingestion lands.',                        higher_is_better: true,  format: 'integer'  },
  { metric_key: 'pr_cycle_time_h', raw_field: 'pr_cycle_time_h',  label: 'PR Cycle Time',  unit: 'h', sublabel: 'Bitbucket',            description: 'Average hours from PR opened to merged. Source not ingested yet — cell shows ComingSoon until Bitbucket PR ingestion lands.',                   higher_is_better: false, format: 'hours'    },
  { metric_key: 'ai_sessions',     raw_field: 'ai_sessions',      label: 'AI Sessions',    unit: '',  sublabel: 'Cursor',               description: 'Distinct Cursor sessions in the selected period. Proxy for how often AI tooling is engaged.',                                                 higher_is_better: true,  format: 'integer'  },
];

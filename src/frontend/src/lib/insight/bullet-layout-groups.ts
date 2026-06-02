/**
 * Sub-grouping for legacy v1 bullet layouts (Refs #82 — extracted from
 * the deleted `threshold-config.ts`).
 *
 * UI composition picks metrics by group name instead of hand-maintained
 * `metric_key` arrays sprinkled through screen components. Group names
 * are scoped by section — they only need to be unique within one
 * section. This is FE-only layout metadata (not metric metadata): the
 * wire response carries metric labels / sublabels / thresholds; how a
 * v1 screen arranges those bullets into columns lives here.
 *
 * Only sections that use sub-grouping have entries: `estimation`
 * (3 groups), `ai_adoption` (7 groups), `collaboration` (4 groups by
 * activity type: chat / email / meetings / files). `task_delivery`,
 * `git_output`, `code_quality` don't sub-group.
 *
 * Used by `engineering-dashboard.tsx` (v1 IC) and
 * `team-bullet-sections.tsx` (v1 team). The v2 surfaces in
 * `src/lib/insight/v2/` use a different composition model and don't
 * read this map.
 */

import type { BulletMetric } from '@/types/insight';

export const BULLET_LAYOUT_GROUPS: Record<string, string> = {
  estimation_accuracy: 'estimate_accuracy',
  overrun_ratio:       'estimate_accuracy',
  scope_completion:    'sprint_scope',
  scope_creep:         'sprint_scope',
  on_time_delivery:    'deadline',
  avg_slip:            'deadline',

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

  // Grouped by activity type (chat / email / meetings / files), not by
  // vendor — Slack-chat and Teams-chat answer the same question so they
  // belong in one column. Source (Slack / Microsoft 365 / Zoom) stays
  // visible per-row via each metric's own sublabel.
  slack_messages_sent:        'chat',
  slack_channel_posts:        'chat',
  slack_active_days:          'chat',
  slack_msgs_per_active_day:  'chat',
  slack_dm_ratio:             'chat',
  m365_teams_chats:           'chat',
  m365_active_days:           'chat',
  m365_emails_sent:           'email',
  m365_emails_received:       'email',
  m365_emails_read:           'email',
  m365_files_engaged:         'files',
  m365_files_shared_internal: 'files',
  m365_files_shared_external: 'files',
  meeting_hours:              'meetings',
  meetings_count:             'meetings',
  teams_meeting_hours:        'meetings',
  zoom_meeting_hours:         'meetings',
  teams_meetings:             'meetings',
  zoom_meetings:              'meetings',
  meeting_free:               'meetings',
};

/**
 * Canonical ordering for bullet metrics across v1 layouts. Keys earlier
 * in this array render first when a section is filtered down to a
 * sub-group. Backend response order is unconstrained (CH `GROUP BY` is
 * unordered), so the FE re-sorts using this list before paint.
 *
 * Keys absent from the array sort to the end in stable order.
 */
const BULLET_DISPLAY_ORDER: readonly string[] = [
  // task_delivery
  'tasks_completed',
  'task_dev_time',
  'task_reopen_rate',
  'due_date_compliance',
  'estimation_accuracy',
  'worklog_logging_accuracy',
  'bugs_to_task_ratio',
  'mean_time_to_resolution',
  'stale_in_progress',
  'flow_efficiency',
  'pickup_time',

  // git_output
  'commits',
  'prs_created',
  'prs_merged',
  'clean_loc',
  'pr_cycle_time_h',
  'pr_size',
  'merge_rate',
  'lines_per_commit',
  'commits_per_active_day',

  // code_quality
  'prs_per_dev',
  'build_success',
  'pr_cycle_time',
  'bugs_fixed',

  // estimation
  'overrun_ratio',
  'scope_completion',
  'scope_creep',
  'on_time_delivery',
  'avg_slip',

  // ai_adoption
  'active_ai_members',
  'cursor_active',
  'cc_active',
  'codex_active',
  'team_ai_loc',
  'cursor_acceptance',
  'cc_tool_acceptance',
  'cc_tool_accept',
  'cursor_lines',
  'cursor_agents',
  'cursor_completions',
  'cc_lines',
  'cc_sessions',
  'chatgpt',
  'claude_web',
  'ai_loc_share2',
  'cc_cost',
  'prs_with_cc',
  'prs_total',

  // collaboration
  'slack_messages_sent',
  'slack_channel_posts',
  'slack_active_days',
  'slack_msgs_per_active_day',
  'slack_dm_ratio',
  'm365_active_days',
  'm365_emails_sent',
  'm365_emails_received',
  'm365_emails_read',
  'm365_teams_chats',
  'm365_files_engaged',
  'm365_files_shared_internal',
  'm365_files_shared_external',
  'meeting_hours',
  'meetings_count',
  'teams_meeting_hours',
  'zoom_meeting_hours',
  'teams_meetings',
  'zoom_meetings',
  'meeting_free',
];

const ORDER_INDEX = new Map(BULLET_DISPLAY_ORDER.map((k, i) => [k, i]));

export function filterBulletsByLayoutGroup(
  metrics: BulletMetric[],
  group: string,
): BulletMetric[] {
  const filtered = metrics.filter((m) => BULLET_LAYOUT_GROUPS[m.metric_key] === group);
  return filtered.sort(
    (a, b) =>
      (ORDER_INDEX.get(a.metric_key) ?? Number.MAX_SAFE_INTEGER) -
      (ORDER_INDEX.get(b.metric_key) ?? Number.MAX_SAFE_INTEGER),
  );
}

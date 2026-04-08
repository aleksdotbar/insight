/**
 * Team View — base fixture data
 * Reference data for GET /views/team?period={p}
 *
 * This file is the contract reference for backend developers.
 * Do not abbreviate or summarise — every row matters.
 */

import type {
  TeamMember,
  TeamKpi,
  BulletMetric,
  BulletSection,
  PeriodValue,
  TeamViewConfig,
  DataAvailability,
} from '../../../types';

// ---------------------------------------------------------------------------
// Team Members (month baseline, 12 members)
// ---------------------------------------------------------------------------

export const TEAM_MEMBERS_MONTH: TeamMember[] = [
  { person_id: 'p1',  period: 'month', name: 'Alice Kim',    seniority: 'Senior', tasks_closed: 12, bugs_fixed: 5, dev_time_h: 14, prs_merged: 11, build_success_pct: 96, focus_time_pct: 72, ai_tools: ['Cursor', 'Claude Code'], ai_loc_share_pct: 27 },
  { person_id: 'p2',  period: 'month', name: 'Ben Clarke',   seniority: 'Middle', tasks_closed: 2,  bugs_fixed: 1, dev_time_h: 31, prs_merged: 2,  build_success_pct: 78, focus_time_pct: 48, ai_tools: [],                        ai_loc_share_pct: 0,  trend_label: '3 months declining' },
  { person_id: 'p3',  period: 'month', name: 'Tom Sullivan', seniority: 'Middle', tasks_closed: 3,  bugs_fixed: 2, dev_time_h: 26, prs_merged: 3,  build_success_pct: 83, focus_time_pct: 55, ai_tools: ['Codex'],                  ai_loc_share_pct: 8,  trend_label: '2 months declining' },
  { person_id: 'p4',  period: 'month', name: 'Sara Jansen',  seniority: 'Junior', tasks_closed: 5,  bugs_fixed: 3, dev_time_h: 18, prs_merged: 4,  build_success_pct: 91, focus_time_pct: 62, ai_tools: ['Cursor'],                 ai_loc_share_pct: 12 },
  { person_id: 'p5',  period: 'month', name: 'Mike Chen',    seniority: 'Senior', tasks_closed: 10, bugs_fixed: 4, dev_time_h: 12, prs_merged: 9,  build_success_pct: 97, focus_time_pct: 68, ai_tools: ['Cursor', 'Claude Code'], ai_loc_share_pct: 31 },
  { person_id: 'p6',  period: 'month', name: 'Emma Davis',   seniority: 'Lead',   tasks_closed: 8,  bugs_fixed: 2, dev_time_h: 10, prs_merged: 7,  build_success_pct: 99, focus_time_pct: 81, ai_tools: ['Claude Code'],           ai_loc_share_pct: 22 },
  { person_id: 'p7',  period: 'month', name: 'Carlos Ruiz',  seniority: 'Middle', tasks_closed: 6,  bugs_fixed: 3, dev_time_h: 16, prs_merged: 5,  build_success_pct: 88, focus_time_pct: 59, ai_tools: ['Cursor'],                 ai_loc_share_pct: 14, trend_label: '2 months declining' },
  { person_id: 'p8',  period: 'month', name: 'Priya Patel',  seniority: 'Senior', tasks_closed: 9,  bugs_fixed: 4, dev_time_h: 13, prs_merged: 8,  build_success_pct: 94, focus_time_pct: 74, ai_tools: ['Cursor', 'Codex'],        ai_loc_share_pct: 19 },
  { person_id: 'p9',  period: 'month', name: 'James Wilson', seniority: 'Junior', tasks_closed: 4,  bugs_fixed: 2, dev_time_h: 20, prs_merged: 3,  build_success_pct: 82, focus_time_pct: 42, ai_tools: [],                        ai_loc_share_pct: 0,  trend_label: '3 months declining' },
  { person_id: 'p10', period: 'month', name: 'Ana Kovac',    seniority: 'Middle', tasks_closed: 7,  bugs_fixed: 3, dev_time_h: 15, prs_merged: 6,  build_success_pct: 92, focus_time_pct: 65, ai_tools: ['Cursor'],                 ai_loc_share_pct: 16 },
  { person_id: 'p11', period: 'month', name: 'Leo Zhang',    seniority: 'Senior', tasks_closed: 11, bugs_fixed: 6, dev_time_h: 11, prs_merged: 10, build_success_pct: 98, focus_time_pct: 76, ai_tools: ['Claude Code', 'Codex'],  ai_loc_share_pct: 34 },
  { person_id: 'p12', period: 'month', name: 'Nina Ross',    seniority: 'Junior', tasks_closed: 3,  bugs_fixed: 1, dev_time_h: 22, prs_merged: 2,  build_success_pct: 79, focus_time_pct: 53, ai_tools: ['Cursor'],                 ai_loc_share_pct: 10, trend_label: '2 months declining' },
];

// ---------------------------------------------------------------------------
// Bullet Sections (month baseline, 5 sections)
// ---------------------------------------------------------------------------

export const BULLET_SECTIONS_MONTH: BulletSection[] = [
  {
    id: 'task_delivery',
    title: 'Task Delivery',
    metrics: [
      {
        period: 'month', section: 'task_delivery', metric_key: 'tasks_completed', label: 'Tasks Closed / Developer',
        sublabel: 'Jira · closed issues in sprint · team median per developer', value: '5.3', unit: 'tasks',
        range_min: '0', range_max: '15', median: '5.8', median_label: 'Company median: 5.8',
        bar_left_pct: 0, bar_width_pct: 53, median_left_pct: 48,
        status: 'good', drill_id: 'team-tasks',
      },
      {
        period: 'month', section: 'task_delivery', metric_key: 'task_dev_time', label: 'Task Development Time',
        sublabel: 'Jira · time in In Progress state · lower = better', value: '14', unit: 'h',
        range_min: '8h', range_max: '31h', median: '15', median_label: 'Median: 15h',
        bar_left_pct: 0, bar_width_pct: 43, median_left_pct: 48,
        status: 'good', drill_id: 'team-dev-time',
      },
      {
        period: 'month', section: 'task_delivery', metric_key: 'task_reopen_rate', label: 'Task Reopen Rate',
        sublabel: 'Jira · closed then reopened within 14 days · lower = better', value: '4', unit: '%',
        range_min: '0%', range_max: '15%', median: '5', median_label: 'Median: 5%',
        bar_left_pct: 0, bar_width_pct: 27, median_left_pct: 33,
        status: 'good', drill_id: 'team-reopen',
      },
      {
        period: 'month', section: 'task_delivery', metric_key: 'due_date_compliance', label: 'Due Date Compliance',
        sublabel: 'Jira · tasks closed by due date', value: '78', unit: '%',
        range_min: '40%', range_max: '100%', median: '72', median_label: 'Company median: 72%',
        bar_left_pct: 0, bar_width_pct: 63, median_left_pct: 53,
        status: 'good', drill_id: '',
      },
    ] as BulletMetric[],
  },
  {
    id: 'code_quality',
    title: 'Code & Quality',
    metrics: [
      {
        period: 'month', section: 'code_quality', metric_key: 'prs_per_dev', label: 'Pull Requests Merged / Developer',
        sublabel: 'Bitbucket · authored and merged · team median', value: '7.2', unit: '',
        range_min: '0', range_max: '20', median: '6', median_label: 'Company median: 6',
        bar_left_pct: 0, bar_width_pct: 60, median_left_pct: 42,
        status: 'good', drill_id: 'team-prs',
      },
      {
        period: 'month', section: 'code_quality', metric_key: 'build_success', label: 'Build Success Rate',
        sublabel: 'CI · passed ÷ total runs · target ≥90%', value: '94', unit: '%',
        range_min: '78%', range_max: '97%', median: '89', median_label: 'Median: 89%',
        bar_left_pct: 0, bar_width_pct: 84, median_left_pct: 58,
        status: 'good', drill_id: 'team-build',
      },
      {
        period: 'month', section: 'code_quality', metric_key: 'pr_cycle_time', label: 'Pull Request Cycle Time',
        sublabel: 'Bitbucket · PR opened → merged · lower = better', value: '18', unit: 'h',
        range_min: '10h', range_max: '35h', median: '22', median_label: 'Median: 22h',
        bar_left_pct: 0, bar_width_pct: 32, median_left_pct: 48,
        status: 'good', drill_id: 'team-pr-cycle',
      },
      {
        period: 'month', section: 'code_quality', metric_key: 'bugs_fixed', label: 'Bugs Fixed',
        sublabel: 'Jira · bug-type issues closed', value: '4', unit: 'count',
        range_min: '1', range_max: '8', median: '3', median_label: 'Median: 3',
        bar_left_pct: 0, bar_width_pct: 43, median_left_pct: 29,
        status: 'good', drill_id: 'team-bugs',
      },
    ] as BulletMetric[],
  },
  {
    id: 'estimation',
    title: 'Estimation',
    metrics: [
      {
        period: 'month', section: 'estimation', metric_key: 'estimation_accuracy', label: 'Within ±20% of estimate',
        sublabel: 'Jira · original estimate vs time spent', value: '61', unit: '%',
        range_min: '0%', range_max: '100%', median: '58', median_label: 'Company median: 58%',
        bar_left_pct: 0, bar_width_pct: 61, median_left_pct: 58,
        status: 'good', drill_id: '',
      },
      {
        period: 'month', section: 'estimation', metric_key: 'overrun_ratio', label: 'Median overrun ratio',
        sublabel: 'Jira · actual ÷ estimated · lower = better', value: '1.3', unit: '\u00d7',
        range_min: '1\u00d7', range_max: '3\u00d7', median: '1.5', median_label: 'Company median: 1.5\u00d7',
        bar_left_pct: 0, bar_width_pct: 43, median_left_pct: 50,
        status: 'good', drill_id: '',
      },
      {
        period: 'month', section: 'estimation', metric_key: 'scope_completion', label: 'Scope Completion Rate',
        sublabel: 'Jira · tasks done ÷ committed at sprint start', value: '82', unit: '%',
        range_min: '0%', range_max: '100%', median: '79', median_label: 'Company median: 79%',
        bar_left_pct: 0, bar_width_pct: 82, median_left_pct: 79,
        status: 'good', drill_id: '',
      },
      {
        period: 'month', section: 'estimation', metric_key: 'scope_creep', label: 'Scope Creep Rate',
        sublabel: 'Jira · added mid-sprint ÷ original count · lower = better', value: '14', unit: '%',
        range_min: '0%', range_max: '50%', median: '19', median_label: 'Company median: 19% · lower = better',
        bar_left_pct: 0, bar_width_pct: 28, median_left_pct: 38,
        status: 'good', drill_id: '',
      },
      {
        period: 'month', section: 'estimation', metric_key: 'on_time_delivery', label: 'On-time Delivery Rate',
        sublabel: 'Jira · closed by due date', value: '74', unit: '%',
        range_min: '0%', range_max: '100%', median: '71', median_label: 'Company median: 71%',
        bar_left_pct: 0, bar_width_pct: 74, median_left_pct: 71,
        status: 'good', drill_id: '',
      },
      {
        period: 'month', section: 'estimation', metric_key: 'avg_slip', label: 'Avg Slip When Late',
        sublabel: 'Jira · days past due date · lower = better', value: '2.1', unit: 'd',
        range_min: '0d', range_max: '6d', median: '3.1', median_label: 'Company median: 3.1d',
        bar_left_pct: 0, bar_width_pct: 35, median_left_pct: 52,
        status: 'good', drill_id: '',
      },
    ] as BulletMetric[],
  },
  {
    id: 'ai_adoption',
    title: 'AI Adoption',
    metrics: [
      {
        period: 'month', section: 'ai_adoption', metric_key: 'active_ai_members', label: 'Active members',
        sublabel: 'Cursor · Claude Code · Codex · any activity this month', value: '7', unit: '/ 12',
        range_min: '0', range_max: '12', median: '7', median_label: 'Company median: 7 of 12 (58%)',
        bar_left_pct: 0, bar_width_pct: 58, median_left_pct: 58,
        status: 'good', drill_id: 'team-ai-active',
      },
      {
        period: 'month', section: 'ai_adoption', metric_key: 'cursor_active', label: 'Cursor — active members',
        sublabel: 'Cursor · any activity this month', value: '7', unit: '/ 12',
        range_min: '0', range_max: '12', median: '6', median_label: 'Company median: 6 of 12',
        bar_left_pct: 0, bar_width_pct: 58, median_left_pct: 50,
        status: 'good', drill_id: '',
      },
      {
        period: 'month', section: 'ai_adoption', metric_key: 'cc_active', label: 'Claude Code — active members',
        sublabel: 'Anthropic Enterprise API · sessions this month', value: '4', unit: '/ 12',
        range_min: '0', range_max: '12', median: '3', median_label: 'Company median: 3 of 12',
        bar_left_pct: 0, bar_width_pct: 33, median_left_pct: 25,
        status: 'good', drill_id: '',
      },
      {
        period: 'month', section: 'ai_adoption', metric_key: 'codex_active', label: 'Codex — active members',
        sublabel: 'OpenAI API · completions this month', value: '1', unit: '/ 12',
        range_min: '0', range_max: '12', median: '2', median_label: 'Company median: 2 of 12',
        bar_left_pct: 0, bar_width_pct: 8, median_left_pct: 17,
        status: 'good', drill_id: '',
      },
      {
        period: 'month', section: 'ai_adoption', metric_key: 'team_ai_loc', label: 'Team AI LOC Share',
        sublabel: 'Cursor + Claude Code · accepted lines ÷ clean LOC', value: '16', unit: '%',
        range_min: '0%', range_max: '50%', median: '14', median_label: 'Company median: 14%',
        bar_left_pct: 0, bar_width_pct: 32, median_left_pct: 28,
        status: 'good', drill_id: 'team-ai-loc',
      },
      {
        period: 'month', section: 'ai_adoption', metric_key: 'cursor_acceptance', label: 'Cursor Acceptance Rate',
        sublabel: 'Cursor · accepted ÷ shown completions · team median', value: '71', unit: '%',
        range_min: '0%', range_max: '100%', median: '58', median_label: 'Company median: 58%',
        bar_left_pct: 0, bar_width_pct: 71, median_left_pct: 58,
        status: 'good', drill_id: '',
      },
      {
        period: 'month', section: 'ai_adoption', metric_key: 'cc_tool_acceptance', label: 'Claude Code Tool Acceptance',
        sublabel: 'Anthropic Enterprise API · accepted ÷ offered · team median', value: '76', unit: '%',
        range_min: '0%', range_max: '100%', median: '64', median_label: 'Company median: 64%',
        bar_left_pct: 0, bar_width_pct: 76, median_left_pct: 64,
        status: 'good', drill_id: '',
      },
    ] as BulletMetric[],
  },
  {
    id: 'collaboration',
    title: 'Collaboration',
    metrics: [
      {
        period: 'month', section: 'collaboration', metric_key: 'slack_thread_participation', label: 'Thread Participation',
        sublabel: 'Slack · replies to others\' threads', value: '34', unit: 'replies',
        range_min: '0', range_max: '80', median: '29', median_label: 'Company median: 29',
        bar_left_pct: 0, bar_width_pct: 43, median_left_pct: 36,
        status: 'good', drill_id: '',
      },
      {
        period: 'month', section: 'collaboration', metric_key: 'slack_message_engagement', label: 'Message Engagement',
        sublabel: 'Slack · avg replies per thread', value: '2.1', unit: 'avg',
        range_min: '0', range_max: '5', median: '1.8', median_label: 'Company median: 1.8',
        bar_left_pct: 0, bar_width_pct: 42, median_left_pct: 36,
        status: 'good', drill_id: '',
      },
      {
        period: 'month', section: 'collaboration', metric_key: 'slack_dm_ratio', label: 'DM Ratio',
        sublabel: 'Slack · DMs ÷ all messages · lower = more open', value: '24', unit: '%',
        range_min: '0%', range_max: '100%', median: '28', median_label: 'Company median: 28% · lower = more open',
        bar_left_pct: 0, bar_width_pct: 24, median_left_pct: 28,
        status: 'good', drill_id: '',
      },
      {
        period: 'month', section: 'collaboration', metric_key: 'm365_teams_messages', label: 'Teams Messages',
        sublabel: 'Microsoft Teams · all channels sent', value: '168', unit: '/mo',
        range_min: '0', range_max: '400', median: '148', median_label: 'Company median: 148',
        bar_left_pct: 0, bar_width_pct: 42, median_left_pct: 37,
        status: 'good', drill_id: '',
      },
      {
        period: 'month', section: 'collaboration', metric_key: 'm365_emails_sent', label: 'Emails Sent',
        sublabel: 'M365 · avg per member · lower = better', value: '31', unit: '/mo',
        range_min: '0', range_max: '120', median: '35', median_label: 'Company median: 35 · lower = better',
        bar_left_pct: 0, bar_width_pct: 26, median_left_pct: 29,
        status: 'good', drill_id: '',
      },
      {
        period: 'month', section: 'collaboration', metric_key: 'm365_files_shared', label: 'Files Shared',
        sublabel: 'M365 · avg per member', value: '9', unit: '/mo',
        range_min: '0', range_max: '30', median: '8', median_label: 'Company median: 8',
        bar_left_pct: 0, bar_width_pct: 30, median_left_pct: 27,
        status: 'good', drill_id: '',
      },
      {
        period: 'month', section: 'collaboration', metric_key: 'meeting_hours', label: 'Meeting Hours',
        sublabel: 'Zoom · meeting duration + M365 audioDuration · avg · lower = better', value: '38', unit: 'h/mo',
        range_min: '0h', range_max: '72h', median: '38', median_label: 'Company median: 38h · Target <40h',
        bar_left_pct: 0, bar_width_pct: 53, median_left_pct: 53,
        status: 'good', drill_id: '',
      },
      {
        period: 'month', section: 'collaboration', metric_key: 'zoom_calls', label: 'Zoom Calls',
        sublabel: 'Zoom API · avg calls attended per member', value: '11', unit: '/mo',
        range_min: '0', range_max: '20', median: '9', median_label: 'Company median: 9',
        bar_left_pct: 0, bar_width_pct: 55, median_left_pct: 45,
        status: 'good', drill_id: '',
      },
      {
        period: 'month', section: 'collaboration', metric_key: 'meeting_free', label: 'Meeting-Free Days',
        sublabel: 'Zoom · days with no meetings + M365 · avg · higher = better', value: '5', unit: 'days',
        range_min: '0', range_max: '10', median: '4', median_label: 'Company median: 4',
        bar_left_pct: 0, bar_width_pct: 50, median_left_pct: 40,
        status: 'good', drill_id: '',
      },
    ] as BulletMetric[],
  },
];

// ---------------------------------------------------------------------------
// Team KPIs by period (4 entries: week, month, quarter, year)
// ---------------------------------------------------------------------------

export const TEAM_KPIS_BY_PERIOD: Record<PeriodValue, TeamKpi[]> = {
  week: [
    { metric_key: 'at_risk_count', label: 'At Risk', value: '1', unit: '', sublabel: 'Declining across 2+ metrics this week', chipLabel: 'Needs attention', status: 'warn', section: 'overview' },
    { metric_key: 'team_dev_time', label: 'Task Dev Time', value: '13h', unit: '', sublabel: 'Team median · Task Delivery', chipLabel: '↓ vs company 16h', status: 'good', section: 'overview' },
    { metric_key: 'focus_gte_60', label: 'Focus ≥ 60%', value: '10 / 12', unit: '', sublabel: '2 members below target', chipLabel: 'On track', status: 'good', section: 'overview' },
    { metric_key: 'not_using_ai', label: 'Not Using AI Tools', value: '6', unit: '', sublabel: 'No AI activity logged this week', chipLabel: 'Action needed', status: 'bad', section: 'overview' },
  ],
  month: [
    { metric_key: 'at_risk_count', label: 'At Risk', value: '2', unit: '', sublabel: 'Declining across 2+ metrics for 2+ months', chipLabel: 'Needs attention', status: 'bad', section: 'overview' },
    { metric_key: 'team_dev_time', label: 'Task Dev Time', value: '14h', unit: '', sublabel: 'Team median · Task Delivery', chipLabel: '↓ vs company 16h', status: 'good', section: 'overview' },
    { metric_key: 'focus_gte_60', label: 'Focus ≥ 60%', value: '9 / 12', unit: '', sublabel: '3 members below target', chipLabel: 'Monitor closely', status: 'warn', section: 'overview' },
    { metric_key: 'not_using_ai', label: 'Not Using AI Tools', value: '5', unit: '', sublabel: 'No AI activity logged this month', chipLabel: 'Action needed', status: 'bad', section: 'overview' },
  ],
  quarter: [
    { metric_key: 'at_risk_count', label: 'At Risk', value: '3', unit: '', sublabel: 'Declining across 2+ metrics for 2+ quarters', chipLabel: 'Needs attention', status: 'bad', section: 'overview' },
    { metric_key: 'team_dev_time', label: 'Task Dev Time', value: '15h', unit: '', sublabel: 'Team median · Task Delivery', chipLabel: '↑ vs company 16h', status: 'warn', section: 'overview' },
    { metric_key: 'focus_gte_60', label: 'Focus ≥ 60%', value: '8 / 12', unit: '', sublabel: '4 members below target', chipLabel: 'Monitor closely', status: 'warn', section: 'overview' },
    { metric_key: 'not_using_ai', label: 'Not Using AI Tools', value: '4', unit: '', sublabel: 'No AI activity logged this quarter', chipLabel: 'Action needed', status: 'bad', section: 'overview' },
  ],
  year: [
    { metric_key: 'at_risk_count', label: 'At Risk', value: '2', unit: '', sublabel: 'Declining across 2+ metrics over the year', chipLabel: 'Needs attention', status: 'bad', section: 'overview' },
    { metric_key: 'team_dev_time', label: 'Task Dev Time', value: '14h', unit: '', sublabel: 'Team median · Task Delivery · annual avg', chipLabel: '↓ vs company 16h', status: 'good', section: 'overview' },
    { metric_key: 'focus_gte_60', label: 'Focus ≥ 60%', value: '9 / 12', unit: '', sublabel: '3 members below target · annual avg', chipLabel: 'Monitor closely', status: 'warn', section: 'overview' },
    { metric_key: 'not_using_ai', label: 'Not Using AI Tools', value: '5', unit: '', sublabel: 'No AI activity logged this year', chipLabel: 'Action needed', status: 'bad', section: 'overview' },
  ],
};

// ---------------------------------------------------------------------------
// Team View Config — thresholds configurable by backend / team lead
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

export const TEAM_DATA_AVAILABILITY: DataAvailability = {
  git:   'available',
  tasks: 'available',
  ci:    'available',
  comms: 'available',
  hr:    'available',
  ai:    'available',
};

/**
 * IC Dashboard — Alice Kim (person_id: 'p1') fixture data
 * Reference data for GET /views/ic/p1?period={p}
 *
 * This is the most detailed IC dashboard mock, used as the gold standard.
 * Every row and metric is hand-crafted. Do not abbreviate or summarise.
 */

import type {
  BulletMetric,
  IcKpi,
  IcDashboardData,
  LocDataPoint,
  DeliveryDataPoint,
  DrillData,
  PeriodValue,
} from '../../../types';

// ---------------------------------------------------------------------------
// Drills — 8 drill types with full row data
// ---------------------------------------------------------------------------

export const ALICE_DRILLS: Record<string, DrillData> = {
  'tasks-completed': {
    title: 'Tasks Completed',
    source: 'Jira',
    srcClass: 'bg-blue-600',
    value: '12',
    filter: 'assignee = alice.kim AND status = Done AND updatedDate >= -30d',
    columns: ['Task', 'Story Points', 'Dev Time', 'Closed'],
    rows: [
      { Task: 'INSIGHT-101', 'Story Points': 3, 'Dev Time': '12h', Closed: '2026-03-28' },
      { Task: 'INSIGHT-98',  'Story Points': 5, 'Dev Time': '18h', Closed: '2026-03-25' },
      { Task: 'INSIGHT-95',  'Story Points': 2, 'Dev Time': '8h',  Closed: '2026-03-22' },
      { Task: 'INSIGHT-91',  'Story Points': 1, 'Dev Time': '4h',  Closed: '2026-03-19' },
      { Task: 'INSIGHT-88',  'Story Points': 3, 'Dev Time': '11h', Closed: '2026-03-17' },
      { Task: 'INSIGHT-85',  'Story Points': 5, 'Dev Time': '22h', Closed: '2026-03-14' },
      { Task: 'INSIGHT-82',  'Story Points': 2, 'Dev Time': '9h',  Closed: '2026-03-12' },
      { Task: 'INSIGHT-79',  'Story Points': 3, 'Dev Time': '13h', Closed: '2026-03-10' },
      { Task: 'INSIGHT-76',  'Story Points': 1, 'Dev Time': '5h',  Closed: '2026-03-07' },
      { Task: 'INSIGHT-73',  'Story Points': 5, 'Dev Time': '20h', Closed: '2026-03-05' },
      { Task: 'INSIGHT-70',  'Story Points': 2, 'Dev Time': '7h',  Closed: '2026-03-03' },
      { Task: 'INSIGHT-67',  'Story Points': 3, 'Dev Time': '15h', Closed: '2026-03-01' },
    ],
  },
  'cycle-time': {
    title: 'Task Development Time',
    source: 'Jira',
    srcClass: 'bg-blue-600',
    value: '14h',
    filter: 'assignee = alice.kim AND status changed to "In Progress"',
    columns: ['Task', 'Story Points', 'Dev Time', 'Status'],
    rows: [
      { Task: 'INSIGHT-103', 'Story Points': 3, 'Dev Time': '14h', Status: 'In Progress' },
      { Task: 'INSIGHT-101', 'Story Points': 5, 'Dev Time': '18h', Status: 'Done' },
    ],
  },
  'task-reopen': {
    title: 'Reopened Tasks',
    source: 'Jira',
    srcClass: 'bg-blue-600',
    value: '1',
    filter: 'assignee = alice.kim AND status changed from Done to "In Progress" after -14d',
    columns: ['Task', 'Reason', 'Reopened', 'Resolved'],
    rows: [
      { Task: 'INSIGHT-87', Reason: 'Regression found in QA', Reopened: '2026-03-20', Resolved: '2026-03-21' },
    ],
  },
  'commits': {
    title: 'Commits',
    source: 'Bitbucket',
    srcClass: 'bg-blue-800',
    value: '34',
    filter: 'author = alice.kim AND date >= 2026-03-01 (showing last 20)',
    columns: ['Commit', 'Repository', '+LOC', '-LOC', 'Date'],
    rows: [
      { Commit: 'a1b2c3d', Repository: 'insight-front',   '+LOC': 120, '-LOC': 45,  Date: '2026-03-31' },
      { Commit: 'e4f5g6h', Repository: 'insight-api',     '+LOC': 88,  '-LOC': 22,  Date: '2026-03-29' },
      { Commit: 'i7j8k9l', Repository: 'insight-front',   '+LOC': 55,  '-LOC': 10,  Date: '2026-03-27' },
      { Commit: 'm1n2o3p', Repository: 'insight-api',     '+LOC': 210, '-LOC': 80,  Date: '2026-03-25' },
      { Commit: 'q4r5s6t', Repository: 'insight-front',   '+LOC': 33,  '-LOC': 5,   Date: '2026-03-24' },
      { Commit: 'u7v8w9x', Repository: 'insight-shared',  '+LOC': 74,  '-LOC': 31,  Date: '2026-03-22' },
      { Commit: 'y0z1a2b', Repository: 'insight-front',   '+LOC': 99,  '-LOC': 44,  Date: '2026-03-20' },
      { Commit: 'c3d4e5f', Repository: 'insight-api',     '+LOC': 140, '-LOC': 60,  Date: '2026-03-18' },
      { Commit: 'g6h7i8j', Repository: 'insight-front',   '+LOC': 28,  '-LOC': 7,   Date: '2026-03-17' },
      { Commit: 'k9l0m1n', Repository: 'insight-shared',  '+LOC': 51,  '-LOC': 19,  Date: '2026-03-15' },
      { Commit: 'o2p3q4r', Repository: 'insight-front',   '+LOC': 175, '-LOC': 92,  Date: '2026-03-13' },
      { Commit: 's5t6u7v', Repository: 'insight-api',     '+LOC': 63,  '-LOC': 14,  Date: '2026-03-12' },
      { Commit: 'w8x9y0z', Repository: 'insight-front',   '+LOC': 44,  '-LOC': 8,   Date: '2026-03-10' },
      { Commit: 'a2b3c4d', Repository: 'insight-shared',  '+LOC': 91,  '-LOC': 35,  Date: '2026-03-08' },
      { Commit: 'e5f6g7h', Repository: 'insight-front',   '+LOC': 38,  '-LOC': 12,  Date: '2026-03-07' },
      { Commit: 'i8j9k0l', Repository: 'insight-api',     '+LOC': 107, '-LOC': 47,  Date: '2026-03-05' },
      { Commit: 'm1n2o3p', Repository: 'insight-front',   '+LOC': 22,  '-LOC': 3,   Date: '2026-03-04' },
      { Commit: 'q4r5s6t', Repository: 'insight-shared',  '+LOC': 66,  '-LOC': 28,  Date: '2026-03-03' },
      { Commit: 'u7v8w9a', Repository: 'insight-front',   '+LOC': 83,  '-LOC': 39,  Date: '2026-03-02' },
      { Commit: 'b0c1d2e', Repository: 'insight-api',     '+LOC': 48,  '-LOC': 17,  Date: '2026-03-01' },
    ],
  },
  'pull-requests': {
    title: 'Pull Requests',
    source: 'Bitbucket',
    srcClass: 'bg-blue-800',
    value: '11',
    filter: 'author = alice.kim AND created >= 2026-03-01',
    columns: ['PR', 'Repository', 'Status', 'Cycle Time'],
    rows: [
      { PR: 'PR #214', Repository: 'insight-front',  Status: 'Merged', 'Cycle Time': '16h' },
      { PR: 'PR #208', Repository: 'insight-api',    Status: 'Merged', 'Cycle Time': '20h' },
      { PR: 'PR #201', Repository: 'insight-front',  Status: 'Merged', 'Cycle Time': '14h' },
      { PR: 'PR #197', Repository: 'insight-shared', Status: 'Merged', 'Cycle Time': '11h' },
      { PR: 'PR #193', Repository: 'insight-api',    Status: 'Merged', 'Cycle Time': '24h' },
      { PR: 'PR #188', Repository: 'insight-front',  Status: 'Merged', 'Cycle Time': '18h' },
      { PR: 'PR #182', Repository: 'insight-front',  Status: 'Merged', 'Cycle Time': '9h'  },
      { PR: 'PR #176', Repository: 'insight-api',    Status: 'Merged', 'Cycle Time': '31h' },
      { PR: 'PR #171', Repository: 'insight-shared', Status: 'Merged', 'Cycle Time': '13h' },
      { PR: 'PR #165', Repository: 'insight-front',  Status: 'Merged', 'Cycle Time': '22h' },
      { PR: 'PR #159', Repository: 'insight-api',    Status: 'Open',   'Cycle Time': '\u2014'   },
    ],
  },
  'reviews': {
    title: 'Code Reviews Given',
    source: 'Bitbucket',
    srcClass: 'bg-blue-800',
    value: '14',
    filter: 'reviewer = alice.kim AND reviewed >= 2026-03-01',
    columns: ['PR', 'Author', 'Outcome', 'Time to Review'],
    rows: [
      { PR: 'PR #215', Author: 'Ben Clarke',    Outcome: 'Approved',           'Time to Review': '2h'  },
      { PR: 'PR #209', Author: 'Tom Sullivan',  Outcome: 'Changes Requested',  'Time to Review': '4h'  },
      { PR: 'PR #206', Author: 'Sara Lee',      Outcome: 'Approved',           'Time to Review': '1h'  },
      { PR: 'PR #203', Author: 'Mike Torres',   Outcome: 'Approved',           'Time to Review': '3h'  },
      { PR: 'PR #199', Author: 'Ben Clarke',    Outcome: 'Changes Requested',  'Time to Review': '5h'  },
      { PR: 'PR #195', Author: 'Tom Sullivan',  Outcome: 'Approved',           'Time to Review': '2h'  },
      { PR: 'PR #191', Author: 'Sara Lee',      Outcome: 'Approved',           'Time to Review': '1h'  },
      { PR: 'PR #186', Author: 'Mike Torres',   Outcome: 'Changes Requested',  'Time to Review': '6h'  },
      { PR: 'PR #180', Author: 'Ben Clarke',    Outcome: 'Approved',           'Time to Review': '2h'  },
      { PR: 'PR #174', Author: 'Tom Sullivan',  Outcome: 'Approved',           'Time to Review': '3h'  },
      { PR: 'PR #168', Author: 'Sara Lee',      Outcome: 'Changes Requested',  'Time to Review': '4h'  },
      { PR: 'PR #162', Author: 'Mike Torres',   Outcome: 'Approved',           'Time to Review': '1h'  },
      { PR: 'PR #156', Author: 'Ben Clarke',    Outcome: 'Approved',           'Time to Review': '2h'  },
      { PR: 'PR #150', Author: 'Tom Sullivan',  Outcome: 'Approved',           'Time to Review': '3h'  },
    ],
  },
  'builds': {
    title: 'Build Results',
    source: 'Bitbucket',
    srcClass: 'bg-blue-800',
    value: '97%',
    filter: 'author = alice.kim AND build.date >= 2026-03-01',
    columns: ['Build', 'Branch', 'Status', 'Duration'],
    rows: [
      { Build: 'build-1042', Branch: 'feat/period-domain', Status: 'Passed', Duration: '3m 12s' },
      { Build: 'build-1039', Branch: 'feat/period-domain', Status: 'Failed', Duration: '1m 58s' },
      { Build: 'build-1038', Branch: 'feat/exec-view', Status: 'Passed', Duration: '3m 05s' },
    ],
  },
  'bugs-fixed': {
    title: 'Bugs Fixed',
    source: 'Jira',
    srcClass: 'bg-blue-600',
    value: '23',
    filter: 'assignee = alice.kim AND issuetype = Bug AND status = Done AND resolved >= 2026-03-01',
    columns: ['Bug', 'Priority', 'Fix Time', 'Closed'],
    rows: [
      { Bug: 'INSIGHT-92',  Priority: 'High',     'Fix Time': '6h',  Closed: '2026-03-31' },
      { Bug: 'INSIGHT-90',  Priority: 'Medium',   'Fix Time': '3h',  Closed: '2026-03-29' },
      { Bug: 'INSIGHT-88',  Priority: 'Critical', 'Fix Time': '11h', Closed: '2026-03-27' },
      { Bug: 'INSIGHT-85',  Priority: 'Medium',   'Fix Time': '3h',  Closed: '2026-03-25' },
      { Bug: 'INSIGHT-83',  Priority: 'Low',      'Fix Time': '1h',  Closed: '2026-03-23' },
      { Bug: 'INSIGHT-80',  Priority: 'High',     'Fix Time': '8h',  Closed: '2026-03-21' },
      { Bug: 'INSIGHT-78',  Priority: 'Medium',   'Fix Time': '4h',  Closed: '2026-03-19' },
      { Bug: 'INSIGHT-75',  Priority: 'High',     'Fix Time': '7h',  Closed: '2026-03-17' },
      { Bug: 'INSIGHT-72',  Priority: 'Low',      'Fix Time': '2h',  Closed: '2026-03-15' },
      { Bug: 'INSIGHT-69',  Priority: 'Medium',   'Fix Time': '5h',  Closed: '2026-03-14' },
      { Bug: 'INSIGHT-66',  Priority: 'Critical', 'Fix Time': '14h', Closed: '2026-03-12' },
      { Bug: 'INSIGHT-63',  Priority: 'Low',      'Fix Time': '1h',  Closed: '2026-03-11' },
      { Bug: 'INSIGHT-60',  Priority: 'High',     'Fix Time': '9h',  Closed: '2026-03-09' },
      { Bug: 'INSIGHT-57',  Priority: 'Medium',   'Fix Time': '3h',  Closed: '2026-03-08' },
      { Bug: 'INSIGHT-54',  Priority: 'Low',      'Fix Time': '2h',  Closed: '2026-03-06' },
      { Bug: 'INSIGHT-51',  Priority: 'High',     'Fix Time': '6h',  Closed: '2026-03-05' },
      { Bug: 'INSIGHT-48',  Priority: 'Medium',   'Fix Time': '4h',  Closed: '2026-03-04' },
      { Bug: 'INSIGHT-45',  Priority: 'Critical', 'Fix Time': '12h', Closed: '2026-03-03' },
      { Bug: 'INSIGHT-42',  Priority: 'Low',      'Fix Time': '1h',  Closed: '2026-03-03' },
      { Bug: 'INSIGHT-39',  Priority: 'Medium',   'Fix Time': '5h',  Closed: '2026-03-02' },
      { Bug: 'INSIGHT-36',  Priority: 'High',     'Fix Time': '7h',  Closed: '2026-03-02' },
      { Bug: 'INSIGHT-33',  Priority: 'Low',      'Fix Time': '2h',  Closed: '2026-03-01' },
      { Bug: 'INSIGHT-30',  Priority: 'Medium',   'Fix Time': '3h',  Closed: '2026-03-01' },
    ],
  },
};

// ---------------------------------------------------------------------------
// Bullet Metrics (month baseline, ~30 metrics)
// ---------------------------------------------------------------------------

export const ALICE_BULLET_METRICS_MONTH: BulletMetric[] = [
  // task_delivery
  { period: 'month', section: 'task_delivery', metric_key: 'tasks_completed', label: 'Tasks Completed', sublabel: 'Jira · closed issues in sprint', value: '12', unit: 'count', range_min: '2', range_max: '12', median: '7', median_label: 'Median: 7', bar_left_pct: 0, bar_width_pct: 100, median_left_pct: 50, status: 'good', drill_id: 'tasks-completed' },
  { period: 'month', section: 'task_delivery', metric_key: 'task_dev_time', label: 'Task Development Time', sublabel: 'Jira · time in In Progress state · lower = better', value: '14', unit: 'h', range_min: '8h', range_max: '31h', median: '15', median_label: 'Median: 15h', bar_left_pct: 0, bar_width_pct: 43, median_left_pct: 48, status: 'good', drill_id: 'cycle-time' },
  { period: 'month', section: 'task_delivery', metric_key: 'estimation_accuracy', label: 'Estimation Accuracy', sublabel: 'Jira · tasks within \u00b120% of original estimate', value: '1.1', unit: '\u00d7', range_min: '0\u00d7', range_max: '3\u00d7', median: '', median_label: 'Target 0.9\u20131.3\u00d7', bar_left_pct: 0, bar_width_pct: 22, median_left_pct: 33, status: 'good', drill_id: '' },
  { period: 'month', section: 'task_delivery', metric_key: 'task_reopen_rate', label: 'Task Reopen Rate', sublabel: 'Jira · closed then reopened within 14 days · lower = better', value: '4', unit: '%', range_min: '0%', range_max: '15%', median: '5', median_label: 'Median: 5%', bar_left_pct: 0, bar_width_pct: 27, median_left_pct: 33, status: 'good', drill_id: 'task-reopen' },
  { period: 'month', section: 'task_delivery', metric_key: 'due_date_compliance', label: 'Due Date Compliance', sublabel: 'Jira · tasks closed by due date', value: '92', unit: '%', range_min: '40%', range_max: '100%', median: '72', median_label: 'Median: 72%', bar_left_pct: 0, bar_width_pct: 87, median_left_pct: 53, status: 'good', drill_id: '' },
  // git_output
  { period: 'month', section: 'git_output', metric_key: 'commits', label: 'Commits Created', sublabel: 'Bitbucket · commits authored', value: '34', unit: 'count', range_min: '8', range_max: '55', median: '22', median_label: 'Median: 22', bar_left_pct: 0, bar_width_pct: 55, median_left_pct: 30, status: 'good', drill_id: 'commits' },
  { period: 'month', section: 'git_output', metric_key: 'prs_created', label: 'Pull Requests Created', sublabel: 'Bitbucket · PRs authored', value: '11', unit: 'count', range_min: '2', range_max: '14', median: '6', median_label: 'Median: 6', bar_left_pct: 0, bar_width_pct: 75, median_left_pct: 33, status: 'good', drill_id: 'pull-requests' },
  { period: 'month', section: 'git_output', metric_key: 'prs_merged', label: 'Pull Requests Merged', sublabel: 'Bitbucket · authored and merged', value: '9', unit: 'count', range_min: '0', range_max: '20', median: '6', median_label: 'Median: 6', bar_left_pct: 0, bar_width_pct: 45, median_left_pct: 38, status: 'good', drill_id: '' },
  { period: 'month', section: 'git_output', metric_key: 'clean_loc', label: 'Clean LOC', sublabel: 'Bitbucket · lines added · excl. spec/config', value: '12k', unit: 'count', range_min: '1k', range_max: '18k', median: '7k', median_label: 'Median: 7k', bar_left_pct: 0, bar_width_pct: 65, median_left_pct: 35, status: 'good', drill_id: '' },
  // code_quality
  { period: 'month', section: 'code_quality', metric_key: 'reviews_given', label: 'Reviews Given', sublabel: 'Bitbucket · PRs reviewed', value: '14', unit: 'count', range_min: '0', range_max: '20', median: '8', median_label: 'Median: 8', bar_left_pct: 0, bar_width_pct: 70, median_left_pct: 40, status: 'good', drill_id: 'reviews' },
  { period: 'month', section: 'code_quality', metric_key: 'rework_ratio', label: 'Rework Ratio', sublabel: 'Bitbucket · lines changed in follow-up commits · lower = better', value: '12', unit: '%', range_min: '0%', range_max: '50%', median: '', median_label: 'Target <20%', bar_left_pct: 0, bar_width_pct: 12, median_left_pct: 20, status: 'good', drill_id: '' },
  { period: 'month', section: 'code_quality', metric_key: 'build_success', label: 'Build Success Rate', sublabel: 'CI · passed \u00f7 total runs · target \u226590%', value: '94', unit: '%', range_min: '0%', range_max: '100%', median: '87', median_label: 'Target \u226590% · Median: 87%', bar_left_pct: 0, bar_width_pct: 94, median_left_pct: 83, status: 'good', drill_id: 'builds' },
  { period: 'month', section: 'code_quality', metric_key: 'pr_cycle_time', label: 'Pull Request Cycle Time', sublabel: 'Bitbucket · PR opened \u2192 merged · lower = better', value: '18', unit: 'h', range_min: '0h', range_max: '72h', median: '24', median_label: 'Median: 24h', bar_left_pct: 0, bar_width_pct: 36, median_left_pct: 35, status: 'good', drill_id: 'pull-requests' },
  { period: 'month', section: 'code_quality', metric_key: 'pickup_time', label: 'Pickup Time', sublabel: 'Bitbucket · PR opened \u2192 first review · lower = better', value: '4.2', unit: 'h', range_min: '0h', range_max: '48h', median: '', median_label: 'Target <24h', bar_left_pct: 0, bar_width_pct: 17, median_left_pct: 24, status: 'good', drill_id: '' },
  { period: 'month', section: 'code_quality', metric_key: 'bugs_fixed', label: 'Bugs Fixed', sublabel: 'Jira · bug-type issues closed', value: '23', unit: 'count', range_min: '0', range_max: '30', median: '9', median_label: 'Median: 9', bar_left_pct: 0, bar_width_pct: 77, median_left_pct: 30, status: 'good', drill_id: 'bugs-fixed' },
  { period: 'month', section: 'code_quality', metric_key: 'bug_reopen_rate', label: 'Bug Reopen Rate', sublabel: 'Jira · bugs reopened · lower = better', value: '9', unit: '%', range_min: '0%', range_max: '30%', median: '14', median_label: 'Median: 14% · Target <15%', bar_left_pct: 0, bar_width_pct: 30, median_left_pct: 47, status: 'good', drill_id: '' },
  // ai_tools
  { period: 'month', section: 'ai_tools', metric_key: 'cursor_completions', label: 'Cursor Completions', sublabel: 'Cursor · completions suggested this month', value: '1.2k', unit: 'count', range_min: '200', range_max: '5k', median: '800', median_label: 'Median: 800', bar_left_pct: 0, bar_width_pct: 24, median_left_pct: 16, status: 'good', drill_id: '' },
  { period: 'month', section: 'ai_tools', metric_key: 'cursor_agents', label: 'Cursor Agent Sessions', sublabel: 'Cursor · agentic sessions started', value: '18', unit: 'count', range_min: '2', range_max: '40', median: '10', median_label: 'Median: 10', bar_left_pct: 0, bar_width_pct: 45, median_left_pct: 25, status: 'good', drill_id: '' },
  { period: 'month', section: 'ai_tools', metric_key: 'cursor_lines', label: 'Lines Accepted', sublabel: 'Cursor · lines of code accepted', value: '3.2k', unit: 'count', range_min: '0', range_max: '5k', median: '1.8k', median_label: 'Median: 1.8k', bar_left_pct: 0, bar_width_pct: 64, median_left_pct: 36, status: 'good', drill_id: '' },
  { period: 'month', section: 'ai_tools', metric_key: 'cc_sessions', label: 'Claude Code Sessions', sublabel: 'Anthropic Enterprise API · sessions this month', value: '24', unit: 'count', range_min: '0', range_max: '60', median: '12', median_label: 'Median: 12', bar_left_pct: 0, bar_width_pct: 40, median_left_pct: 20, status: 'good', drill_id: '' },
  { period: 'month', section: 'ai_tools', metric_key: 'cc_tool_accept', label: 'Tool Acceptance Rate', sublabel: 'Anthropic Enterprise API · accepted \u00f7 offered', value: '76', unit: '%', range_min: '0%', range_max: '100%', median: '64', median_label: 'Median: 64%', bar_left_pct: 0, bar_width_pct: 76, median_left_pct: 64, status: 'good', drill_id: '' },
  { period: 'month', section: 'ai_tools', metric_key: 'cc_lines', label: 'Lines Added (Claude Code)', sublabel: 'Anthropic Enterprise API · lines added by Claude Code', value: '1.1k', unit: 'count', range_min: '0', range_max: '3k', median: '600', median_label: 'Median: 600', bar_left_pct: 0, bar_width_pct: 37, median_left_pct: 20, status: 'good', drill_id: '' },
  { period: 'month', section: 'ai_tools', metric_key: 'ai_loc_share2', label: 'AI LOC Share', sublabel: 'Cursor + Claude Code · accepted lines \u00f7 clean LOC', value: '27', unit: '%', range_min: '0%', range_max: '34%', median: '18', median_label: 'Median: 18%', bar_left_pct: 0, bar_width_pct: 79, median_left_pct: 53, status: 'good', drill_id: '' },
  { period: 'month', section: 'ai_tools', metric_key: 'claude_web', label: 'Claude Web Usage', sublabel: 'Claude Web · conversations this month', value: '32', unit: 'count', range_min: '0', range_max: '80', median: '18', median_label: 'Median: 18', bar_left_pct: 0, bar_width_pct: 40, median_left_pct: 23, status: 'good', drill_id: '' },
  { period: 'month', section: 'ai_tools', metric_key: 'chatgpt', label: 'ChatGPT Usage', sublabel: 'ChatGPT · conversations this month', value: '8', unit: 'count', range_min: '0', range_max: '40', median: '12', median_label: 'Median: 12', bar_left_pct: 0, bar_width_pct: 20, median_left_pct: 30, status: 'good', drill_id: '' },
  // collab
  { period: 'month', section: 'collab', metric_key: 'slack_thread_participation', label: 'Thread Participation', sublabel: 'Slack · replies to others\' threads', value: '34', unit: 'replies', range_min: '0', range_max: '80', median: '29', median_label: 'Median: 29', bar_left_pct: 0, bar_width_pct: 43, median_left_pct: 36, status: 'good', drill_id: '' },
  { period: 'month', section: 'collab', metric_key: 'slack_message_engagement', label: 'Message Engagement', sublabel: 'Slack · avg replies per thread', value: '2.1', unit: 'avg', range_min: '0', range_max: '5', median: '1.8', median_label: 'Median: 1.8', bar_left_pct: 0, bar_width_pct: 42, median_left_pct: 36, status: 'good', drill_id: '' },
  { period: 'month', section: 'collab', metric_key: 'slack_dm_ratio', label: 'DM Ratio', sublabel: 'Slack · DMs \u00f7 all messages · lower = more open', value: '24', unit: '%', range_min: '0%', range_max: '100%', median: '28', median_label: 'Median: 28% · lower = more open', bar_left_pct: 0, bar_width_pct: 24, median_left_pct: 28, status: 'good', drill_id: '' },
  { period: 'month', section: 'collab', metric_key: 'm365_teams_messages', label: 'Teams Messages', sublabel: 'Microsoft Teams · all channels sent', value: '168', unit: '/mo', range_min: '0', range_max: '400', median: '148', median_label: 'Median: 148', bar_left_pct: 0, bar_width_pct: 42, median_left_pct: 37, status: 'good', drill_id: '' },
  { period: 'month', section: 'collab', metric_key: 'm365_emails_sent', label: 'Emails Sent', sublabel: 'M365 · avg per member · lower = better', value: '31', unit: '/mo', range_min: '0', range_max: '120', median: '35', median_label: 'Median: 35 · lower = better', bar_left_pct: 0, bar_width_pct: 26, median_left_pct: 29, status: 'good', drill_id: '' },
  { period: 'month', section: 'collab', metric_key: 'm365_files_shared', label: 'Files Shared', sublabel: 'M365 · avg per member', value: '9', unit: '/mo', range_min: '0', range_max: '30', median: '8', median_label: 'Median: 8', bar_left_pct: 0, bar_width_pct: 30, median_left_pct: 27, status: 'good', drill_id: '' },
  { period: 'month', section: 'collab', metric_key: 'focus_time_pct', label: 'Focus Time', sublabel: 'Calendar / M365 · uninterrupted blocks \u226560 min', value: '68', unit: '%', range_min: '42%', range_max: '81%', median: '63', median_label: 'Median: 63%', bar_left_pct: 0, bar_width_pct: 67, median_left_pct: 54, status: 'good', drill_id: '' },
  { period: 'month', section: 'collab', metric_key: 'meeting_hours', label: 'Meeting Hours', sublabel: 'Zoom · meeting duration + M365 audioDuration · lower = more focus time', value: '12', unit: 'h/mo', range_min: '4h', range_max: '28h', median: '16', median_label: 'Median: 16h', bar_left_pct: 0, bar_width_pct: 33, median_left_pct: 50, status: 'good', drill_id: '' },
  { period: 'month', section: 'collab', metric_key: 'zoom_calls', label: 'Zoom Calls', sublabel: 'Zoom API · avg calls attended per member', value: '11', unit: '/mo', range_min: '0', range_max: '20', median: '9', median_label: 'Median: 9', bar_left_pct: 0, bar_width_pct: 55, median_left_pct: 45, status: 'good', drill_id: '' },
  { period: 'month', section: 'collab', metric_key: 'meeting_free', label: 'Meeting-Free Days', sublabel: 'Zoom · days with no meetings + M365 · avg · higher = better', value: '5', unit: 'days', range_min: '0', range_max: '10', median: '4', median_label: 'Median: 4', bar_left_pct: 0, bar_width_pct: 50, median_left_pct: 40, status: 'good', drill_id: '' },
];

// ---------------------------------------------------------------------------
// KPIs (month baseline, 5 items)
// ---------------------------------------------------------------------------

export const ALICE_KPIS_MONTH: IcKpi[] = [
  { period: 'month', metric_key: 'bugs_fixed', label: 'Bugs Fixed', value: '23', unit: '', sublabel: 'Jira', description: 'Bug tickets resolved and closed.', delta: '\u2191 5 vs last month', delta_type: 'good' },
  { period: 'month', metric_key: 'clean_loc', label: 'Clean LOC', value: '12k', unit: '', sublabel: 'Bitbucket', description: 'Production code lines, excl. tests & generated files.', delta: '\u2191 8% vs median 19%', delta_type: 'good' },
  { period: 'month', metric_key: 'ai_loc_share', label: 'AI LOC Share', value: '27', unit: '%', sublabel: 'Cursor + Claude Code', description: 'Code lines accepted from AI suggestions out of total authored.', delta: '\u2248 team avg 22%', delta_type: 'neutral' },
  { period: 'month', metric_key: 'spec_lines', label: 'Spec Lines', value: '840', unit: '', sublabel: 'Jira', description: 'Lines written in Markdown specification files.', delta: '\u2191 12% vs last month', delta_type: 'good' },
  { period: 'month', metric_key: 'focus_time_pct', label: 'Focus Time', value: '68', unit: '%', sublabel: 'Calendar / M365', description: 'Work time in uninterrupted 60-min+ blocks.', delta: '\u2191 5% vs last month', delta_type: 'good' },
];

// ---------------------------------------------------------------------------
// Charts (month baseline)
// ---------------------------------------------------------------------------

export const ALICE_CHARTS_MONTH: { locTrend: LocDataPoint[]; deliveryTrend: DeliveryDataPoint[] } = {
  locTrend: [
    { label: 'W1', aiLoc: 780, codeLoc: 2100, specLines: 180 },
    { label: 'W2', aiLoc: 920, codeLoc: 2800, specLines: 210 },
    { label: 'W3', aiLoc: 860, codeLoc: 3100, specLines: 240 },
    { label: 'W4', aiLoc: 1040, codeLoc: 3200, specLines: 210 },
  ],
  deliveryTrend: [
    { label: 'W1', commits: 7, prsMerged: 2, tasksDone: 3 },
    { label: 'W2', commits: 9, prsMerged: 3, tasksDone: 3 },
    { label: 'W3', commits: 8, prsMerged: 3, tasksDone: 3 },
    { label: 'W4', commits: 10, prsMerged: 3, tasksDone: 3 },
  ],
};

// ---------------------------------------------------------------------------
// Full 4-period IC Dashboard (references above constants for month,
// hand-crafted week/quarter/year chart data)
// ---------------------------------------------------------------------------

/**
 * Helper: scale KPIs by period factor, apply rate overrides for non-count metrics.
 */
const IC_KPI_RATE_OVERRIDES: Record<PeriodValue, Record<string, string>> = {
  week:    { ai_loc_share: '31', focus_time_pct: '72' },
  month:   {},
  quarter: { ai_loc_share: '29', focus_time_pct: '65' },
  year:    { ai_loc_share: '25', focus_time_pct: '63' },
};

function scaleAliceKpis(kpis: IcKpi[], period: PeriodValue, factor: number): IcKpi[] {
  const rateOverrides = IC_KPI_RATE_OVERRIDES[period];
  return kpis.map((k) => {
    if (rateOverrides[k.metric_key] !== undefined) {
      return { ...k, period, value: rateOverrides[k.metric_key] as string };
    }
    if (['bugs_fixed', 'clean_loc', 'spec_lines'].includes(k.metric_key)) {
      const raw = parseFloat(k.value.replace('k', ''));
      const scaled = k.value.includes('k')
        ? `${(raw * factor).toFixed(0)}k`
        : String(Math.round(raw * factor));
      return { ...k, period, value: scaled };
    }
    return { ...k, period };
  });
}

/**
 * Helper: apply rate overrides to bullet metrics per period.
 * Values ending in '%' are stored without the symbol — the unit field carries it.
 */
const IC_BULLET_RATE_OVERRIDES: Record<PeriodValue, Record<string, string>> = {
  week:    { build_success: '99', focus_time_pct: '72', ai_loc_share2: '31', due_date_compliance: '95', task_reopen_rate: '2', cc_tool_accept: '80', rework_ratio: '8', pr_cycle_time: '14', pickup_time: '3.1', bug_reopen_rate: '6' },
  month:   {},
  quarter: { build_success: '95', focus_time_pct: '65', ai_loc_share2: '28', due_date_compliance: '87', task_reopen_rate: '5', cc_tool_accept: '74', rework_ratio: '14', pr_cycle_time: '20', pickup_time: '5.4', bug_reopen_rate: '11' },
  year:    { build_success: '93', focus_time_pct: '63', ai_loc_share2: '24', due_date_compliance: '83', task_reopen_rate: '6', cc_tool_accept: '71', rework_ratio: '16', pr_cycle_time: '22', pickup_time: '6.0', bug_reopen_rate: '13' },
};

/**
 * Count-accumulating metric keys — totals that should scale with the period factor.
 * Rate / average / cycle-time metrics are intentionally excluded.
 */
const BULLET_ACCUMULATE_KEYS = new Set([
  'tasks_completed',
  'commits', 'prs_created', 'prs_merged', 'clean_loc',
  'reviews_given', 'bugs_fixed',
  'cursor_completions', 'cursor_agents', 'cursor_lines',
  'cc_sessions', 'cc_lines',
  'claude_web', 'chatgpt',
  'slack_thread_participation',
  'm365_teams_messages', 'm365_emails_sent', 'm365_files_shared',
  'zoom_calls', 'meeting_free', 'meeting_hours',
]);

/**
 * Scale a numeric string that may end with 'k' or 'h' (e.g. '12', '3.2k', '4h').
 * Produces compact output: '1.8k', '7h', '36', etc.
 */
function scaleCountStr(raw: string, factor: number): string {
  const match = raw.match(/^([\d.]+)(k|h)?$/i);
  if (!match) return raw;
  const [, numStr, sfx = ''] = match;
  const num = parseFloat(numStr) * (sfx.toLowerCase() === 'k' ? 1000 : 1);
  const scaled = num * factor;

  if (sfx.toLowerCase() === 'h') {
    const f = numStr.includes('.') ? scaled.toFixed(1) : String(Math.round(scaled));
    return `${f}h`;
  }
  if (sfx.toLowerCase() === 'k' || scaled >= 1000) {
    const kVal = scaled / 1000;
    return `${parseFloat(kVal.toFixed(1))}k`;
  }
  return numStr.includes('.') ? scaled.toFixed(1) : String(Math.round(scaled));
}

/**
 * Scale the first number (with optional k/h suffix) found in a label string.
 * E.g. 'Median: 7k' × 3 → 'Median: 21k', 'Median: 16h' × 0.25 → 'Median: 4h'
 */
function scaleNumInLabel(label: string, factor: number): string {
  return label.replace(/([\d.]+)(k|h)?/i, (_, num, sfx = '') => {
    const raw = parseFloat(num) * (sfx.toLowerCase() === 'k' ? 1000 : 1);
    const scaled = raw * factor;
    if (sfx.toLowerCase() === 'h') {
      const f = num.includes('.') ? scaled.toFixed(1) : String(Math.round(scaled));
      return `${f}h`;
    }
    if (sfx.toLowerCase() === 'k' || scaled >= 1000) {
      return `${parseFloat((scaled / 1000).toFixed(1))}k`;
    }
    return num.includes('.') ? scaled.toFixed(1) : String(Math.round(scaled));
  });
}

function scaleAliceBullets(metrics: BulletMetric[], period: PeriodValue, factor: number): BulletMetric[] {
  const overrides = IC_BULLET_RATE_OVERRIDES[period];
  return metrics.map((m) => {
    if (overrides[m.metric_key] !== undefined) {
      // Rate/ratio override: update value; for %-metrics also update bar_width_pct
      const newValue = overrides[m.metric_key] as string;
      const numMatch = newValue.match(/^(\d+(?:\.\d+)?)/);
      const newBarPct = (m.unit === '%' && numMatch) ? parseFloat(numMatch[1]) : m.bar_width_pct;
      return { ...m, period, value: newValue, bar_width_pct: newBarPct };
    }
    if (factor !== 1.0 && BULLET_ACCUMULATE_KEYS.has(m.metric_key)) {
      // Count metric: scale value, range, median; bar_width_pct stays (proportional)
      return {
        ...m,
        period,
        value:        scaleCountStr(m.value, factor),
        range_min:    scaleCountStr(m.range_min, factor),
        range_max:    scaleCountStr(m.range_max, factor),
        median:       m.median ? scaleCountStr(m.median, factor) : m.median,
        median_label: scaleNumInLabel(m.median_label, factor),
      };
    }
    return { ...m, period };
  });
}

export const IC_DASHBOARD_MOCK: Record<string, Record<PeriodValue, IcDashboardData>> = {
  p1: {
    week: {
      person: { person_id: 'p1', name: 'Alice Kim', role: 'Senior Backend Developer', seniority: 'Senior' },
      kpis: scaleAliceKpis(ALICE_KPIS_MONTH, 'week', 0.25),
      bulletMetrics: scaleAliceBullets(ALICE_BULLET_METRICS_MONTH, 'week', 0.25),
      charts: {
        locTrend: [
          { label: 'Mon', aiLoc: 200, codeLoc: 520, specLines: 40 },
          { label: 'Tue', aiLoc: 240, codeLoc: 680, specLines: 55 },
          { label: 'Wed', aiLoc: 190, codeLoc: 600, specLines: 48 },
          { label: 'Thu', aiLoc: 260, codeLoc: 750, specLines: 60 },
          { label: 'Fri', aiLoc: 150, codeLoc: 430, specLines: 35 },
        ],
        deliveryTrend: [
          { label: 'Mon', commits: 2, prsMerged: 0, tasksDone: 1 },
          { label: 'Tue', commits: 3, prsMerged: 1, tasksDone: 1 },
          { label: 'Wed', commits: 2, prsMerged: 1, tasksDone: 0 },
          { label: 'Thu', commits: 3, prsMerged: 1, tasksDone: 1 },
          { label: 'Fri', commits: 2, prsMerged: 0, tasksDone: 0 },
        ],
      },
      timeOffNotice: null,
      drills: ALICE_DRILLS,
    },
    month: {
      person: { person_id: 'p1', name: 'Alice Kim', role: 'Senior Backend Developer', seniority: 'Senior' },
      kpis: ALICE_KPIS_MONTH,
      bulletMetrics: ALICE_BULLET_METRICS_MONTH,
      charts: ALICE_CHARTS_MONTH,
      timeOffNotice: { days: 5, dateRange: 'Jul 14\u201318', bambooHrUrl: '#bamboo' },
      drills: ALICE_DRILLS,
    },
    quarter: {
      person: { person_id: 'p1', name: 'Alice Kim', role: 'Senior Backend Developer', seniority: 'Senior' },
      kpis: scaleAliceKpis(ALICE_KPIS_MONTH, 'quarter', 3),
      bulletMetrics: scaleAliceBullets(ALICE_BULLET_METRICS_MONTH, 'quarter', 3),
      charts: {
        locTrend: [
          { label: 'Jan', aiLoc: 2600, codeLoc: 9200, specLines: 640 },
          { label: 'Feb', aiLoc: 3100, codeLoc: 10400, specLines: 720 },
          { label: 'Mar', aiLoc: 3600, codeLoc: 11800, specLines: 840 },
        ],
        deliveryTrend: [
          { label: 'Jan', commits: 28, prsMerged: 9, tasksDone: 10 },
          { label: 'Feb', commits: 32, prsMerged: 10, tasksDone: 11 },
          { label: 'Mar', commits: 34, prsMerged: 11, tasksDone: 12 },
        ],
      },
      timeOffNotice: { days: 5, dateRange: 'Jul 14\u201318', bambooHrUrl: '#bamboo' },
      drills: ALICE_DRILLS,
    },
    year: {
      person: { person_id: 'p1', name: 'Alice Kim', role: 'Senior Backend Developer', seniority: 'Senior' },
      kpis: scaleAliceKpis(ALICE_KPIS_MONTH, 'year', 12),
      bulletMetrics: scaleAliceBullets(ALICE_BULLET_METRICS_MONTH, 'year', 12),
      charts: {
        locTrend: [
          { label: 'Q1', aiLoc: 9300, codeLoc: 31500, specLines: 2100 },
          { label: 'Q2', aiLoc: 10200, codeLoc: 34800, specLines: 2400 },
          { label: 'Q3', aiLoc: 11400, codeLoc: 37200, specLines: 2700 },
          { label: 'Q4', aiLoc: 12800, codeLoc: 40100, specLines: 3000 },
        ],
        deliveryTrend: [
          { label: 'Q1', commits: 94, prsMerged: 30, tasksDone: 33 },
          { label: 'Q2', commits: 108, prsMerged: 34, tasksDone: 38 },
          { label: 'Q3', commits: 116, prsMerged: 37, tasksDone: 42 },
          { label: 'Q4', commits: 124, prsMerged: 40, tasksDone: 47 },
        ],
      },
      timeOffNotice: { days: 5, dateRange: 'Jul 14\u201318', bambooHrUrl: '#bamboo' },
      drills: ALICE_DRILLS,
    },
  },
};

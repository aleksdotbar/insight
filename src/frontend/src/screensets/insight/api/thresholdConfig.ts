/**
 * Threshold & metric configuration
 *
 * Centralizes the static metadata that the backend does not return in every
 * response: labels, units, default percentile ranges, and status thresholds.
 *
 * Values are extracted from the hand-crafted mock fixtures so that the
 * transform layer can work with raw backend aggregates.
 */

// ---------------------------------------------------------------------------
// Bullet metric definitions
// ---------------------------------------------------------------------------

export type BulletThresholdDef = {
  metric_key: string;
  section: string;
  label: string;
  sublabel: string;
  unit: string;
  drill_id: string;
  higher_is_better: boolean;
  range_min: number;
  range_max: number;
  median: number;
  good_threshold: number;
  warn_threshold: number;
};

/**
 * Team-level bullet metric definitions.
 *
 * Sections match the IDs used by the Team View bullet sections:
 *   task_delivery, code_quality, estimation, ai_adoption, collaboration
 */
export const BULLET_DEFS: BulletThresholdDef[] = [
  // --- task_delivery ---
  { metric_key: 'tasks_completed',    section: 'task_delivery', label: 'Tasks Closed / Developer',           sublabel: 'Jira \u00b7 closed issues in sprint \u00b7 team median per developer', unit: 'tasks',  drill_id: 'team-tasks',     higher_is_better: true,  range_min: 0,  range_max: 15,  median: 5.8, good_threshold: 5,   warn_threshold: 3   },
  { metric_key: 'task_dev_time',      section: 'task_delivery', label: 'Task Development Time',              sublabel: 'Jira \u00b7 time in In Progress state \u00b7 lower = better',          unit: 'h',      drill_id: 'team-dev-time',  higher_is_better: false, range_min: 8,  range_max: 31,  median: 15,  good_threshold: 15,  warn_threshold: 22  },
  { metric_key: 'task_reopen_rate',   section: 'task_delivery', label: 'Task Reopen Rate',                   sublabel: 'Jira \u00b7 closed then reopened within 14 days \u00b7 lower = better', unit: '%',      drill_id: 'team-reopen',    higher_is_better: false, range_min: 0,  range_max: 15,  median: 5,   good_threshold: 5,   warn_threshold: 10  },
  { metric_key: 'due_date_compliance', section: 'task_delivery', label: 'Due Date Compliance',               sublabel: 'Jira \u00b7 tasks closed by due date',                                 unit: '%',      drill_id: '',               higher_is_better: true,  range_min: 40, range_max: 100, median: 72,  good_threshold: 72,  warn_threshold: 55  },

  // --- code_quality ---
  { metric_key: 'prs_per_dev',        section: 'code_quality',  label: 'Pull Requests Merged / Developer',   sublabel: 'Bitbucket \u00b7 authored and merged \u00b7 team median',              unit: '',       drill_id: 'team-prs',       higher_is_better: true,  range_min: 0,  range_max: 20,  median: 6,   good_threshold: 6,   warn_threshold: 3   },
  { metric_key: 'build_success',      section: 'code_quality',  label: 'Build Success Rate',                 sublabel: 'CI \u00b7 passed \u00f7 total runs \u00b7 target \u226590%',            unit: '%',      drill_id: 'team-build',     higher_is_better: true,  range_min: 78, range_max: 97,  median: 89,  good_threshold: 90,  warn_threshold: 80  },
  { metric_key: 'pr_cycle_time',      section: 'code_quality',  label: 'Pull Request Cycle Time',            sublabel: 'Bitbucket \u00b7 PR opened \u2192 merged \u00b7 lower = better',       unit: 'h',      drill_id: 'team-pr-cycle',  higher_is_better: false, range_min: 10, range_max: 35,  median: 22,  good_threshold: 22,  warn_threshold: 28  },
  { metric_key: 'bugs_fixed',         section: 'code_quality',  label: 'Bugs Fixed',                         sublabel: 'Jira \u00b7 bug-type issues closed',                                   unit: 'count',  drill_id: 'team-bugs',      higher_is_better: true,  range_min: 1,  range_max: 8,   median: 3,   good_threshold: 3,   warn_threshold: 1   },

  // --- estimation ---
  { metric_key: 'estimation_accuracy', section: 'estimation',   label: 'Within \u00b120% of estimate',       sublabel: 'Jira \u00b7 original estimate vs time spent',                          unit: '%',      drill_id: '',               higher_is_better: true,  range_min: 0,  range_max: 100, median: 58,  good_threshold: 55,  warn_threshold: 40  },
  { metric_key: 'overrun_ratio',       section: 'estimation',   label: 'Median overrun ratio',               sublabel: 'Jira \u00b7 actual \u00f7 estimated \u00b7 lower = better',            unit: '\u00d7', drill_id: '',               higher_is_better: false, range_min: 1,  range_max: 3,   median: 1.5, good_threshold: 1.5, warn_threshold: 2   },
  { metric_key: 'scope_completion',    section: 'estimation',   label: 'Scope Completion Rate',              sublabel: 'Jira \u00b7 tasks done \u00f7 committed at sprint start',              unit: '%',      drill_id: '',               higher_is_better: true,  range_min: 0,  range_max: 100, median: 79,  good_threshold: 75,  warn_threshold: 60  },
  { metric_key: 'scope_creep',         section: 'estimation',   label: 'Scope Creep Rate',                   sublabel: 'Jira \u00b7 added mid-sprint \u00f7 original count \u00b7 lower = better', unit: '%',  drill_id: '',               higher_is_better: false, range_min: 0,  range_max: 50,  median: 19,  good_threshold: 19,  warn_threshold: 30  },
  { metric_key: 'on_time_delivery',    section: 'estimation',   label: 'On-time Delivery Rate',              sublabel: 'Jira \u00b7 closed by due date',                                       unit: '%',      drill_id: '',               higher_is_better: true,  range_min: 0,  range_max: 100, median: 71,  good_threshold: 70,  warn_threshold: 55  },
  { metric_key: 'avg_slip',            section: 'estimation',   label: 'Avg Slip When Late',                 sublabel: 'Jira \u00b7 days past due date \u00b7 lower = better',                 unit: 'd',      drill_id: '',               higher_is_better: false, range_min: 0,  range_max: 6,   median: 3.1, good_threshold: 3.1, warn_threshold: 4.5 },

  // --- ai_adoption ---
  { metric_key: 'active_ai_members',  section: 'ai_adoption',   label: 'Active members',                     sublabel: 'Cursor \u00b7 Claude Code \u00b7 Codex \u00b7 any activity this month', unit: '/ 12',  drill_id: 'team-ai-active', higher_is_better: true,  range_min: 0,  range_max: 12,  median: 7,   good_threshold: 6,   warn_threshold: 3   },
  { metric_key: 'cursor_active',      section: 'ai_adoption',   label: 'Cursor \u2014 active members',       sublabel: 'Cursor \u00b7 any activity this month',                                unit: '/ 12',  drill_id: '',               higher_is_better: true,  range_min: 0,  range_max: 12,  median: 6,   good_threshold: 5,   warn_threshold: 3   },
  { metric_key: 'cc_active',          section: 'ai_adoption',   label: 'Claude Code \u2014 active members',  sublabel: 'Anthropic Enterprise API \u00b7 sessions this month',                  unit: '/ 12',  drill_id: '',               higher_is_better: true,  range_min: 0,  range_max: 12,  median: 3,   good_threshold: 3,   warn_threshold: 1   },
  { metric_key: 'codex_active',       section: 'ai_adoption',   label: 'Codex \u2014 active members',        sublabel: 'OpenAI API \u00b7 completions this month',                             unit: '/ 12',  drill_id: '',               higher_is_better: true,  range_min: 0,  range_max: 12,  median: 2,   good_threshold: 2,   warn_threshold: 1   },
  { metric_key: 'team_ai_loc',        section: 'ai_adoption',   label: 'Team AI LOC Share',                  sublabel: 'Cursor + Claude Code \u00b7 accepted lines \u00f7 clean LOC',          unit: '%',      drill_id: 'team-ai-loc',    higher_is_better: true,  range_min: 0,  range_max: 50,  median: 14,  good_threshold: 14,  warn_threshold: 8   },
  { metric_key: 'cursor_acceptance',  section: 'ai_adoption',   label: 'Cursor Acceptance Rate',             sublabel: 'Cursor \u00b7 accepted \u00f7 shown completions \u00b7 team median',   unit: '%',      drill_id: '',               higher_is_better: true,  range_min: 0,  range_max: 100, median: 58,  good_threshold: 55,  warn_threshold: 35  },
  { metric_key: 'cc_tool_acceptance', section: 'ai_adoption',   label: 'Claude Code Tool Acceptance',        sublabel: 'Anthropic Enterprise API \u00b7 accepted \u00f7 offered \u00b7 team median', unit: '%', drill_id: '',               higher_is_better: true,  range_min: 0,  range_max: 100, median: 64,  good_threshold: 60,  warn_threshold: 40  },

  // --- collaboration ---
  { metric_key: 'slack_thread_participation', section: 'collaboration', label: 'Thread Participation',   sublabel: 'Slack \u00b7 replies to others\' threads',                                     unit: 'replies', drill_id: '',   higher_is_better: true,  range_min: 0,   range_max: 80,  median: 29,  good_threshold: 25,  warn_threshold: 15  },
  { metric_key: 'slack_message_engagement',   section: 'collaboration', label: 'Message Engagement',     sublabel: 'Slack \u00b7 avg replies per thread',                                          unit: 'avg',     drill_id: '',   higher_is_better: true,  range_min: 0,   range_max: 5,   median: 1.8, good_threshold: 1.5, warn_threshold: 0.8 },
  { metric_key: 'slack_dm_ratio',             section: 'collaboration', label: 'DM Ratio',               sublabel: 'Slack \u00b7 DMs \u00f7 all messages \u00b7 lower = more open',                unit: '%',       drill_id: '',   higher_is_better: false, range_min: 0,   range_max: 100, median: 28,  good_threshold: 30,  warn_threshold: 50  },
  { metric_key: 'm365_teams_messages',        section: 'collaboration', label: 'Teams Messages',         sublabel: 'Microsoft Teams \u00b7 all channels sent',                                     unit: '/mo',     drill_id: '',   higher_is_better: true,  range_min: 0,   range_max: 400, median: 148, good_threshold: 100, warn_threshold: 50  },
  { metric_key: 'm365_emails_sent',           section: 'collaboration', label: 'Emails Sent',            sublabel: 'M365 \u00b7 avg per member \u00b7 lower = better',                             unit: '/mo',     drill_id: '',   higher_is_better: false, range_min: 0,   range_max: 120, median: 35,  good_threshold: 40,  warn_threshold: 70  },
  { metric_key: 'm365_files_shared',          section: 'collaboration', label: 'Files Shared',           sublabel: 'M365 \u00b7 avg per member',                                                   unit: '/mo',     drill_id: '',   higher_is_better: true,  range_min: 0,   range_max: 30,  median: 8,   good_threshold: 6,   warn_threshold: 3   },
  { metric_key: 'meeting_hours',              section: 'collaboration', label: 'Meeting Hours',          sublabel: 'Zoom \u00b7 meeting duration + M365 audioDuration \u00b7 avg \u00b7 lower = better', unit: 'h/mo', drill_id: '',   higher_is_better: false, range_min: 0,   range_max: 72,  median: 38,  good_threshold: 40,  warn_threshold: 55  },
  { metric_key: 'zoom_calls',                 section: 'collaboration', label: 'Zoom Calls',             sublabel: 'Zoom API \u00b7 avg calls attended per member',                                 unit: '/mo',     drill_id: '',   higher_is_better: true,  range_min: 0,   range_max: 20,  median: 9,   good_threshold: 6,   warn_threshold: 3   },
  { metric_key: 'meeting_free',               section: 'collaboration', label: 'Meeting-Free Days',      sublabel: 'Zoom \u00b7 days with no meetings + M365 \u00b7 avg \u00b7 higher = better',   unit: 'days',    drill_id: '',   higher_is_better: true,  range_min: 0,   range_max: 10,  median: 4,   good_threshold: 4,   warn_threshold: 2   },
];

// ---------------------------------------------------------------------------
// IC KPI definitions
// ---------------------------------------------------------------------------

export type IcKpiDef = {
  metric_key: string;
  raw_field: string;
  label: string;
  unit: string;
  sublabel: string;
  description: string;
  higher_is_better: boolean;
  format: 'integer' | 'decimal1' | 'percent' | 'hours';
};

/**
 * IC-level KPI definitions.
 *
 * `raw_field` maps to the column name in RawIcAggregateRow.
 * The transform layer reads the value from that field and formats it
 * according to `format`.
 */
export const IC_KPI_DEFS: IcKpiDef[] = [
  { metric_key: 'bugs_fixed',     raw_field: 'bugs_fixed',       label: 'Bugs Fixed',    unit: '',  sublabel: 'Jira',                 description: 'Bug-type Jira issues closed in the selected period. Reflects quality contribution and team reliability.',                                          higher_is_better: true,  format: 'integer'  },
  { metric_key: 'clean_loc',      raw_field: 'loc',              label: 'Clean LOC',     unit: '',  sublabel: 'Bitbucket',            description: 'Authored lines of code excluding AI-generated and config/spec lines. Reflects hands-on coding output.',                                          higher_is_better: true,  format: 'integer'  },
  { metric_key: 'ai_loc_share',   raw_field: 'ai_loc_share_pct', label: 'AI LOC Share',  unit: '%', sublabel: 'Cursor + Claude Code', description: 'Share of authored lines accepted from AI suggestions (Cursor + Claude Code). Reflects how much AI tooling contributes to actual output.',       higher_is_better: true,  format: 'percent'  },
  { metric_key: 'focus_time_pct', raw_field: 'focus_time_pct',   label: 'Focus Time',    unit: '%', sublabel: 'Calendar / M365',      description: 'Share of work time spent in uninterrupted 60-minute+ blocks. Higher means fewer context switches and more deep work.',                          higher_is_better: true,  format: 'percent'  },
  { metric_key: 'tasks_closed',   raw_field: 'tasks_closed',     label: 'Tasks Closed',  unit: '',  sublabel: 'Jira',                 description: 'Jira tasks moved to Done in the selected period. Direct measure of delivery throughput.',                                                      higher_is_better: true,  format: 'integer'  },
];

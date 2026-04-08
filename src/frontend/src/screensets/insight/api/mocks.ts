/**
 * insight API Mocks
 * Mock data for development and testing
 *
 * Keys use full URL patterns (including baseURL path).
 * Register mocks via MockPlugin in main.tsx or screenset.
 */

import { toLower, slice } from 'lodash';
import type { MockMap } from '@hai3/react';
import type {
  DashboardData,
  SpeedData,
  ExecViewData,
  ExecTeamRow,
  TeamViewData,
  TeamMember,
  TeamKpi,
  BulletMetric,
  BulletSection,
  IcDashboardData,
  IcKpi,
  LocDataPoint,
  DeliveryDataPoint,
  DrillData,
  DrillRow,
  PeriodValue,
} from '../types';

const mockDashboardData: DashboardData = {
  stats: [
    { key: 'total_commits', value: '4.2K', sub: '825.5K lines added' },
    { key: 'lines_of_code', value: '1.2M', sub: '+928.5K - 313.8K' },
    { key: 'ai_involvement', value: '86%', sub: 'ai_involvement_sub' },
    { key: 'active_total', value: '402 / 582', sub: 'active_total_sub' },
  ],
  chartData: [
    { date: '03-19', aiLoc: 170, commitLoc: 300 },
    { date: '03-20', aiLoc: 150, commitLoc: 155 },
    { date: '03-21', aiLoc: 155, commitLoc: 160 },
    { date: '03-22', aiLoc: 165, commitLoc: 200 },
    { date: '03-23', aiLoc: 280, commitLoc: 290 },
    { date: '03-24', aiLoc: 200, commitLoc: 270 },
    { date: '03-25', aiLoc: 150, commitLoc: 175 },
    { date: '03-26', aiLoc: 140, commitLoc: 160 },
    { date: '03-27', aiLoc: 130, commitLoc: 150 },
    { date: '03-28', aiLoc: 120, commitLoc: 140 },
    { date: '03-29', aiLoc: 110, commitLoc: 130 },
    { date: '03-30', aiLoc: 100, commitLoc: 120 },
    { date: '03-31', aiLoc: 130, commitLoc: 380 },
  ],
  bottomMetrics: [
    { key: 'avg_ai_suggested', value: '235' },
    { key: 'avg_ai_accepted', value: '261' },
    { key: 'avg_ai_deleted', value: '31' },
    { key: 'avg_ai_nloc', value: '261' },
    { key: 'avg_ai_loc', value: '273' },
    { key: 'avg_commit_added', value: '96' },
    { key: 'avg_commit_deleted', value: '22' },
    { key: 'avg_commit_nloc', value: '108' },
    { key: 'avg_commit_loc', value: '137' },
  ],
  periodSplits: [
    { label: '2026-03-19 → 2026-03-25', value: 156 },
    { label: '2026-03-26 → 2026-03-31', value: 123 },
  ],
  periodDelta: -33,
};


const mockSpeedData: SpeedData = {
  value: 42,
  min: 0,
  max: 70,
  unit: 'km/h',
  label: 'current_speed',
};

// ---------------------------------------------------------------------------
// Executive View Mock
// ---------------------------------------------------------------------------

// Base month teams
const EXEC_TEAMS_MONTH: ExecTeamRow[] = [
  { team_id: 'platform', team_name: 'Platform', headcount: 12, tasks_closed: 48, bugs_fixed: 18, build_success_pct: 94, focus_time_pct: 72, ai_adoption_pct: 68, ai_loc_share_pct: 22, pr_cycle_time_h: 18, status: 'good' },
  { team_id: 'mobile', team_name: 'Mobile', headcount: 8, tasks_closed: 29, bugs_fixed: 11, build_success_pct: 87, focus_time_pct: 54, ai_adoption_pct: 42, ai_loc_share_pct: 12, pr_cycle_time_h: 26, status: 'warn' },
  { team_id: 'data', team_name: 'Data', headcount: 6, tasks_closed: 22, bugs_fixed: 8, build_success_pct: 91, focus_time_pct: 65, ai_adoption_pct: 71, ai_loc_share_pct: 28, pr_cycle_time_h: 20, status: 'good' },
  { team_id: 'frontend', team_name: 'Frontend', headcount: 10, tasks_closed: 38, bugs_fixed: 14, build_success_pct: 82, focus_time_pct: 48, ai_adoption_pct: 35, ai_loc_share_pct: 9, pr_cycle_time_h: 31, status: 'bad' },
  { team_id: 'devops', team_name: 'DevOps', headcount: 5, tasks_closed: 19, bugs_fixed: 7, build_success_pct: 96, focus_time_pct: 78, ai_adoption_pct: 80, ai_loc_share_pct: 31, pr_cycle_time_h: 14, status: 'good' },
  { team_id: 'qa', team_name: 'QA', headcount: 7, tasks_closed: 25, bugs_fixed: 21, build_success_pct: 88, focus_time_pct: 61, ai_adoption_pct: 55, ai_loc_share_pct: 18, pr_cycle_time_h: 22, status: 'warn' },
];

type RateDeltas = { build: number; focus: number; ai: number; aiLoc: number; cycle: number };

function scaleExecTeams(teams: ExecTeamRow[], factor: number, rates?: RateDeltas): ExecTeamRow[] {
  return teams.map((t) => ({
    ...t,
    tasks_closed: Math.round(t.tasks_closed * factor),
    bugs_fixed: Math.round(t.bugs_fixed * factor),
    build_success_pct: rates ? Math.min(100, Math.max(0, t.build_success_pct + rates.build)) : t.build_success_pct,
    focus_time_pct:    rates ? Math.min(100, Math.max(0, t.focus_time_pct    + rates.focus)) : t.focus_time_pct,
    ai_adoption_pct:   rates ? Math.min(100, Math.max(0, t.ai_adoption_pct   + rates.ai))   : t.ai_adoption_pct,
    ai_loc_share_pct:  rates ? Math.min(100, Math.max(0, t.ai_loc_share_pct  + rates.aiLoc)): t.ai_loc_share_pct,
    pr_cycle_time_h:   rates ? Math.max(1,               t.pr_cycle_time_h   + rates.cycle)  : t.pr_cycle_time_h,
  }));
}

export const EXEC_VIEW_MOCK: Record<PeriodValue, ExecViewData> = {
  week: {
    teams: scaleExecTeams(EXEC_TEAMS_MONTH, 0.25, { build: +2, focus: -3, ai: -5, aiLoc: -4, cycle: -3 }),
    orgKpis: { avgBuildSuccess: 93, avgAiAdoption: 55, avgFocus: 61, bugResolutionScore: 80, prCycleScore: 68 },
  },
  month: {
    teams: EXEC_TEAMS_MONTH,
    orgKpis: { avgBuildSuccess: 90, avgAiAdoption: 59, avgFocus: 63, bugResolutionScore: 78, prCycleScore: 65 },
  },
  quarter: {
    teams: scaleExecTeams(EXEC_TEAMS_MONTH, 3, { build: -2, focus: +2, ai: +4, aiLoc: +2, cycle: +2 }),
    orgKpis: { avgBuildSuccess: 88, avgAiAdoption: 62, avgFocus: 64, bugResolutionScore: 76, prCycleScore: 63 },
  },
  year: {
    teams: scaleExecTeams(EXEC_TEAMS_MONTH, 12, { build: -3, focus: +3, ai: -2, aiLoc: -3, cycle: +4 }),
    orgKpis: { avgBuildSuccess: 87, avgAiAdoption: 57, avgFocus: 62, bugResolutionScore: 75, prCycleScore: 62 },
  },
};

// ---------------------------------------------------------------------------
// Team View Mock
// ---------------------------------------------------------------------------

export const TEAM_MEMBERS_MONTH: TeamMember[] = [
  { person_id: 'p1',  period: 'month', name: 'Alice Kim',      seniority: 'Senior', tasks_closed: 12, bugs_fixed: 5, dev_time_h: 14, prs_merged: 11, build_success_pct: 97, focus_time_pct: 72, ai_tools: ['Cursor', 'Claude Code'], ai_loc_share_pct: 27 },
  { person_id: 'p2',  period: 'month', name: 'Ben Clarke',     seniority: 'Middle', tasks_closed: 2,  bugs_fixed: 1, dev_time_h: 31, prs_merged: 2,  build_success_pct: 78, focus_time_pct: 48, ai_tools: [],                         ai_loc_share_pct: 0  },
  { person_id: 'p3',  period: 'month', name: 'Tom Sullivan',   seniority: 'Middle', tasks_closed: 3,  bugs_fixed: 2, dev_time_h: 26, prs_merged: 3,  build_success_pct: 82, focus_time_pct: 55, ai_tools: ['Codex'],                   ai_loc_share_pct: 8  },
  { person_id: 'p4',  period: 'month', name: 'Sara Jansen',    seniority: 'Junior', tasks_closed: 5,  bugs_fixed: 3, dev_time_h: 18, prs_merged: 4,  build_success_pct: 85, focus_time_pct: 62, ai_tools: ['Cursor'],                  ai_loc_share_pct: 12 },
  { person_id: 'p5',  period: 'month', name: 'Mike Chen',      seniority: 'Senior', tasks_closed: 10, bugs_fixed: 4, dev_time_h: 12, prs_merged: 9,  build_success_pct: 94, focus_time_pct: 68, ai_tools: ['Cursor', 'Claude Code'], ai_loc_share_pct: 31 },
  { person_id: 'p6',  period: 'month', name: 'Emma Davis',     seniority: 'Lead',   tasks_closed: 8,  bugs_fixed: 2, dev_time_h: 10, prs_merged: 7,  build_success_pct: 96, focus_time_pct: 81, ai_tools: ['Claude Code'],            ai_loc_share_pct: 22 },
  { person_id: 'p7',  period: 'month', name: 'Carlos Ruiz',    seniority: 'Middle', tasks_closed: 6,  bugs_fixed: 3, dev_time_h: 16, prs_merged: 5,  build_success_pct: 88, focus_time_pct: 59, ai_tools: ['Cursor'],                  ai_loc_share_pct: 14 },
  { person_id: 'p8',  period: 'month', name: 'Priya Patel',    seniority: 'Senior', tasks_closed: 9,  bugs_fixed: 4, dev_time_h: 13, prs_merged: 8,  build_success_pct: 92, focus_time_pct: 74, ai_tools: ['Cursor', 'Codex'],         ai_loc_share_pct: 19 },
  { person_id: 'p9',  period: 'month', name: 'James Wilson',   seniority: 'Junior', tasks_closed: 4,  bugs_fixed: 2, dev_time_h: 20, prs_merged: 3,  build_success_pct: 80, focus_time_pct: 42, ai_tools: [],                         ai_loc_share_pct: 0  },
  { person_id: 'p10', period: 'month', name: 'Ana Kovac',      seniority: 'Middle', tasks_closed: 7,  bugs_fixed: 3, dev_time_h: 15, prs_merged: 6,  build_success_pct: 89, focus_time_pct: 65, ai_tools: ['Cursor'],                  ai_loc_share_pct: 16 },
  { person_id: 'p11', period: 'month', name: 'Leo Zhang',      seniority: 'Senior', tasks_closed: 11, bugs_fixed: 6, dev_time_h: 11, prs_merged: 10, build_success_pct: 95, focus_time_pct: 76, ai_tools: ['Claude Code', 'Codex'],   ai_loc_share_pct: 34 },
  { person_id: 'p12', period: 'month', name: 'Nina Ross',      seniority: 'Junior', tasks_closed: 3,  bugs_fixed: 1, dev_time_h: 22, prs_merged: 2,  build_success_pct: 83, focus_time_pct: 53, ai_tools: ['Cursor'],                  ai_loc_share_pct: 10 },
];

function scaleTeamMembers(members: TeamMember[], period: PeriodValue, factor: number): TeamMember[] {
  return members.map((m) => ({
    ...m,
    period,
    tasks_closed: Math.round(m.tasks_closed * factor),
    bugs_fixed: Math.round(m.bugs_fixed * factor),
    prs_merged: Math.round(m.prs_merged * factor),
    // dev_time_h, build_success_pct, focus_time_pct, ai_loc_share_pct are rates — stay the same
  }));
}

const BULLET_SECTIONS_MONTH: BulletSection[] = [
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
        sublabel: 'Jira · actual ÷ estimated · lower = better', value: '1.3', unit: '×',
        range_min: '1×', range_max: '3×', median: '1.5', median_label: 'Company median: 1.5×',
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

const TEAM_KPIS_BY_PERIOD: Record<PeriodValue, TeamKpi[]> = {
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

// Rate-metric deltas per period (applied to % and rate bullet metrics)
const BULLET_RATE_OVERRIDES: Record<PeriodValue, Record<string, string>> = {
  week:    { build_success: '99%', focus_time_pct: '72%', ai_loc_share_pct: '31%', ai_loc_share2: '31%', due_date_compliance: '84%', task_reopen_rate: '2%' },
  month:   {},  // baseline — use original values
  quarter: { build_success: '95%', focus_time_pct: '65%', ai_loc_share_pct: '28%', ai_loc_share2: '28%', due_date_compliance: '76%', task_reopen_rate: '5%' },
  year:    { build_success: '93%', focus_time_pct: '63%', ai_loc_share_pct: '24%', ai_loc_share2: '24%', due_date_compliance: '74%', task_reopen_rate: '6%' },
};

function applyRateOverrides(sections: BulletSection[], period: PeriodValue): BulletSection[] {
  const overrides = BULLET_RATE_OVERRIDES[period];
  if (Object.keys(overrides).length === 0) return sections;
  return sections.map((s) => ({
    ...s,
    metrics: s.metrics.map((m) =>
      overrides[m.metric_key] !== undefined ? { ...m, period, value: overrides[m.metric_key] as string } : { ...m, period },
    ),
  }));
}

export const TEAM_VIEW_MOCK: Record<PeriodValue, TeamViewData> = {
  week:    { teamName: 'Platform Engineering', teamKpis: TEAM_KPIS_BY_PERIOD.week,    members: scaleTeamMembers(TEAM_MEMBERS_MONTH, 'week', 0.25),   bulletSections: applyRateOverrides(BULLET_SECTIONS_MONTH, 'week') },
  month:   { teamName: 'Platform Engineering', teamKpis: TEAM_KPIS_BY_PERIOD.month,   members: TEAM_MEMBERS_MONTH,                                   bulletSections: BULLET_SECTIONS_MONTH },
  quarter: { teamName: 'Platform Engineering', teamKpis: TEAM_KPIS_BY_PERIOD.quarter, members: scaleTeamMembers(TEAM_MEMBERS_MONTH, 'quarter', 3),   bulletSections: applyRateOverrides(BULLET_SECTIONS_MONTH, 'quarter') },
  year:    { teamName: 'Platform Engineering', teamKpis: TEAM_KPIS_BY_PERIOD.year,    members: scaleTeamMembers(TEAM_MEMBERS_MONTH, 'year', 12),     bulletSections: applyRateOverrides(BULLET_SECTIONS_MONTH, 'year') },
};

// ---------------------------------------------------------------------------
// IC Dashboard Mock
// ---------------------------------------------------------------------------

const ALICE_DRILLS: Record<string, DrillData> = {
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
      { PR: 'PR #159', Repository: 'insight-api',    Status: 'Open',   'Cycle Time': '—'   },
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

const ALICE_BULLET_METRICS_MONTH: BulletMetric[] = [
  // task_delivery
  { period: 'month', section: 'task_delivery', metric_key: 'tasks_completed', label: 'Tasks Completed', sublabel: 'Jira · closed issues in sprint', value: '12', unit: 'count', range_min: '2', range_max: '12', median: '7', median_label: 'Median: 7', bar_left_pct: 0, bar_width_pct: 100, median_left_pct: 50, status: 'good', drill_id: 'tasks-completed' },
  { period: 'month', section: 'task_delivery', metric_key: 'task_dev_time', label: 'Task Development Time', sublabel: 'Jira · time in In Progress state · lower = better', value: '14', unit: 'h', range_min: '8h', range_max: '31h', median: '15', median_label: 'Median: 15h', bar_left_pct: 0, bar_width_pct: 43, median_left_pct: 48, status: 'good', drill_id: 'cycle-time' },
  { period: 'month', section: 'task_delivery', metric_key: 'estimation_accuracy', label: 'Estimation Accuracy', sublabel: 'Jira · tasks within ±20% of original estimate', value: '1.1', unit: '×', range_min: '0×', range_max: '3×', median: '', median_label: 'Target 0.9–1.3×', bar_left_pct: 0, bar_width_pct: 22, median_left_pct: 33, status: 'good', drill_id: '' },
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
  { period: 'month', section: 'code_quality', metric_key: 'build_success', label: 'Build Success Rate', sublabel: 'CI · passed ÷ total runs · target ≥90%', value: '94', unit: '%', range_min: '0%', range_max: '100%', median: '87', median_label: 'Target ≥90% · Median: 87%', bar_left_pct: 0, bar_width_pct: 94, median_left_pct: 83, status: 'good', drill_id: 'builds' },
  { period: 'month', section: 'code_quality', metric_key: 'pr_cycle_time', label: 'Pull Request Cycle Time', sublabel: 'Bitbucket · PR opened → merged · lower = better', value: '18', unit: 'h', range_min: '0h', range_max: '72h', median: '24', median_label: 'Median: 24h', bar_left_pct: 0, bar_width_pct: 36, median_left_pct: 35, status: 'good', drill_id: 'pull-requests' },
  { period: 'month', section: 'code_quality', metric_key: 'pickup_time', label: 'Pickup Time', sublabel: 'Bitbucket · PR opened → first review · lower = better', value: '4.2', unit: 'h', range_min: '0h', range_max: '48h', median: '', median_label: 'Target <24h', bar_left_pct: 0, bar_width_pct: 17, median_left_pct: 24, status: 'good', drill_id: '' },
  { period: 'month', section: 'code_quality', metric_key: 'bugs_fixed', label: 'Bugs Fixed', sublabel: 'Jira · bug-type issues closed', value: '23', unit: 'count', range_min: '0', range_max: '30', median: '9', median_label: 'Median: 9', bar_left_pct: 0, bar_width_pct: 77, median_left_pct: 30, status: 'good', drill_id: 'bugs-fixed' },
  { period: 'month', section: 'code_quality', metric_key: 'bug_reopen_rate', label: 'Bug Reopen Rate', sublabel: 'Jira · bugs reopened · lower = better', value: '9', unit: '%', range_min: '0%', range_max: '30%', median: '14', median_label: 'Median: 14% · Target <15%', bar_left_pct: 0, bar_width_pct: 30, median_left_pct: 47, status: 'good', drill_id: '' },
  // ai_tools
  { period: 'month', section: 'ai_tools', metric_key: 'cursor_completions', label: 'Cursor Completions', sublabel: 'Cursor · completions suggested this month', value: '1.2k', unit: 'count', range_min: '200', range_max: '5k', median: '800', median_label: 'Median: 800', bar_left_pct: 0, bar_width_pct: 24, median_left_pct: 16, status: 'good', drill_id: '' },
  { period: 'month', section: 'ai_tools', metric_key: 'cursor_agents', label: 'Cursor Agent Sessions', sublabel: 'Cursor · agentic sessions started', value: '18', unit: 'count', range_min: '2', range_max: '40', median: '10', median_label: 'Median: 10', bar_left_pct: 0, bar_width_pct: 45, median_left_pct: 25, status: 'good', drill_id: '' },
  { period: 'month', section: 'ai_tools', metric_key: 'cursor_lines', label: 'Lines Accepted', sublabel: 'Cursor · lines of code accepted', value: '3.2k', unit: 'count', range_min: '0', range_max: '5k', median: '1.8k', median_label: 'Median: 1.8k', bar_left_pct: 0, bar_width_pct: 64, median_left_pct: 36, status: 'good', drill_id: '' },
  { period: 'month', section: 'ai_tools', metric_key: 'cc_sessions', label: 'Claude Code Sessions', sublabel: 'Anthropic Enterprise API · sessions this month', value: '24', unit: 'count', range_min: '0', range_max: '60', median: '12', median_label: 'Median: 12', bar_left_pct: 0, bar_width_pct: 40, median_left_pct: 20, status: 'good', drill_id: '' },
  { period: 'month', section: 'ai_tools', metric_key: 'cc_tool_accept', label: 'Tool Acceptance Rate', sublabel: 'Anthropic Enterprise API · accepted ÷ offered', value: '76', unit: '%', range_min: '0%', range_max: '100%', median: '64', median_label: 'Median: 64%', bar_left_pct: 0, bar_width_pct: 76, median_left_pct: 64, status: 'good', drill_id: '' },
  { period: 'month', section: 'ai_tools', metric_key: 'cc_lines', label: 'Lines Added (Claude Code)', sublabel: 'Anthropic Enterprise API · lines added by Claude Code', value: '1.1k', unit: 'count', range_min: '0', range_max: '3k', median: '600', median_label: 'Median: 600', bar_left_pct: 0, bar_width_pct: 37, median_left_pct: 20, status: 'good', drill_id: '' },
  { period: 'month', section: 'ai_tools', metric_key: 'ai_loc_share2', label: 'AI LOC Share', sublabel: 'Cursor + Claude Code · accepted lines ÷ clean LOC', value: '27', unit: '%', range_min: '0%', range_max: '34%', median: '18', median_label: 'Median: 18%', bar_left_pct: 0, bar_width_pct: 79, median_left_pct: 53, status: 'good', drill_id: '' },
  { period: 'month', section: 'ai_tools', metric_key: 'claude_web', label: 'Claude Web Usage', sublabel: 'Claude Web · conversations this month', value: '32', unit: 'count', range_min: '0', range_max: '80', median: '18', median_label: 'Median: 18', bar_left_pct: 0, bar_width_pct: 40, median_left_pct: 23, status: 'good', drill_id: '' },
  { period: 'month', section: 'ai_tools', metric_key: 'chatgpt', label: 'ChatGPT Usage', sublabel: 'ChatGPT · conversations this month', value: '8', unit: 'count', range_min: '0', range_max: '40', median: '12', median_label: 'Median: 12', bar_left_pct: 0, bar_width_pct: 20, median_left_pct: 30, status: 'good', drill_id: '' },
  // collab
  { period: 'month', section: 'collab', metric_key: 'slack_thread_participation', label: 'Thread Participation', sublabel: 'Slack · replies to others\' threads', value: '34', unit: 'replies', range_min: '0', range_max: '80', median: '29', median_label: 'Median: 29', bar_left_pct: 0, bar_width_pct: 43, median_left_pct: 36, status: 'good', drill_id: '' },
  { period: 'month', section: 'collab', metric_key: 'slack_message_engagement', label: 'Message Engagement', sublabel: 'Slack · avg replies per thread', value: '2.1', unit: 'avg', range_min: '0', range_max: '5', median: '1.8', median_label: 'Median: 1.8', bar_left_pct: 0, bar_width_pct: 42, median_left_pct: 36, status: 'good', drill_id: '' },
  { period: 'month', section: 'collab', metric_key: 'slack_dm_ratio', label: 'DM Ratio', sublabel: 'Slack · DMs ÷ all messages · lower = more open', value: '24', unit: '%', range_min: '0%', range_max: '100%', median: '28', median_label: 'Median: 28% · lower = more open', bar_left_pct: 0, bar_width_pct: 24, median_left_pct: 28, status: 'good', drill_id: '' },
  { period: 'month', section: 'collab', metric_key: 'm365_teams_messages', label: 'Teams Messages', sublabel: 'Microsoft Teams · all channels sent', value: '168', unit: '/mo', range_min: '0', range_max: '400', median: '148', median_label: 'Median: 148', bar_left_pct: 0, bar_width_pct: 42, median_left_pct: 37, status: 'good', drill_id: '' },
  { period: 'month', section: 'collab', metric_key: 'm365_emails_sent', label: 'Emails Sent', sublabel: 'M365 · avg per member · lower = better', value: '31', unit: '/mo', range_min: '0', range_max: '120', median: '35', median_label: 'Median: 35 · lower = better', bar_left_pct: 0, bar_width_pct: 26, median_left_pct: 29, status: 'good', drill_id: '' },
  { period: 'month', section: 'collab', metric_key: 'm365_files_shared', label: 'Files Shared', sublabel: 'M365 · avg per member', value: '9', unit: '/mo', range_min: '0', range_max: '30', median: '8', median_label: 'Median: 8', bar_left_pct: 0, bar_width_pct: 30, median_left_pct: 27, status: 'good', drill_id: '' },
  { period: 'month', section: 'collab', metric_key: 'focus_time_pct', label: 'Focus Time', sublabel: 'Calendar / M365 · uninterrupted blocks ≥60 min', value: '68', unit: '%', range_min: '42%', range_max: '81%', median: '63', median_label: 'Median: 63%', bar_left_pct: 0, bar_width_pct: 67, median_left_pct: 54, status: 'good', drill_id: '' },
  { period: 'month', section: 'collab', metric_key: 'meeting_hours', label: 'Meeting Hours', sublabel: 'Zoom · meeting duration + M365 audioDuration · lower = more focus time', value: '12', unit: 'h/mo', range_min: '4h', range_max: '28h', median: '16', median_label: 'Median: 16h', bar_left_pct: 0, bar_width_pct: 33, median_left_pct: 50, status: 'good', drill_id: '' },
  { period: 'month', section: 'collab', metric_key: 'zoom_calls', label: 'Zoom Calls', sublabel: 'Zoom API · avg calls attended per member', value: '11', unit: '/mo', range_min: '0', range_max: '20', median: '9', median_label: 'Median: 9', bar_left_pct: 0, bar_width_pct: 55, median_left_pct: 45, status: 'good', drill_id: '' },
  { period: 'month', section: 'collab', metric_key: 'meeting_free', label: 'Meeting-Free Days', sublabel: 'Zoom · days with no meetings + M365 · avg · higher = better', value: '5', unit: 'days', range_min: '0', range_max: '10', median: '4', median_label: 'Median: 4', bar_left_pct: 0, bar_width_pct: 50, median_left_pct: 40, status: 'good', drill_id: '' },
];

const ALICE_KPIS_MONTH: IcKpi[] = [
  { period: 'month', metric_key: 'bugs_fixed', label: 'Bugs Fixed', value: '23', unit: '', sublabel: 'Jira', delta: '↑ 5 vs last month', delta_type: 'good' },
  { period: 'month', metric_key: 'clean_loc', label: 'Clean LOC', value: '12k', unit: '', sublabel: 'Bitbucket', delta: '↑ 8% vs median 19%', delta_type: 'good' },
  { period: 'month', metric_key: 'ai_loc_share', label: 'AI LOC Share', value: '27', unit: '%', sublabel: 'Cursor + Claude Code', delta: '≈ team avg 22%', delta_type: 'neutral' },
  { period: 'month', metric_key: 'spec_lines', label: 'Spec Lines', value: '840', unit: '', sublabel: 'Jira', delta: '↑ 12% vs last month', delta_type: 'good' },
  { period: 'month', metric_key: 'focus_time_pct', label: 'Focus Time', value: '68', unit: '%', sublabel: 'Calendar / M365', delta: '↑ 5% vs last month', delta_type: 'good' },
];

const ALICE_CHARTS_MONTH: { locTrend: LocDataPoint[]; deliveryTrend: DeliveryDataPoint[] } = {
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

const IC_BULLET_RATE_OVERRIDES: Record<PeriodValue, Record<string, string>> = {
  week:    { build_success: '99%', focus_time_pct: '72%', ai_loc_share2: '31%', due_date_compliance: '95%', task_reopen_rate: '2%', cc_tool_accept: '80%', rework_ratio: '8%', pr_cycle_time: '14', pickup_time: '3.1', bug_reopen_rate: '6%' },
  month:   {},
  quarter: { build_success: '95%', focus_time_pct: '65%', ai_loc_share2: '28%', due_date_compliance: '87%', task_reopen_rate: '5%', cc_tool_accept: '74%', rework_ratio: '14%', pr_cycle_time: '20', pickup_time: '5.4', bug_reopen_rate: '11%' },
  year:    { build_success: '93%', focus_time_pct: '63%', ai_loc_share2: '24%', due_date_compliance: '83%', task_reopen_rate: '6%', cc_tool_accept: '71%', rework_ratio: '16%', pr_cycle_time: '22', pickup_time: '6.0', bug_reopen_rate: '13%' },
};

function scaleAliceBullets(metrics: BulletMetric[], period: PeriodValue): BulletMetric[] {
  const overrides = IC_BULLET_RATE_OVERRIDES[period];
  return metrics.map((m) =>
    overrides[m.metric_key] !== undefined
      ? { ...m, period, value: overrides[m.metric_key] as string }
      : { ...m, period },
  );
}

export const IC_DASHBOARD_MOCK: Record<string, Record<PeriodValue, IcDashboardData>> = {
  p1: {
    week: {
      person: { person_id: 'p1', name: 'Alice Kim', role: 'Senior Backend Developer', seniority: 'Senior' },
      kpis: scaleAliceKpis(ALICE_KPIS_MONTH, 'week', 0.25),
      bulletMetrics: scaleAliceBullets(ALICE_BULLET_METRICS_MONTH, 'week'),
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
      timeOffNotice: { days: 5, dateRange: 'Jul 14–18', bambooHrUrl: '#bamboo' },
      drills: ALICE_DRILLS,
    },
    quarter: {
      person: { person_id: 'p1', name: 'Alice Kim', role: 'Senior Backend Developer', seniority: 'Senior' },
      kpis: scaleAliceKpis(ALICE_KPIS_MONTH, 'quarter', 3),
      bulletMetrics: scaleAliceBullets(ALICE_BULLET_METRICS_MONTH, 'quarter'),
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
      timeOffNotice: { days: 5, dateRange: 'Jul 14–18', bambooHrUrl: '#bamboo' },
      drills: ALICE_DRILLS,
    },
    year: {
      person: { person_id: 'p1', name: 'Alice Kim', role: 'Senior Backend Developer', seniority: 'Senior' },
      kpis: scaleAliceKpis(ALICE_KPIS_MONTH, 'year', 12),
      bulletMetrics: scaleAliceBullets(ALICE_BULLET_METRICS_MONTH, 'year'),
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
      timeOffNotice: { days: 5, dateRange: 'Jul 14–18', bambooHrUrl: '#bamboo' },
      drills: ALICE_DRILLS,
    },
  },
};

// ---------------------------------------------------------------------------
// IC Dashboard Generator
// Derives IcDashboardData from a TeamMember baseline (month values).
// All other periods are scaled by PERIOD_FACTOR.
// ---------------------------------------------------------------------------

const PERIOD_FACTOR: Record<PeriodValue, number> = { week: 0.25, month: 1, quarter: 3, year: 12 };

function memberRole(seniority: string): string {
  if (seniority === 'Lead')   return 'Team Lead';
  if (seniority === 'Senior') return 'Senior Software Engineer';
  if (seniority === 'Middle') return 'Software Engineer';
  return 'Junior Software Engineer';
}

function barPct(value: number, min: number, max: number): number {
  return Math.max(0, Math.min(100, Math.round(((value - min) / (max - min)) * 100)));
}

function bm(
  period: PeriodValue, section: string, metric_key: string,
  label: string, sublabel: string,
  value: string, unit: string,
  range_min: string, range_max: string,
  median: string, median_label: string,
  bar_width_pct: number, median_left_pct: number,
  status: 'good' | 'warn' | 'bad', drill_id: string,
): BulletMetric {
  return {
    period, section, metric_key, label, sublabel, value, unit,
    range_min, range_max, median, median_label,
    bar_left_pct: 0, bar_width_pct, median_left_pct, status, drill_id,
  };
}

function generateIcDashboard(baseMember: TeamMember, period: PeriodValue): IcDashboardData {
  const f   = PERIOD_FACTOR[period];
  const m   = baseMember;
  const pid = parseInt(m.person_id.replace('p', ''), 10);

  // Count metrics — scaled by period
  const tasks   = Math.max(0, Math.round(m.tasks_closed * f));
  const bugs    = Math.max(0, Math.round(m.bugs_fixed * f));
  const prs     = Math.max(0, Math.round(m.prs_merged * f));
  const commits = Math.max(0, Math.round(m.prs_merged * 3.5 * f));
  const reviews = Math.max(0, Math.round(prs * 1.5));

  // Rate metrics — constant across periods
  const build  = m.build_success_pct;
  const focus  = m.focus_time_pct;
  const aiLoc  = m.ai_loc_share_pct;

  const cleanLocK = Math.max(1, Math.round(prs * 1.2 * 1000));
  const login     = toLower(m.name).replace(' ', '.');
  const role      = memberRole(m.seniority);

  // ── KPIs ──────────────────────────────────────────────────────────────────
  const kpis: IcKpi[] = [
    { period, metric_key: 'tasks_closed',   label: 'Tasks Closed',  value: String(tasks),
      unit: '',  sublabel: 'Jira',                  delta: '',               delta_type: 'neutral' },
    { period, metric_key: 'bugs_fixed',     label: 'Bugs Fixed',    value: String(bugs),
      unit: '',  sublabel: 'Jira',                  delta: '',               delta_type: bugs > 0 ? 'good' : 'neutral' },
    { period, metric_key: 'clean_loc',      label: 'Clean LOC',
      value: cleanLocK >= 1000 ? `${Math.round(cleanLocK / 1000)}k` : String(cleanLocK),
      unit: '',  sublabel: 'Bitbucket',             delta: '',               delta_type: 'neutral' },
    { period, metric_key: 'ai_loc_share',   label: 'AI LOC Share',  value: String(aiLoc),
      unit: '%', sublabel: 'Cursor + Claude Code',  delta: '≈ team avg 22%', delta_type: aiLoc >= 18 ? 'good' : 'neutral' },
    { period, metric_key: 'focus_time_pct', label: 'Focus Time',    value: String(focus),
      unit: '%', sublabel: 'Calendar / M365',       delta: '',               delta_type: focus >= 60 ? 'good' : 'warn' },
  ];

  // ── Bullet metrics ─────────────────────────────────────────────────────────
  const bulletMetrics: BulletMetric[] = [
    // task_delivery
    bm(period, 'task_delivery', 'tasks_completed', 'Tasks Completed', 'Jira · closed issues in sprint',
      String(tasks), 'count', '0', '15', '7', 'Median: 7',
      barPct(tasks, 0, 15), 47, tasks >= 7 ? 'good' : tasks >= 4 ? 'warn' : 'bad', 'tasks-completed'),
    bm(period, 'task_delivery', 'task_dev_time', 'Task Development Time', 'Jira · time in In Progress state · lower = better',
      String(m.dev_time_h), 'h', '8h', '31h', '15', 'Median: 15h',
      barPct(m.dev_time_h, 8, 31), 30, m.dev_time_h <= 15 ? 'good' : m.dev_time_h <= 22 ? 'warn' : 'bad', 'cycle-time'),
    bm(period, 'task_delivery', 'task_reopen_rate', 'Task Reopen Rate', 'Jira · closed then reopened within 14 days · lower = better',
      '4', '%', '0%', '15%', '5', 'Median: 5%', 27, 33, 'good', 'task-reopen'),
    bm(period, 'task_delivery', 'due_date_compliance', 'Due Date Compliance', 'Jira · tasks closed by due date',
      String(Math.min(100, Math.round(60 + build / 4))), '%', '40%', '100%', '72', 'Median: 72%',
      barPct(60 + build / 4, 40, 100), 53, 'good', ''),
    // git_output
    bm(period, 'git_output', 'commits', 'Commits Created', 'Bitbucket · commits authored',
      String(commits), 'count', '2', '55', '22', 'Median: 22',
      barPct(commits, 2, 55), 36, commits >= 22 ? 'good' : commits >= 10 ? 'warn' : 'bad', 'commits'),
    bm(period, 'git_output', 'prs_created', 'Pull Requests Created', 'Bitbucket · PRs authored',
      String(prs), 'count', '0', '14', '6', 'Median: 6',
      barPct(prs, 0, 14), 43, prs >= 6 ? 'good' : prs >= 3 ? 'warn' : 'bad', 'pull-requests'),
    bm(period, 'git_output', 'prs_merged', 'Pull Requests Merged', 'Bitbucket · authored and merged',
      String(Math.max(0, prs - 1)), 'count', '0', '14', '6', 'Median: 6',
      barPct(Math.max(0, prs - 1), 0, 14), 43, 'good', ''),
    // code_quality
    bm(period, 'code_quality', 'reviews_given', 'Reviews Given', 'Bitbucket · PRs reviewed',
      String(reviews), 'count', '0', '20', '8', 'Median: 8',
      barPct(reviews, 0, 20), 40, reviews >= 8 ? 'good' : 'warn', 'reviews'),
    bm(period, 'code_quality', 'build_success', 'Build Success Rate', 'CI · passed ÷ total runs · target ≥90%',
      String(build), '%', '78%', '100%', '87', 'Median: 87%',
      barPct(build, 78, 100), 41, build >= 90 ? 'good' : build >= 80 ? 'warn' : 'bad', 'builds'),
    bm(period, 'code_quality', 'bugs_fixed', 'Bugs Fixed', 'Jira · bug-type issues closed',
      String(bugs), 'count', '0', '30', '9', 'Median: 9',
      barPct(bugs, 0, 30), 30, 'good', 'bugs-fixed'),
    bm(period, 'code_quality', 'pr_cycle_time', 'PR Cycle Time', 'Bitbucket · PR opened → merged · lower = better',
      String(m.dev_time_h), 'h', '0h', '72h', '24', 'Median: 24h',
      barPct(m.dev_time_h, 0, 72), 33, m.dev_time_h <= 18 ? 'good' : m.dev_time_h <= 26 ? 'warn' : 'bad', 'pull-requests'),
    // collab
    bm(period, 'collab', 'focus_time_pct', 'Focus Time', 'Calendar / M365 · uninterrupted blocks ≥60 min',
      String(focus), '%', '42%', '81%', '63', 'Median: 63%',
      barPct(focus, 42, 81), 54, focus >= 60 ? 'good' : focus >= 50 ? 'warn' : 'bad', ''),
    bm(period, 'collab', 'meeting_hours', 'Meeting Hours', 'Zoom · avg · lower = better',
      `${Math.round(40 - focus / 5)}h`, 'h/mo', '4h', '40h', '16', 'Median: 16h',
      barPct(40 - focus / 5, 4, 40), 33, focus >= 60 ? 'good' : 'warn', ''),
    // ai_tools — only if member uses AI
    ...(m.ai_tools.length > 0 ? [
      bm(period, 'ai_tools', 'ai_loc_share2', 'AI LOC Share', 'Cursor + Claude Code · accepted lines ÷ clean LOC',
        `${aiLoc}%`, '%', '0%', '40%', '18', 'Median: 18%',
        barPct(aiLoc, 0, 40), 45, aiLoc >= 18 ? 'good' : aiLoc >= 8 ? 'warn' : 'bad', ''),
    ] : []),
  ];

  // ── Charts ─────────────────────────────────────────────────────────────────
  const locPerWeek = Math.round(cleanLocK / 4);
  const wW = [0.9, 1.1, 0.95, 1.2, 0.75] as const;
  const mW = [0.9, 1.05, 0.95, 1.1]       as const;
  const qW = [0.9, 1.0, 1.1]              as const;
  const yW = [0.85, 0.95, 1.05, 1.15]     as const;

  const locPt = (label: string, w: number, base: number): LocDataPoint => ({
    label,
    aiLoc:     Math.max(0, Math.round(base * aiLoc / 100 * w)),
    codeLoc:   Math.max(0, Math.round(base * w)),
    specLines: Math.max(0, Math.round(base * 0.06 * w)),
  });
  const delivPt = (label: string, w: number): DeliveryDataPoint => ({
    label,
    commits:   Math.max(0, Math.round(commits * w)),
    prsMerged: Math.max(0, Math.round(prs     * w)),
    tasksDone: Math.max(0, Math.round(tasks   * w)),
  });

  const locTrend: LocDataPoint[] =
    period === 'week'    ? (['Mon','Tue','Wed','Thu','Fri'] as const).map((l, i) => locPt(l, wW[i]!, locPerWeek)) :
    period === 'quarter' ? (['Jan','Feb','Mar']             as const).map((l, i) => locPt(l, qW[i]!, cleanLocK)) :
    period === 'year'    ? (['Q1','Q2','Q3','Q4']           as const).map((l, i) => locPt(l, yW[i]!, cleanLocK * 3)) :
    /* month */            (['W1','W2','W3','W4']            as const).map((l, i) => locPt(l, mW[i]!, locPerWeek));

  const deliveryTrend: DeliveryDataPoint[] =
    period === 'week'    ? (['Mon','Tue','Wed','Thu','Fri'] as const).map((l, i) => delivPt(l, wW[i]!)) :
    period === 'quarter' ? (['Jan','Feb','Mar']             as const).map((l, i) => delivPt(l, qW[i]!)) :
    period === 'year'    ? (['Q1','Q2','Q3','Q4']           as const).map((l, i) => delivPt(l, yW[i]!)) :
    /* month */            (['W1','W2','W3','W4']            as const).map((l, i) => delivPt(l, mW[i]!));

  // ── Drills — row counts MUST match value field ─────────────────────────────
  const idn = (base: number, i: number, step: number): string =>
    `INSIGHT-${Math.max(1, base - i * step)}`;
  const date = (total: number, i: number): string =>
    `2026-03-${String(Math.max(1, 31 - Math.round((i * 28) / Math.max(total, 1)))).padStart(2, '0')}`;

  const taskRows: DrillRow[] = Array.from({ length: tasks }, (_, i) => ({
    Task:           idn(300 + pid * 20, i, 7),
    'Story Points': [1, 2, 3, 5, 3, 2, 1, 5][i % 8] ?? 3,
    'Dev Time':     `${m.dev_time_h + ([-2, 0, 2, -4, 4, -2, 0, 2][i % 8] ?? 0)}h`,
    Closed:         date(tasks, i),
  }));
  const bugRows: DrillRow[] = Array.from({ length: bugs }, (_, i) => ({
    Bug:        idn(200 + pid * 15, i, 8),
    Priority:   ['High', 'Medium', 'Low', 'Critical', 'Medium', 'High'][i % 6] ?? 'Medium',
    'Fix Time': `${Math.max(1, Math.round(m.dev_time_h * 0.35) + ([-1, 0, 1, -2, 2, 0][i % 6] ?? 0))}h`,
    Closed:     date(bugs, i),
  }));
  const prRows: DrillRow[] = Array.from({ length: prs }, (_, i) => ({
    PR:           `PR #${500 + pid * 10 - i * 5}`,
    Repository:   ['insight-front', 'insight-api', 'insight-shared'][i % 3] ?? 'insight-front',
    Status:       i === prs - 1 && prs > 1 ? 'Open' : 'Merged',
    'Cycle Time': i === prs - 1 && prs > 1 ? '—' : `${m.dev_time_h + ([-4, -2, 0, 2, 4][i % 5] ?? 0)}h`,
  }));
  const commitRows: DrillRow[] = Array.from({ length: Math.min(commits, 20) }, (_, i) => ({
    Commit:     `${m.person_id}${String(i).padStart(3, '0')}x`,
    Repository: ['insight-front', 'insight-api', 'insight-shared'][i % 3] ?? 'insight-front',
    '+LOC':     Math.round((50 + i * 15) * (1 + aiLoc / 100)),
    '-LOC':     Math.round(10 + i * 4),
    Date:       date(Math.min(commits, 20), i),
  }));
  const reviewRows: DrillRow[] = Array.from({ length: reviews }, (_, i) => ({
    PR:               `PR #${550 + pid * 8 - i * 4}`,
    Author:           ['Alice Kim', 'Ben Clarke', 'Tom Sullivan', 'Sara Jansen', 'Mike Chen', 'Emma Davis'][i % 6] ?? 'Alice Kim',
    Outcome:          i % 3 === 1 ? 'Changes Requested' : 'Approved',
    'Time to Review': `${1 + (i % 6)}h`,
  }));

  const drills: Record<string, DrillData> = {
    'tasks-completed': {
      title: 'Tasks Completed', source: 'Jira', srcClass: 'bg-blue-600',
      value: String(tasks),
      filter: `assignee = ${login} AND status = Done AND updatedDate >= -30d`,
      columns: ['Task', 'Story Points', 'Dev Time', 'Closed'],
      rows: taskRows,
    },
    'cycle-time': {
      title: 'Task Development Time', source: 'Jira', srcClass: 'bg-blue-600',
      value: `${m.dev_time_h}h`,
      filter: `assignee = ${login} AND status changed to "In Progress"`,
      columns: ['Task', 'Story Points', 'Dev Time', 'Status'],
      rows: slice(taskRows, 0, Math.min(5, taskRows.length)).map((r, i) => ({ ...r, Status: i === 0 ? 'In Progress' : 'Done' })),
    },
    'task-reopen': {
      title: 'Reopened Tasks', source: 'Jira', srcClass: 'bg-blue-600',
      value: tasks > 5 ? '1' : '0',
      filter: `assignee = ${login} AND status changed from Done to "In Progress" after -14d`,
      columns: ['Task', 'Reason', 'Reopened', 'Resolved'],
      rows: tasks > 5
        ? [{ Task: idn(270 + pid * 5, 0, 1), Reason: 'QA regression', Reopened: '2026-03-20', Resolved: '2026-03-21' }]
        : [],
    },
    'commits': {
      title: 'Commits', source: 'Bitbucket', srcClass: 'bg-blue-800',
      value: String(commits),
      filter: `author = ${login} AND date >= 2026-03-01${commits > 20 ? ' (showing last 20)' : ''}`,
      columns: ['Commit', 'Repository', '+LOC', '-LOC', 'Date'],
      rows: commitRows,
    },
    'pull-requests': {
      title: 'Pull Requests', source: 'Bitbucket', srcClass: 'bg-blue-800',
      value: String(prs),
      filter: `author = ${login} AND created >= 2026-03-01`,
      columns: ['PR', 'Repository', 'Status', 'Cycle Time'],
      rows: prRows,
    },
    'reviews': {
      title: 'Code Reviews Given', source: 'Bitbucket', srcClass: 'bg-blue-800',
      value: String(reviews),
      filter: `reviewer = ${login} AND reviewed >= 2026-03-01`,
      columns: ['PR', 'Author', 'Outcome', 'Time to Review'],
      rows: reviewRows,
    },
    'builds': {
      title: 'Build Results', source: 'Bitbucket', srcClass: 'bg-blue-800',
      value: `${build}%`,
      filter: `author = ${login} AND build.date >= 2026-03-01`,
      columns: ['Build', 'Branch', 'Status', 'Duration'],
      rows: [
        { Build: `build-${1000 + pid * 10 + 2}`, Branch: 'feat/main-feature', Status: 'Passed',                  Duration: '3m 12s' },
        { Build: `build-${1000 + pid * 10 + 1}`, Branch: 'feat/main-feature', Status: build < 90 ? 'Failed' : 'Passed', Duration: '2m 08s' },
        { Build: `build-${1000 + pid * 10}`,     Branch: 'fix/bug-fix',        Status: 'Passed',                  Duration: '3m 01s' },
      ],
    },
    'bugs-fixed': {
      title: 'Bugs Fixed', source: 'Jira', srcClass: 'bg-blue-600',
      value: String(bugs),
      filter: `assignee = ${login} AND issuetype = Bug AND status = Done AND resolved >= 2026-03-01`,
      columns: ['Bug', 'Priority', 'Fix Time', 'Closed'],
      rows: bugRows,
    },
  };

  return {
    person: { person_id: m.person_id, name: m.name, role, seniority: m.seniority },
    kpis,
    bulletMetrics,
    charts: { locTrend, deliveryTrend },
    timeOffNotice: null,
    drills,
  };
}

// Pre-compute all IC mocks (12 members × 4 periods).
// Alice (p1) uses her detailed hand-crafted data; others are generated.
const ALL_IC_PERIODS: PeriodValue[] = ['week', 'month', 'quarter', 'year'];
const ALL_IC: Record<string, IcDashboardData> = {};
for (const member of TEAM_MEMBERS_MONTH) {
  for (const p of ALL_IC_PERIODS) {
    const key = `${member.person_id}/${p}`;
    ALL_IC[key] = member.person_id === 'p1' && IC_DASHBOARD_MOCK.p1?.[p]
      ? IC_DASHBOARD_MOCK.p1[p]!
      : generateIcDashboard(member, p);
  }
}

// Flat mock-map entries (no wildcards — framework cannot read URL params).
const IC_MOCK_ENTRIES: Record<string, () => IcDashboardData | null> = {};
const IC_DRILL_ENTRIES: Record<string, () => DrillData | null> = {};
for (const member of TEAM_MEMBERS_MONTH) {
  for (const p of ALL_IC_PERIODS) {
    const data = ALL_IC[`${member.person_id}/${p}`];
    IC_MOCK_ENTRIES[`GET /api/insight/ic-dashboard/${member.person_id}/${p}`] = () => data ?? null;
  }
  // Drills are period-agnostic — use month data as source of truth
  const monthData = ALL_IC[`${member.person_id}/month`];
  if (monthData) {
    for (const drillId of Object.keys(monthData.drills)) {
      const drill = monthData.drills[drillId];
      IC_DRILL_ENTRIES[`GET /api/insight/ic-dashboard/${member.person_id}/drill/${drillId}`] =
        () => drill ?? null;
    }
  }
}

// ---------------------------------------------------------------------------
// Team View Drills
// ---------------------------------------------------------------------------

const TEAM_DRILLS: Record<string, DrillData> = {
  'team-members': {
    title: 'Team Members Overview',
    source: 'Jira + Bitbucket',
    srcClass: 'bg-blue-700',
    value: '12',
    filter: 'team = backend-dev AND period = current_month',
    columns: ['Name', 'Seniority', 'Tasks', 'Dev Time', 'PRs', 'Build %', 'Focus %', 'AI LOC %'],
    rows: [
      { Name: 'Alice Kim',    Seniority: 'Senior', Tasks: 12, 'Dev Time': '14h', PRs: 11, 'Build %': '97%', 'Focus %': '72%', 'AI LOC %': '27%' },
      { Name: 'Mike Chen',    Seniority: 'Senior', Tasks: 10, 'Dev Time': '12h', PRs: 9,  'Build %': '94%', 'Focus %': '68%', 'AI LOC %': '31%' },
      { Name: 'Leo Zhang',    Seniority: 'Senior', Tasks: 11, 'Dev Time': '11h', PRs: 10, 'Build %': '95%', 'Focus %': '76%', 'AI LOC %': '34%' },
      { Name: 'Priya Patel',  Seniority: 'Senior', Tasks: 9,  'Dev Time': '13h', PRs: 8,  'Build %': '92%', 'Focus %': '74%', 'AI LOC %': '19%' },
      { Name: 'Emma Davis',   Seniority: 'Lead',   Tasks: 8,  'Dev Time': '10h', PRs: 7,  'Build %': '96%', 'Focus %': '81%', 'AI LOC %': '22%' },
      { Name: 'Ana Kovac',    Seniority: 'Middle', Tasks: 7,  'Dev Time': '15h', PRs: 6,  'Build %': '89%', 'Focus %': '65%', 'AI LOC %': '16%' },
      { Name: 'Carlos Ruiz',  Seniority: 'Middle', Tasks: 6,  'Dev Time': '16h', PRs: 5,  'Build %': '88%', 'Focus %': '59%', 'AI LOC %': '14%' },
      { Name: 'Sara Jansen',  Seniority: 'Junior', Tasks: 5,  'Dev Time': '18h', PRs: 4,  'Build %': '85%', 'Focus %': '62%', 'AI LOC %': '12%' },
      { Name: 'James Wilson', Seniority: 'Junior', Tasks: 4,  'Dev Time': '20h', PRs: 3,  'Build %': '80%', 'Focus %': '42%', 'AI LOC %': '0%'  },
      { Name: 'Nina Ross',    Seniority: 'Junior', Tasks: 3,  'Dev Time': '22h', PRs: 2,  'Build %': '83%', 'Focus %': '53%', 'AI LOC %': '10%' },
      { Name: 'Tom Sullivan', Seniority: 'Middle', Tasks: 3,  'Dev Time': '26h', PRs: 3,  'Build %': '82%', 'Focus %': '55%', 'AI LOC %': '8%'  },
      { Name: 'Ben Clarke',   Seniority: 'Middle', Tasks: 2,  'Dev Time': '31h', PRs: 2,  'Build %': '78%', 'Focus %': '48%', 'AI LOC %': '0%'  },
    ],
  },
  'team-tasks': {
    title: 'Tasks Closed per Developer',
    source: 'Jira',
    srcClass: 'bg-blue-600',
    value: '5.3 avg',
    filter: 'team = backend-dev AND status = Done AND sprint in closedSprints() ORDER BY tasks DESC',
    columns: ['Name', 'Tasks Closed', 'Story Points', 'Avg SP / Task', 'vs Team'],
    rows: [
      { Name: 'Alice Kim',    'Tasks Closed': 12, 'Story Points': 38, 'Avg SP / Task': '3.2', 'vs Team': '+126%' },
      { Name: 'Leo Zhang',    'Tasks Closed': 11, 'Story Points': 35, 'Avg SP / Task': '3.2', 'vs Team': '+108%' },
      { Name: 'Mike Chen',    'Tasks Closed': 10, 'Story Points': 32, 'Avg SP / Task': '3.2', 'vs Team': '+89%'  },
      { Name: 'Priya Patel',  'Tasks Closed': 9,  'Story Points': 28, 'Avg SP / Task': '3.1', 'vs Team': '+70%'  },
      { Name: 'Emma Davis',   'Tasks Closed': 8,  'Story Points': 24, 'Avg SP / Task': '3.0', 'vs Team': '+51%'  },
      { Name: 'Ana Kovac',    'Tasks Closed': 7,  'Story Points': 21, 'Avg SP / Task': '3.0', 'vs Team': '+32%'  },
      { Name: 'Carlos Ruiz',  'Tasks Closed': 6,  'Story Points': 18, 'Avg SP / Task': '3.0', 'vs Team': '+13%'  },
      { Name: 'Sara Jansen',  'Tasks Closed': 5,  'Story Points': 14, 'Avg SP / Task': '2.8', 'vs Team': '-6%'   },
      { Name: 'James Wilson', 'Tasks Closed': 4,  'Story Points': 11, 'Avg SP / Task': '2.8', 'vs Team': '-25%'  },
      { Name: 'Nina Ross',    'Tasks Closed': 3,  'Story Points': 8,  'Avg SP / Task': '2.7', 'vs Team': '-43%'  },
      { Name: 'Tom Sullivan', 'Tasks Closed': 3,  'Story Points': 8,  'Avg SP / Task': '2.7', 'vs Team': '-43%'  },
      { Name: 'Ben Clarke',   'Tasks Closed': 2,  'Story Points': 5,  'Avg SP / Task': '2.5', 'vs Team': '-62%'  },
    ],
  },
  'team-dev-time': {
    title: 'Task Development Time per Member',
    source: 'Jira',
    srcClass: 'bg-blue-600',
    value: '14h median',
    filter: 'team = backend-dev AND status changed to "In Progress" ORDER BY dev_time ASC',
    columns: ['Name', 'Dev Time', 'vs Team (14h)', 'vs Company (16h)', 'Tasks Sampled'],
    rows: [
      { Name: 'Emma Davis',   'Dev Time': '10h', 'vs Team (14h)': '↓ 29%', 'vs Company (16h)': '↓ 38%', 'Tasks Sampled': 8  },
      { Name: 'Leo Zhang',    'Dev Time': '11h', 'vs Team (14h)': '↓ 21%', 'vs Company (16h)': '↓ 31%', 'Tasks Sampled': 11 },
      { Name: 'Mike Chen',    'Dev Time': '12h', 'vs Team (14h)': '↓ 14%', 'vs Company (16h)': '↓ 25%', 'Tasks Sampled': 10 },
      { Name: 'Alice Kim',    'Dev Time': '14h', 'vs Team (14h)': '=',     'vs Company (16h)': '↓ 13%', 'Tasks Sampled': 12 },
      { Name: 'Priya Patel',  'Dev Time': '13h', 'vs Team (14h)': '↓ 7%', 'vs Company (16h)': '↓ 19%', 'Tasks Sampled': 9  },
      { Name: 'Ana Kovac',    'Dev Time': '15h', 'vs Team (14h)': '↑ 7%', 'vs Company (16h)': '↓ 6%',  'Tasks Sampled': 7  },
      { Name: 'Carlos Ruiz',  'Dev Time': '16h', 'vs Team (14h)': '↑ 14%','vs Company (16h)': '=',      'Tasks Sampled': 6  },
      { Name: 'Sara Jansen',  'Dev Time': '18h', 'vs Team (14h)': '↑ 29%','vs Company (16h)': '↑ 13%', 'Tasks Sampled': 5  },
      { Name: 'James Wilson', 'Dev Time': '20h', 'vs Team (14h)': '↑ 43%','vs Company (16h)': '↑ 25%', 'Tasks Sampled': 4  },
      { Name: 'Nina Ross',    'Dev Time': '22h', 'vs Team (14h)': '↑ 57%','vs Company (16h)': '↑ 38%', 'Tasks Sampled': 3  },
      { Name: 'Tom Sullivan', 'Dev Time': '26h', 'vs Team (14h)': '↑ 86%','vs Company (16h)': '↑ 63%', 'Tasks Sampled': 3  },
      { Name: 'Ben Clarke',   'Dev Time': '31h', 'vs Team (14h)': '↑ 121%','vs Company (16h)': '↑ 94%','Tasks Sampled': 2  },
    ],
  },
  'team-build': {
    title: 'Build Success Rate per Member',
    source: 'Bitbucket CI',
    srcClass: 'bg-blue-800',
    value: '89% avg',
    filter: 'team = backend-dev AND build.date >= period_start ORDER BY build_success DESC',
    columns: ['Name', 'Build %', 'Passed', 'Failed', 'Status'],
    rows: [
      { Name: 'Alice Kim',    'Build %': '97%', Passed: 64, Failed: 2,  Status: '✓ Good'    },
      { Name: 'Emma Davis',   'Build %': '96%', Passed: 54, Failed: 2,  Status: '✓ Good'    },
      { Name: 'Leo Zhang',    'Build %': '95%', Passed: 73, Failed: 4,  Status: '✓ Good'    },
      { Name: 'Mike Chen',    'Build %': '94%', Passed: 61, Failed: 4,  Status: '✓ Good'    },
      { Name: 'Priya Patel',  'Build %': '92%', Passed: 58, Failed: 5,  Status: '✓ Good'    },
      { Name: 'Ana Kovac',    'Build %': '89%', Passed: 48, Failed: 6,  Status: '✓ Good'    },
      { Name: 'Carlos Ruiz',  'Build %': '88%', Passed: 44, Failed: 6,  Status: '✓ Good'    },
      { Name: 'Nina Ross',    'Build %': '83%', Passed: 30, Failed: 6,  Status: '⚠ Monitor' },
      { Name: 'Sara Jansen',  'Build %': '85%', Passed: 34, Failed: 6,  Status: '⚠ Monitor' },
      { Name: 'James Wilson', 'Build %': '80%', Passed: 28, Failed: 7,  Status: '⚠ Monitor' },
      { Name: 'Tom Sullivan', 'Build %': '82%', Passed: 32, Failed: 7,  Status: '⚠ Monitor' },
      { Name: 'Ben Clarke',   'Build %': '78%', Passed: 25, Failed: 7,  Status: '✗ At risk' },
    ],
  },
  'team-prs': {
    title: 'Pull Requests Merged per Developer',
    source: 'Bitbucket',
    srcClass: 'bg-blue-800',
    value: '7.2 avg',
    filter: 'team = backend-dev AND state = MERGED ORDER BY pr_count DESC',
    columns: ['Name', 'PRs Merged', 'Avg Cycle Time', 'Avg Size (LOC)', 'Reviews Given'],
    rows: [
      { Name: 'Alice Kim',    'PRs Merged': 11, 'Avg Cycle Time': '16h', 'Avg Size (LOC)': 280, 'Reviews Given': 8  },
      { Name: 'Leo Zhang',    'PRs Merged': 10, 'Avg Cycle Time': '14h', 'Avg Size (LOC)': 260, 'Reviews Given': 9  },
      { Name: 'Mike Chen',    'PRs Merged': 9,  'Avg Cycle Time': '15h', 'Avg Size (LOC)': 240, 'Reviews Given': 7  },
      { Name: 'Priya Patel',  'PRs Merged': 8,  'Avg Cycle Time': '18h', 'Avg Size (LOC)': 220, 'Reviews Given': 6  },
      { Name: 'Emma Davis',   'PRs Merged': 7,  'Avg Cycle Time': '12h', 'Avg Size (LOC)': 190, 'Reviews Given': 11 },
      { Name: 'Ana Kovac',    'PRs Merged': 6,  'Avg Cycle Time': '19h', 'Avg Size (LOC)': 180, 'Reviews Given': 5  },
      { Name: 'Carlos Ruiz',  'PRs Merged': 5,  'Avg Cycle Time': '21h', 'Avg Size (LOC)': 160, 'Reviews Given': 4  },
      { Name: 'Sara Jansen',  'PRs Merged': 4,  'Avg Cycle Time': '24h', 'Avg Size (LOC)': 140, 'Reviews Given': 3  },
      { Name: 'James Wilson', 'PRs Merged': 3,  'Avg Cycle Time': '28h', 'Avg Size (LOC)': 120, 'Reviews Given': 2  },
      { Name: 'Nina Ross',    'PRs Merged': 2,  'Avg Cycle Time': '30h', 'Avg Size (LOC)': 100, 'Reviews Given': 2  },
      { Name: 'Tom Sullivan', 'PRs Merged': 3,  'Avg Cycle Time': '26h', 'Avg Size (LOC)': 130, 'Reviews Given': 3  },
      { Name: 'Ben Clarke',   'PRs Merged': 2,  'Avg Cycle Time': '32h', 'Avg Size (LOC)': 90,  'Reviews Given': 1  },
    ],
  },
  'team-pr-cycle': {
    title: 'Pull Request Cycle Time per Member',
    source: 'Bitbucket',
    srcClass: 'bg-blue-800',
    value: '22h median',
    filter: 'team = backend-dev AND state = MERGED ORDER BY cycle_time ASC',
    columns: ['Name', 'Cycle Time', 'Pickup Time', 'Review Time', 'vs Company (19h)'],
    rows: [
      { Name: 'Emma Davis',   'Cycle Time': '12h', 'Pickup Time': '2h',  'Review Time': '4h',  'vs Company (19h)': '↓ 37%' },
      { Name: 'Leo Zhang',    'Cycle Time': '14h', 'Pickup Time': '3h',  'Review Time': '5h',  'vs Company (19h)': '↓ 26%' },
      { Name: 'Mike Chen',    'Cycle Time': '15h', 'Pickup Time': '3h',  'Review Time': '5h',  'vs Company (19h)': '↓ 21%' },
      { Name: 'Alice Kim',    'Cycle Time': '16h', 'Pickup Time': '4h',  'Review Time': '6h',  'vs Company (19h)': '↓ 16%' },
      { Name: 'Priya Patel',  'Cycle Time': '18h', 'Pickup Time': '4h',  'Review Time': '7h',  'vs Company (19h)': '↓ 5%'  },
      { Name: 'Carlos Ruiz',  'Cycle Time': '21h', 'Pickup Time': '5h',  'Review Time': '9h',  'vs Company (19h)': '↑ 11%' },
      { Name: 'Ana Kovac',    'Cycle Time': '19h', 'Pickup Time': '5h',  'Review Time': '8h',  'vs Company (19h)': '='      },
      { Name: 'Sara Jansen',  'Cycle Time': '24h', 'Pickup Time': '6h',  'Review Time': '10h', 'vs Company (19h)': '↑ 26%' },
      { Name: 'Tom Sullivan', 'Cycle Time': '26h', 'Pickup Time': '8h',  'Review Time': '11h', 'vs Company (19h)': '↑ 37%' },
      { Name: 'James Wilson', 'Cycle Time': '28h', 'Pickup Time': '9h',  'Review Time': '12h', 'vs Company (19h)': '↑ 47%' },
      { Name: 'Nina Ross',    'Cycle Time': '30h', 'Pickup Time': '10h', 'Review Time': '13h', 'vs Company (19h)': '↑ 58%' },
      { Name: 'Ben Clarke',   'Cycle Time': '32h', 'Pickup Time': '12h', 'Review Time': '14h', 'vs Company (19h)': '↑ 68%' },
    ],
  },
  'team-bugs': {
    title: 'Bugs Fixed per Member',
    source: 'Jira',
    srcClass: 'bg-blue-600',
    value: '4 median',
    filter: 'team = backend-dev AND issuetype = Bug AND status = Done ORDER BY bugs DESC',
    columns: ['Name', 'Bugs Fixed', 'Avg Fix Time', 'Reopened', 'Reopen Rate'],
    rows: [
      { Name: 'Leo Zhang',    'Bugs Fixed': 6,  'Avg Fix Time': '4h',  Reopened: 0, 'Reopen Rate': '0%'  },
      { Name: 'Alice Kim',    'Bugs Fixed': 5,  'Avg Fix Time': '5h',  Reopened: 0, 'Reopen Rate': '0%'  },
      { Name: 'Mike Chen',    'Bugs Fixed': 4,  'Avg Fix Time': '6h',  Reopened: 0, 'Reopen Rate': '0%'  },
      { Name: 'Priya Patel',  'Bugs Fixed': 4,  'Avg Fix Time': '7h',  Reopened: 0, 'Reopen Rate': '0%'  },
      { Name: 'Emma Davis',   'Bugs Fixed': 2,  'Avg Fix Time': '4h',  Reopened: 0, 'Reopen Rate': '0%'  },
      { Name: 'Carlos Ruiz',  'Bugs Fixed': 3,  'Avg Fix Time': '8h',  Reopened: 0, 'Reopen Rate': '0%'  },
      { Name: 'Ana Kovac',    'Bugs Fixed': 3,  'Avg Fix Time': '9h',  Reopened: 1, 'Reopen Rate': '33%' },
      { Name: 'Sara Jansen',  'Bugs Fixed': 3,  'Avg Fix Time': '10h', Reopened: 0, 'Reopen Rate': '0%'  },
      { Name: 'James Wilson', 'Bugs Fixed': 2,  'Avg Fix Time': '12h', Reopened: 1, 'Reopen Rate': '50%' },
      { Name: 'Nina Ross',    'Bugs Fixed': 1,  'Avg Fix Time': '14h', Reopened: 0, 'Reopen Rate': '0%'  },
      { Name: 'Tom Sullivan', 'Bugs Fixed': 2,  'Avg Fix Time': '13h', Reopened: 1, 'Reopen Rate': '50%' },
      { Name: 'Ben Clarke',   'Bugs Fixed': 1,  'Avg Fix Time': '18h', Reopened: 1, 'Reopen Rate': '100%'},
    ],
  },
  'team-focus': {
    title: 'Focus Time per Member',
    source: 'Calendar / M365',
    srcClass: 'bg-purple-600',
    value: '63% avg',
    filter: 'team = backend-dev AND calendar.event_type = focus_block ORDER BY focus_pct DESC',
    columns: ['Name', 'Focus %', 'Focus Hours', 'Meeting Hours', 'Target ≥60%'],
    rows: [
      { Name: 'Emma Davis',   'Focus %': '81%', 'Focus Hours': '130h', 'Meeting Hours': '28h', 'Target ≥60%': '✓'  },
      { Name: 'Leo Zhang',    'Focus %': '76%', 'Focus Hours': '122h', 'Meeting Hours': '36h', 'Target ≥60%': '✓'  },
      { Name: 'Alice Kim',    'Focus %': '72%', 'Focus Hours': '115h', 'Meeting Hours': '42h', 'Target ≥60%': '✓'  },
      { Name: 'Priya Patel',  'Focus %': '74%', 'Focus Hours': '118h', 'Meeting Hours': '38h', 'Target ≥60%': '✓'  },
      { Name: 'Mike Chen',    'Focus %': '68%', 'Focus Hours': '109h', 'Meeting Hours': '48h', 'Target ≥60%': '✓'  },
      { Name: 'Ana Kovac',    'Focus %': '65%', 'Focus Hours': '104h', 'Meeting Hours': '54h', 'Target ≥60%': '✓'  },
      { Name: 'Carlos Ruiz',  'Focus %': '59%', 'Focus Hours': '94h',  'Meeting Hours': '62h', 'Target ≥60%': '⚠' },
      { Name: 'Sara Jansen',  'Focus %': '62%', 'Focus Hours': '99h',  'Meeting Hours': '58h', 'Target ≥60%': '✓'  },
      { Name: 'Nina Ross',    'Focus %': '53%', 'Focus Hours': '85h',  'Meeting Hours': '72h', 'Target ≥60%': '⚠' },
      { Name: 'Tom Sullivan', 'Focus %': '55%', 'Focus Hours': '88h',  'Meeting Hours': '68h', 'Target ≥60%': '⚠' },
      { Name: 'James Wilson', 'Focus %': '42%', 'Focus Hours': '67h',  'Meeting Hours': '88h', 'Target ≥60%': '✗' },
      { Name: 'Ben Clarke',   'Focus %': '48%', 'Focus Hours': '77h',  'Meeting Hours': '80h', 'Target ≥60%': '✗' },
    ],
  },
  'team-ai-active': {
    title: 'AI Tool Adoption by Member',
    source: 'Cursor + Anthropic API',
    srcClass: 'bg-violet-600',
    value: '7 / 12 active',
    filter: 'team = backend-dev AND ai_activity > 0 ORDER BY ai_loc_share DESC',
    columns: ['Name', 'Tools Active', 'AI LOC Share', 'Cursor Sessions', 'CC Sessions'],
    rows: [
      { Name: 'Leo Zhang',    'Tools Active': 'Claude Code, Codex',    'AI LOC Share': '34%', 'Cursor Sessions': 0,  'CC Sessions': 31 },
      { Name: 'Mike Chen',    'Tools Active': 'Cursor, Claude Code',   'AI LOC Share': '31%', 'Cursor Sessions': 18, 'CC Sessions': 22 },
      { Name: 'Alice Kim',    'Tools Active': 'Cursor, Claude Code',   'AI LOC Share': '27%', 'Cursor Sessions': 14, 'CC Sessions': 24 },
      { Name: 'Emma Davis',   'Tools Active': 'Claude Code',           'AI LOC Share': '22%', 'Cursor Sessions': 0,  'CC Sessions': 28 },
      { Name: 'Priya Patel',  'Tools Active': 'Cursor, Codex',         'AI LOC Share': '19%', 'Cursor Sessions': 20, 'CC Sessions': 0  },
      { Name: 'Ana Kovac',    'Tools Active': 'Cursor',                'AI LOC Share': '16%', 'Cursor Sessions': 12, 'CC Sessions': 0  },
      { Name: 'Carlos Ruiz',  'Tools Active': 'Cursor',                'AI LOC Share': '14%', 'Cursor Sessions': 10, 'CC Sessions': 0  },
      { Name: 'Sara Jansen',  'Tools Active': 'Cursor',                'AI LOC Share': '12%', 'Cursor Sessions': 8,  'CC Sessions': 0  },
      { Name: 'Nina Ross',    'Tools Active': 'Cursor',                'AI LOC Share': '10%', 'Cursor Sessions': 6,  'CC Sessions': 0  },
      { Name: 'Tom Sullivan', 'Tools Active': 'Codex',                 'AI LOC Share': '8%',  'Cursor Sessions': 0,  'CC Sessions': 0  },
      { Name: 'James Wilson', 'Tools Active': '—',                     'AI LOC Share': '0%',  'Cursor Sessions': 0,  'CC Sessions': 0  },
      { Name: 'Ben Clarke',   'Tools Active': '—',                     'AI LOC Share': '0%',  'Cursor Sessions': 0,  'CC Sessions': 0  },
    ],
  },
  'team-ai-loc': {
    title: 'AI LOC Share per Member',
    source: 'Cursor + Anthropic API',
    srcClass: 'bg-violet-600',
    value: '16% team avg',
    filter: 'team = backend-dev AND ai_loc > 0 ORDER BY ai_loc_share DESC',
    columns: ['Name', 'AI LOC %', 'AI Lines', 'Clean LOC', 'Cursor Lines', 'CC Lines'],
    rows: [
      { Name: 'Leo Zhang',    'AI LOC %': '34%', 'AI Lines': 3900, 'Clean LOC': '11.4k', 'Cursor Lines': 1800, 'CC Lines': 2100 },
      { Name: 'Mike Chen',    'AI LOC %': '31%', 'AI Lines': 3500, 'Clean LOC': '11.3k', 'Cursor Lines': 2200, 'CC Lines': 1300 },
      { Name: 'Alice Kim',    'AI LOC %': '27%', 'AI Lines': 3200, 'Clean LOC': '12.0k', 'Cursor Lines': 2100, 'CC Lines': 1100 },
      { Name: 'Emma Davis',   'AI LOC %': '22%', 'AI Lines': 2200, 'Clean LOC': '10.0k', 'Cursor Lines': 0,    'CC Lines': 2200 },
      { Name: 'Priya Patel',  'AI LOC %': '19%', 'AI Lines': 1900, 'Clean LOC': '10.0k', 'Cursor Lines': 1900, 'CC Lines': 0    },
      { Name: 'Ana Kovac',    'AI LOC %': '16%', 'AI Lines': 1440, 'Clean LOC': '9.0k',  'Cursor Lines': 1440, 'CC Lines': 0    },
      { Name: 'Carlos Ruiz',  'AI LOC %': '14%', 'AI Lines': 1120, 'Clean LOC': '8.0k',  'Cursor Lines': 1120, 'CC Lines': 0    },
      { Name: 'Sara Jansen',  'AI LOC %': '12%', 'AI Lines': 840,  'Clean LOC': '7.0k',  'Cursor Lines': 840,  'CC Lines': 0    },
      { Name: 'Nina Ross',    'AI LOC %': '10%', 'AI Lines': 560,  'Clean LOC': '5.6k',  'Cursor Lines': 560,  'CC Lines': 0    },
      { Name: 'Tom Sullivan', 'AI LOC %': '8%',  'AI Lines': 360,  'Clean LOC': '4.5k',  'Cursor Lines': 0,    'CC Lines': 360  },
      { Name: 'James Wilson', 'AI LOC %': '0%',  'AI Lines': 0,    'Clean LOC': '4.0k',  'Cursor Lines': 0,    'CC Lines': 0    },
      { Name: 'Ben Clarke',   'AI LOC %': '0%',  'AI Lines': 0,    'Clean LOC': '3.0k',  'Cursor Lines': 0,    'CC Lines': 0    },
    ],
  },
  'team-reopen': {
    title: 'Task Reopen Rate per Member',
    source: 'Jira',
    srcClass: 'bg-blue-600',
    value: '11% team avg',
    filter: 'team = backend-dev AND status changed from Done to "In Progress" after -30d ORDER BY reopen_rate ASC',
    columns: ['Name', 'Reopen Rate', 'Reopened', 'Total Closed', 'Reason (top)'],
    rows: [
      { Name: 'Alice Kim',    'Reopen Rate': '4%',  Reopened: 1, 'Total Closed': 25, 'Reason (top)': 'QA regression'      },
      { Name: 'Leo Zhang',    'Reopen Rate': '5%',  Reopened: 1, 'Total Closed': 20, 'Reason (top)': 'Spec change'         },
      { Name: 'Mike Chen',    'Reopen Rate': '6%',  Reopened: 1, 'Total Closed': 17, 'Reason (top)': 'Missing edge case'   },
      { Name: 'Emma Davis',   'Reopen Rate': '7%',  Reopened: 1, 'Total Closed': 14, 'Reason (top)': 'QA regression'      },
      { Name: 'Priya Patel',  'Reopen Rate': '8%',  Reopened: 1, 'Total Closed': 12, 'Reason (top)': 'Acceptance criteria' },
      { Name: 'Carlos Ruiz',  'Reopen Rate': '10%', Reopened: 1, 'Total Closed': 10, 'Reason (top)': 'Spec change'         },
      { Name: 'Ana Kovac',    'Reopen Rate': '11%', Reopened: 1, 'Total Closed': 9,  'Reason (top)': 'Missing edge case'   },
      { Name: 'Sara Jansen',  'Reopen Rate': '13%', Reopened: 1, 'Total Closed': 8,  'Reason (top)': 'QA regression'      },
      { Name: 'Tom Sullivan', 'Reopen Rate': '17%', Reopened: 1, 'Total Closed': 6,  'Reason (top)': 'Spec change'         },
      { Name: 'Nina Ross',    'Reopen Rate': '20%', Reopened: 1, 'Total Closed': 5,  'Reason (top)': 'Missing edge case'   },
      { Name: 'James Wilson', 'Reopen Rate': '25%', Reopened: 1, 'Total Closed': 4,  'Reason (top)': 'Acceptance criteria' },
      { Name: 'Ben Clarke',   'Reopen Rate': '33%', Reopened: 1, 'Total Closed': 3,  'Reason (top)': 'QA regression'      },
    ],
  },
};

/**
 * Mock map for insight API service
 * Period is embedded in the URL path so the mock plugin can route correctly.
 */
export const insightMockMap = {
  'GET /api/insight/dashboard': (): DashboardData => mockDashboardData,
  'GET /api/insight/speed': (): SpeedData => mockSpeedData,
  // Executive View — one entry per period
  'GET /api/insight/executive-view/week':    (): ExecViewData => EXEC_VIEW_MOCK['week'],
  'GET /api/insight/executive-view/month':   (): ExecViewData => EXEC_VIEW_MOCK['month'],
  'GET /api/insight/executive-view/quarter': (): ExecViewData => EXEC_VIEW_MOCK['quarter'],
  'GET /api/insight/executive-view/year':    (): ExecViewData => EXEC_VIEW_MOCK['year'],
  // Team View — one entry per period
  'GET /api/insight/team-view/week':    (): TeamViewData => TEAM_VIEW_MOCK['week'],
  'GET /api/insight/team-view/month':   (): TeamViewData => TEAM_VIEW_MOCK['month'],
  'GET /api/insight/team-view/quarter': (): TeamViewData => TEAM_VIEW_MOCK['quarter'],
  'GET /api/insight/team-view/year':    (): TeamViewData => TEAM_VIEW_MOCK['year'],
  // IC Dashboard — flat paths per personId + period (wildcards ignored by mock framework)
  ...IC_MOCK_ENTRIES,
  // Team View drills — one entry per drill ID per period
  'GET /api/insight/team-view/drill/team-tasks/week':    (): DrillData => TEAM_DRILLS['team-tasks']!,
  'GET /api/insight/team-view/drill/team-tasks/month':   (): DrillData => TEAM_DRILLS['team-tasks']!,
  'GET /api/insight/team-view/drill/team-tasks/quarter': (): DrillData => TEAM_DRILLS['team-tasks']!,
  'GET /api/insight/team-view/drill/team-tasks/year':    (): DrillData => TEAM_DRILLS['team-tasks']!,
  'GET /api/insight/team-view/drill/team-dev-time/week':    (): DrillData => TEAM_DRILLS['team-dev-time']!,
  'GET /api/insight/team-view/drill/team-dev-time/month':   (): DrillData => TEAM_DRILLS['team-dev-time']!,
  'GET /api/insight/team-view/drill/team-dev-time/quarter': (): DrillData => TEAM_DRILLS['team-dev-time']!,
  'GET /api/insight/team-view/drill/team-dev-time/year':    (): DrillData => TEAM_DRILLS['team-dev-time']!,
  'GET /api/insight/team-view/drill/team-build/week':    (): DrillData => TEAM_DRILLS['team-build']!,
  'GET /api/insight/team-view/drill/team-build/month':   (): DrillData => TEAM_DRILLS['team-build']!,
  'GET /api/insight/team-view/drill/team-build/quarter': (): DrillData => TEAM_DRILLS['team-build']!,
  'GET /api/insight/team-view/drill/team-build/year':    (): DrillData => TEAM_DRILLS['team-build']!,
  'GET /api/insight/team-view/drill/team-prs/week':    (): DrillData => TEAM_DRILLS['team-prs']!,
  'GET /api/insight/team-view/drill/team-prs/month':   (): DrillData => TEAM_DRILLS['team-prs']!,
  'GET /api/insight/team-view/drill/team-prs/quarter': (): DrillData => TEAM_DRILLS['team-prs']!,
  'GET /api/insight/team-view/drill/team-prs/year':    (): DrillData => TEAM_DRILLS['team-prs']!,
  'GET /api/insight/team-view/drill/team-pr-cycle/week':    (): DrillData => TEAM_DRILLS['team-pr-cycle']!,
  'GET /api/insight/team-view/drill/team-pr-cycle/month':   (): DrillData => TEAM_DRILLS['team-pr-cycle']!,
  'GET /api/insight/team-view/drill/team-pr-cycle/quarter': (): DrillData => TEAM_DRILLS['team-pr-cycle']!,
  'GET /api/insight/team-view/drill/team-pr-cycle/year':    (): DrillData => TEAM_DRILLS['team-pr-cycle']!,
  'GET /api/insight/team-view/drill/team-bugs/week':    (): DrillData => TEAM_DRILLS['team-bugs']!,
  'GET /api/insight/team-view/drill/team-bugs/month':   (): DrillData => TEAM_DRILLS['team-bugs']!,
  'GET /api/insight/team-view/drill/team-bugs/quarter': (): DrillData => TEAM_DRILLS['team-bugs']!,
  'GET /api/insight/team-view/drill/team-bugs/year':    (): DrillData => TEAM_DRILLS['team-bugs']!,
  'GET /api/insight/team-view/drill/team-focus/week':    (): DrillData => TEAM_DRILLS['team-focus']!,
  'GET /api/insight/team-view/drill/team-focus/month':   (): DrillData => TEAM_DRILLS['team-focus']!,
  'GET /api/insight/team-view/drill/team-focus/quarter': (): DrillData => TEAM_DRILLS['team-focus']!,
  'GET /api/insight/team-view/drill/team-focus/year':    (): DrillData => TEAM_DRILLS['team-focus']!,
  'GET /api/insight/team-view/drill/team-ai-active/week':    (): DrillData => TEAM_DRILLS['team-ai-active']!,
  'GET /api/insight/team-view/drill/team-ai-active/month':   (): DrillData => TEAM_DRILLS['team-ai-active']!,
  'GET /api/insight/team-view/drill/team-ai-active/quarter': (): DrillData => TEAM_DRILLS['team-ai-active']!,
  'GET /api/insight/team-view/drill/team-ai-active/year':    (): DrillData => TEAM_DRILLS['team-ai-active']!,
  'GET /api/insight/team-view/drill/team-ai-loc/week':    (): DrillData => TEAM_DRILLS['team-ai-loc']!,
  'GET /api/insight/team-view/drill/team-ai-loc/month':   (): DrillData => TEAM_DRILLS['team-ai-loc']!,
  'GET /api/insight/team-view/drill/team-ai-loc/quarter': (): DrillData => TEAM_DRILLS['team-ai-loc']!,
  'GET /api/insight/team-view/drill/team-ai-loc/year':    (): DrillData => TEAM_DRILLS['team-ai-loc']!,
  'GET /api/insight/team-view/drill/team-reopen/week':    (): DrillData => TEAM_DRILLS['team-reopen']!,
  'GET /api/insight/team-view/drill/team-reopen/month':   (): DrillData => TEAM_DRILLS['team-reopen']!,
  'GET /api/insight/team-view/drill/team-reopen/quarter': (): DrillData => TEAM_DRILLS['team-reopen']!,
  'GET /api/insight/team-view/drill/team-reopen/year':    (): DrillData => TEAM_DRILLS['team-reopen']!,
  'GET /api/insight/team-view/drill/team-members/week':    (): DrillData => TEAM_DRILLS['team-members']!,
  'GET /api/insight/team-view/drill/team-members/month':   (): DrillData => TEAM_DRILLS['team-members']!,
  'GET /api/insight/team-view/drill/team-members/quarter': (): DrillData => TEAM_DRILLS['team-members']!,
  'GET /api/insight/team-view/drill/team-members/year':    (): DrillData => TEAM_DRILLS['team-members']!,
  // IC Dashboard drills — flat paths per personId + drillId
  ...IC_DRILL_ENTRIES,
} satisfies MockMap;

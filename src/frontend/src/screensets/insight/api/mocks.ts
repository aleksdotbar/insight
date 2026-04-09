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

// ---------------------------------------------------------------------------
// Fixture imports
// ---------------------------------------------------------------------------
import { EXEC_TEAMS_MONTH, EXEC_ORG_KPIS_MONTH, EXEC_VIEW_CONFIG, EXEC_DATA_AVAILABILITY } from './mocks/fixtures/executive-view';
import { TEAM_MEMBERS_MONTH, BULLET_SECTIONS_MONTH, TEAM_KPIS_BY_PERIOD, TEAM_VIEW_CONFIG, TEAM_DATA_AVAILABILITY } from './mocks/fixtures/team-view-base';
import { IC_DASHBOARD_MOCK } from './mocks/fixtures/ic-alice';
import { TEAM_DRILLS } from './mocks/fixtures/team-drills';

// Re-export for external consumers
export { TEAM_MEMBERS_MONTH, IC_DASHBOARD_MOCK };

// ---------------------------------------------------------------------------
// Legacy inline mocks (small)
// ---------------------------------------------------------------------------

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
// Executive View Mock — scaling helpers & computation
// ---------------------------------------------------------------------------

type RateDeltas = { build: number; focus: number; ai: number; aiLoc: number; cycle: number };

function scaleExecTeams(teams: ExecTeamRow[], factor: number, rates?: RateDeltas): ExecTeamRow[] {
  return teams.map((t) => ({
    ...t,
    tasks_closed:      t.tasks_closed !== null ? Math.round(t.tasks_closed * factor) : null,
    bugs_fixed:        t.bugs_fixed   !== null ? Math.round(t.bugs_fixed   * factor) : null,
    build_success_pct: t.build_success_pct !== null
      ? (rates ? Math.min(100, Math.max(0, t.build_success_pct + rates.build)) : t.build_success_pct)
      : null,
    focus_time_pct:    rates ? Math.min(100, Math.max(0, t.focus_time_pct   + rates.focus)) : t.focus_time_pct,
    ai_adoption_pct:   rates ? Math.min(100, Math.max(0, t.ai_adoption_pct  + rates.ai))   : t.ai_adoption_pct,
    ai_loc_share_pct:  rates ? Math.min(100, Math.max(0, t.ai_loc_share_pct + rates.aiLoc)): t.ai_loc_share_pct,
    pr_cycle_time_h:   rates ? Math.max(1,               t.pr_cycle_time_h  + rates.cycle)  : t.pr_cycle_time_h,
  }));
}

export const EXEC_VIEW_MOCK: Record<PeriodValue, ExecViewData> = {
  week: {
    teams: scaleExecTeams(EXEC_TEAMS_MONTH, 0.25, { build: +2, focus: -3, ai: -5, aiLoc: -4, cycle: -3 }),
    orgKpis: { avgBuildSuccess: 93, avgAiAdoption: 55, avgFocus: 61, bugResolutionScore: 80, prCycleScore: 68 },
    config: EXEC_VIEW_CONFIG,
    data_availability: EXEC_DATA_AVAILABILITY,
  },
  month: {
    teams: EXEC_TEAMS_MONTH,
    orgKpis: EXEC_ORG_KPIS_MONTH,
    config: EXEC_VIEW_CONFIG,
    data_availability: EXEC_DATA_AVAILABILITY,
  },
  quarter: {
    teams: scaleExecTeams(EXEC_TEAMS_MONTH, 3, { build: -2, focus: +2, ai: +4, aiLoc: +2, cycle: +2 }),
    orgKpis: { avgBuildSuccess: 88, avgAiAdoption: 62, avgFocus: 64, bugResolutionScore: 76, prCycleScore: 63 },
    config: EXEC_VIEW_CONFIG,
    data_availability: EXEC_DATA_AVAILABILITY,
  },
  year: {
    teams: scaleExecTeams(EXEC_TEAMS_MONTH, 12, { build: -3, focus: +3, ai: -2, aiLoc: -3, cycle: +4 }),
    orgKpis: { avgBuildSuccess: 87, avgAiAdoption: 57, avgFocus: 62, bugResolutionScore: 75, prCycleScore: 62 },
    config: EXEC_VIEW_CONFIG,
    data_availability: EXEC_DATA_AVAILABILITY,
  },
};

// ---------------------------------------------------------------------------
// Team View Mock — scaling helpers & computation
// ---------------------------------------------------------------------------

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

// Derive computed KPIs from actual member data using configured thresholds
function withComputedKpis(teamKpis: TeamKpi[], members: TeamMember[]): TeamKpi[] {
  const focusTrigger = TEAM_VIEW_CONFIG.alert_thresholds.find((t) => t.metric_key === 'focus_time_pct')?.trigger ?? 60;
  const total = members.length;

  const atRisk = members.filter((m) =>
    TEAM_VIEW_CONFIG.alert_thresholds.some(
      (t) => (m[t.metric_key as keyof TeamMember] as number) < t.trigger,
    ),
  ).length;
  const atRiskStatus: 'good' | 'warn' | 'bad' = atRisk === 0 ? 'good' : atRisk <= 2 ? 'warn' : 'bad';

  const focusCount = members.filter((m) => m.focus_time_pct >= focusTrigger).length;
  const belowFocus = total - focusCount;
  const focusStatus: 'good' | 'warn' | 'bad' = belowFocus === 0 ? 'good' : belowFocus <= 2 ? 'warn' : 'bad';

  const noAiCount = members.filter((m) => m.ai_tools.length === 0).length;
  const noAiStatus: 'good' | 'warn' | 'bad' = noAiCount === 0 ? 'good' : noAiCount <= 2 ? 'warn' : 'bad';

  return teamKpis.map((k) => {
    if (k.metric_key === 'at_risk_count') return { ...k, value: String(atRisk), status: atRiskStatus };
    if (k.metric_key === 'focus_gte_60') return { ...k, value: `${focusCount} / ${total}`, sublabel: `${belowFocus} member${belowFocus !== 1 ? 's' : ''} below target`, status: focusStatus };
    if (k.metric_key === 'not_using_ai') return { ...k, value: String(noAiCount), status: noAiStatus };
    return k;
  });
}

// Team roster — maps teamId to member person_ids (must match ORG in currentUserEffects)
const TEAM_ROSTER: Record<string, { label: string; leadId: string; memberIds: string[] }> = {
  backend:  { label: 'Backend',  leadId: 'p5',  memberIds: ['p1', 'p3', 'p4'] },
  platform: { label: 'Platform', leadId: 'p6',  memberIds: ['p2', 'p8'] },
  frontend: { label: 'Frontend', leadId: 'p11', memberIds: ['p7', 'p9', 'p10', 'p12'] },
};

function buildTeamViewMock(teamId: string): Record<PeriodValue, TeamViewData> {
  const roster = TEAM_ROSTER[teamId];
  if (!roster) return {} as Record<PeriodValue, TeamViewData>;

  const allIds = [roster.leadId, ...roster.memberIds];
  const monthMembers = TEAM_MEMBERS_MONTH.filter((m) => allIds.includes(m.person_id));
  const weekMembers    = scaleTeamMembers(monthMembers, 'week', 0.25);
  const quarterMembers = scaleTeamMembers(monthMembers, 'quarter', 3);
  const yearMembers    = scaleTeamMembers(monthMembers, 'year', 12);

  return {
    week:    { teamName: roster.label, teamKpis: withComputedKpis(TEAM_KPIS_BY_PERIOD.week,    weekMembers),    members: weekMembers,    bulletSections: applyRateOverrides(BULLET_SECTIONS_MONTH, 'week'),    config: TEAM_VIEW_CONFIG, data_availability: TEAM_DATA_AVAILABILITY },
    month:   { teamName: roster.label, teamKpis: withComputedKpis(TEAM_KPIS_BY_PERIOD.month,   monthMembers),   members: monthMembers,   bulletSections: BULLET_SECTIONS_MONTH,                                 config: TEAM_VIEW_CONFIG, data_availability: TEAM_DATA_AVAILABILITY },
    quarter: { teamName: roster.label, teamKpis: withComputedKpis(TEAM_KPIS_BY_PERIOD.quarter, quarterMembers), members: quarterMembers, bulletSections: applyRateOverrides(BULLET_SECTIONS_MONTH, 'quarter'), config: TEAM_VIEW_CONFIG, data_availability: TEAM_DATA_AVAILABILITY },
    year:    { teamName: roster.label, teamKpis: withComputedKpis(TEAM_KPIS_BY_PERIOD.year,    yearMembers),    members: yearMembers,    bulletSections: applyRateOverrides(BULLET_SECTIONS_MONTH, 'year'),    config: TEAM_VIEW_CONFIG, data_availability: TEAM_DATA_AVAILABILITY },
  };
}

const TEAM_VIEW_MOCKS: Record<string, Record<PeriodValue, TeamViewData>> = {
  backend:  buildTeamViewMock('backend'),
  platform: buildTeamViewMock('platform'),
  frontend: buildTeamViewMock('frontend'),
};

// Legacy export (defaults to backend)
export const TEAM_VIEW_MOCK = TEAM_VIEW_MOCKS['backend'];

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

  // -- KPIs --
  const kpis: IcKpi[] = [
    { period, metric_key: 'tasks_closed',   label: 'Tasks Closed',  value: String(tasks),
      unit: '',  sublabel: 'Jira',                  description: 'Jira tickets closed in the selected period.',
      delta: '',               delta_type: 'neutral' },
    { period, metric_key: 'bugs_fixed',     label: 'Bugs Fixed',    value: String(bugs),
      unit: '',  sublabel: 'Jira',                  description: 'Bug tickets resolved and closed.',
      delta: '',               delta_type: bugs > 0 ? 'good' : 'neutral' },
    { period, metric_key: 'clean_loc',
      label: 'Clean LOC',      value: cleanLocK >= 1000 ? `${Math.round(cleanLocK / 1000)}k` : String(cleanLocK),
      unit: '',  sublabel: 'Bitbucket',             description: 'Production code lines, excl. tests & generated files.',
      delta: '',               delta_type: 'neutral' },
    { period, metric_key: 'ai_loc_share',   label: 'AI LOC Share',  value: String(aiLoc),
      unit: '%', sublabel: 'Cursor + Claude Code',  description: 'Code lines accepted from AI suggestions out of total authored.',
      delta: '\u2248 team avg 22%', delta_type: aiLoc >= 18 ? 'good' : 'neutral' },
    { period, metric_key: 'focus_time_pct', label: 'Focus Time',    value: String(focus),
      unit: '%', sublabel: 'Calendar / M365',       description: 'Work time in uninterrupted 60-min+ blocks.',
      delta: '',               delta_type: focus >= 60 ? 'good' : 'warn' },
  ];

  // -- Bullet metrics --
  const bulletMetrics: BulletMetric[] = [
    // task_delivery
    bm(period, 'task_delivery', 'tasks_completed', 'Tasks Completed', 'Jira \u00b7 closed issues in sprint',
      String(tasks), 'count', '0', '15', '7', 'Median: 7',
      barPct(tasks, 0, 15), 47, tasks >= 7 ? 'good' : tasks >= 4 ? 'warn' : 'bad', 'tasks-completed'),
    bm(period, 'task_delivery', 'task_dev_time', 'Task Development Time', 'Jira \u00b7 time in In Progress state \u00b7 lower = better',
      String(m.dev_time_h), 'h', '8h', '31h', '15', 'Median: 15h',
      barPct(m.dev_time_h, 8, 31), 30, m.dev_time_h <= 15 ? 'good' : m.dev_time_h <= 22 ? 'warn' : 'bad', 'cycle-time'),
    bm(period, 'task_delivery', 'task_reopen_rate', 'Task Reopen Rate', 'Jira \u00b7 closed then reopened within 14 days \u00b7 lower = better',
      '4', '%', '0%', '15%', '5', 'Median: 5%', 27, 33, 'good', 'task-reopen'),
    bm(period, 'task_delivery', 'due_date_compliance', 'Due Date Compliance', 'Jira \u00b7 tasks closed by due date',
      String(Math.min(100, Math.round(60 + (build ?? 80) / 4))), '%', '40%', '100%', '72', 'Median: 72%',
      barPct(60 + (build ?? 80) / 4, 40, 100), 53, 'good', ''),
    // git_output
    bm(period, 'git_output', 'commits', 'Commits Created', 'Bitbucket \u00b7 commits authored',
      String(commits), 'count', '2', '55', '22', 'Median: 22',
      barPct(commits, 2, 55), 36, commits >= 22 ? 'good' : commits >= 10 ? 'warn' : 'bad', 'commits'),
    bm(period, 'git_output', 'prs_created', 'Pull Requests Created', 'Bitbucket \u00b7 PRs authored',
      String(prs), 'count', '0', '14', '6', 'Median: 6',
      barPct(prs, 0, 14), 43, prs >= 6 ? 'good' : prs >= 3 ? 'warn' : 'bad', 'pull-requests'),
    bm(period, 'git_output', 'prs_merged', 'Pull Requests Merged', 'Bitbucket \u00b7 authored and merged',
      String(Math.max(0, prs - 1)), 'count', '0', '14', '6', 'Median: 6',
      barPct(Math.max(0, prs - 1), 0, 14), 43, 'good', ''),
    // code_quality
    bm(period, 'code_quality', 'reviews_given', 'Reviews Given', 'Bitbucket \u00b7 PRs reviewed',
      String(reviews), 'count', '0', '20', '8', 'Median: 8',
      barPct(reviews, 0, 20), 40, reviews >= 8 ? 'good' : 'warn', 'reviews'),
    bm(period, 'code_quality', 'build_success', 'Build Success Rate', 'CI \u00b7 passed \u00f7 total runs \u00b7 target \u226590%',
      build !== null ? String(build) : '\u2014', '%', '78%', '100%', '87', 'Median: 87%',
      build !== null ? barPct(build, 78, 100) : 0, 41,
      build !== null ? (build >= 90 ? 'good' : build >= 80 ? 'warn' : 'bad') : 'warn', 'builds'),
    bm(period, 'code_quality', 'bugs_fixed', 'Bugs Fixed', 'Jira \u00b7 bug-type issues closed',
      String(bugs), 'count', '0', '30', '9', 'Median: 9',
      barPct(bugs, 0, 30), 30, 'good', 'bugs-fixed'),
    bm(period, 'code_quality', 'pr_cycle_time', 'PR Cycle Time', 'Bitbucket \u00b7 PR opened \u2192 merged \u00b7 lower = better',
      String(m.dev_time_h), 'h', '0h', '72h', '24', 'Median: 24h',
      barPct(m.dev_time_h, 0, 72), 33, m.dev_time_h <= 18 ? 'good' : m.dev_time_h <= 26 ? 'warn' : 'bad', 'pull-requests'),
    // collab — Slack
    bm(period, 'collab', 'slack_thread_participation', 'Thread Participation', 'Slack \u00b7 replies to others\' threads',
      String(Math.round(20 + pid * 3)), 'replies', '0', '80', '29', 'Median: 29',
      barPct(20 + pid * 3, 0, 80), 36, 'good', ''),
    bm(period, 'collab', 'slack_message_engagement', 'Message Engagement', 'Slack \u00b7 avg replies per thread',
      (1.4 + pid * 0.15).toFixed(1), 'avg', '0', '5', '1.8', 'Median: 1.8',
      barPct(1.4 + pid * 0.15, 0, 5), 36, 'good', ''),
    bm(period, 'collab', 'slack_dm_ratio', 'DM Ratio', 'Slack \u00b7 DMs \u00f7 all messages \u00b7 lower = more open',
      String(Math.round(18 + pid * 2)), '%', '0%', '100%', '28', 'Median: 28% \u00b7 lower = more open',
      Math.round(18 + pid * 2), 28, 'good', ''),
    // collab — M365
    bm(period, 'collab', 'm365_teams_messages', 'Teams Messages', 'Microsoft Teams \u00b7 all channels sent',
      String(Math.round(120 + pid * 12)), '/mo', '0', '400', '148', 'Median: 148',
      barPct(120 + pid * 12, 0, 400), 37, 'good', ''),
    bm(period, 'collab', 'm365_emails_sent', 'Emails Sent', 'M365 \u00b7 avg per member \u00b7 lower = better',
      String(Math.round(22 + pid * 3)), '/mo', '0', '120', '35', 'Median: 35 \u00b7 lower = better',
      barPct(22 + pid * 3, 0, 120), 29, 'good', ''),
    bm(period, 'collab', 'm365_files_shared', 'Files Shared', 'M365 \u00b7 avg per member',
      String(Math.round(5 + pid)), '/mo', '0', '30', '8', 'Median: 8',
      barPct(5 + pid, 0, 30), 27, 'good', ''),
    // collab — Focus & Meetings
    bm(period, 'collab', 'focus_time_pct', 'Focus Time', 'Calendar / M365 \u00b7 uninterrupted blocks \u226560 min',
      String(focus), '%', '42%', '81%', '63', 'Median: 63%',
      barPct(focus, 42, 81), 54, focus >= 60 ? 'good' : focus >= 50 ? 'warn' : 'bad', ''),
    bm(period, 'collab', 'meeting_hours', 'Meeting Hours', 'Zoom \u00b7 meeting duration + M365 audioDuration \u00b7 lower = more focus time',
      String(Math.round(40 - focus / 5)), 'h/mo', '4h', '28h', '16', 'Median: 16h',
      barPct(40 - focus / 5, 4, 28), 50, focus >= 60 ? 'good' : 'warn', ''),
    bm(period, 'collab', 'zoom_calls', 'Zoom Calls', 'Zoom API \u00b7 avg calls attended per member',
      String(Math.round(7 + pid * 0.8)), '/mo', '0', '20', '9', 'Median: 9',
      barPct(7 + pid * 0.8, 0, 20), 45, 'good', ''),
    bm(period, 'collab', 'meeting_free', 'Meeting-Free Days', 'Zoom \u00b7 days with no meetings + M365 \u00b7 avg \u00b7 higher = better',
      String(Math.round(3 + focus / 20)), 'days', '0', '10', '4', 'Median: 4',
      barPct(3 + focus / 20, 0, 10), 40, focus >= 60 ? 'good' : 'warn', ''),
    // ai_tools — only if member uses AI
    ...(m.ai_tools.length > 0 ? [
      bm(period, 'ai_tools', 'cursor_completions', 'Cursor Completions', 'Cursor \u00b7 completions suggested this month',
        String(Math.round(600 + pid * 120)), 'count', '200', '5k', '800', 'Median: 800',
        barPct(600 + pid * 120, 200, 5000), 16, 'good', ''),
      bm(period, 'ai_tools', 'cursor_agents', 'Cursor Agent Sessions', 'Cursor \u00b7 agentic sessions started',
        String(Math.round(8 + pid * 2)), 'count', '2', '40', '10', 'Median: 10',
        barPct(8 + pid * 2, 2, 40), 25, 'good', ''),
      bm(period, 'ai_tools', 'cursor_lines', 'Lines Accepted', 'Cursor \u00b7 lines of code accepted',
        String(Math.round(1200 + pid * 400)), 'count', '0', '5k', '1.8k', 'Median: 1.8k',
        barPct(1200 + pid * 400, 0, 5000), 36, 'good', ''),
      bm(period, 'ai_tools', 'cc_sessions', 'Claude Code Sessions', 'Anthropic Enterprise API \u00b7 sessions this month',
        String(Math.round(10 + pid * 3)), 'count', '0', '60', '12', 'Median: 12',
        barPct(10 + pid * 3, 0, 60), 20, 'good', ''),
      bm(period, 'ai_tools', 'cc_tool_accept', 'Tool Acceptance Rate', 'Anthropic Enterprise API \u00b7 accepted \u00f7 offered',
        String(Math.round(60 + pid * 3)), '%', '0%', '100%', '64', 'Median: 64%',
        Math.round(60 + pid * 3), 64, 'good', ''),
      bm(period, 'ai_tools', 'cc_lines', 'Lines Added (Claude Code)', 'Anthropic Enterprise API \u00b7 lines added by Claude Code',
        String(Math.round(400 + pid * 120)), 'count', '0', '3k', '600', 'Median: 600',
        barPct(400 + pid * 120, 0, 3000), 20, 'good', ''),
      bm(period, 'ai_tools', 'ai_loc_share2', 'AI LOC Share', 'Cursor + Claude Code \u00b7 accepted lines \u00f7 clean LOC',
        String(aiLoc), '%', '0%', '34%', '18', 'Median: 18%',
        barPct(aiLoc, 0, 34), 53, aiLoc >= 18 ? 'good' : aiLoc >= 8 ? 'warn' : 'bad', ''),
      bm(period, 'ai_tools', 'claude_web', 'Claude Web Usage', 'Claude Web \u00b7 conversations this month',
        String(Math.round(14 + pid * 4)), 'count', '0', '80', '18', 'Median: 18',
        barPct(14 + pid * 4, 0, 80), 23, 'good', ''),
      bm(period, 'ai_tools', 'chatgpt', 'ChatGPT Usage', 'ChatGPT \u00b7 conversations this month',
        String(Math.round(4 + pid * 2)), 'count', '0', '40', '12', 'Median: 12',
        barPct(4 + pid * 2, 0, 40), 30, 'good', ''),
    ] : []),
  ];

  // -- Charts --
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

  // -- Drills — row counts MUST match value field --
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
    'Cycle Time': i === prs - 1 && prs > 1 ? '\u2014' : `${m.dev_time_h + ([-4, -2, 0, 2, 4][i % 5] ?? 0)}h`,
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
      title: 'Build Results', source: 'CI', srcClass: 'bg-gray-600',
      value: build !== null ? `${build}%` : '\u2014',
      filter: build !== null ? `author = ${login} AND build.date >= 2026-03-01` : 'CI connector not configured',
      columns: ['Build', 'Branch', 'Status', 'Duration'],
      rows: build !== null ? [
        { Build: `build-${1000 + pid * 10 + 2}`, Branch: 'feat/main-feature', Status: 'Passed',                       Duration: '3m 12s' },
        { Build: `build-${1000 + pid * 10 + 1}`, Branch: 'feat/main-feature', Status: build < 90 ? 'Failed' : 'Passed', Duration: '2m 08s' },
        { Build: `build-${1000 + pid * 10}`,     Branch: 'fix/bug-fix',        Status: 'Passed',                       Duration: '3m 01s' },
      ] : [],
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
    data_availability: EXEC_DATA_AVAILABILITY,
  };
}

// Pre-compute all IC mocks (12 members x 4 periods).
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
    IC_MOCK_ENTRIES[`GET /api/v1/analytics/views/ic/${member.person_id}?period=${p}`] = () => data ?? null;
  }
  // Drills are period-agnostic — use month data as source of truth
  const monthData = ALL_IC[`${member.person_id}/month`];
  if (monthData) {
    for (const drillId of Object.keys(monthData.drills)) {
      const drill = monthData.drills[drillId];
      IC_DRILL_ENTRIES[`GET /api/v1/analytics/views/ic/${member.person_id}/drill/${drillId}`] =
        () => drill ?? null;
    }
  }
}

// ---------------------------------------------------------------------------
// Mock map
// ---------------------------------------------------------------------------

/**
 * Mock map for insight API service
 * Period is embedded in the URL path so the mock plugin can route correctly.
 */
export const insightMockMap = {
  'GET /api/v1/analytics/dashboard': (): DashboardData => mockDashboardData,
  'GET /api/v1/analytics/speed': (): SpeedData => mockSpeedData,
  // Executive View — one entry per period
  'GET /api/v1/analytics/views/executive?period=week':    (): ExecViewData => EXEC_VIEW_MOCK['week'],
  'GET /api/v1/analytics/views/executive?period=month':   (): ExecViewData => EXEC_VIEW_MOCK['month'],
  'GET /api/v1/analytics/views/executive?period=quarter': (): ExecViewData => EXEC_VIEW_MOCK['quarter'],
  'GET /api/v1/analytics/views/executive?period=year':    (): ExecViewData => EXEC_VIEW_MOCK['year'],
  // Team View — one entry per teamId + period
  ...Object.entries(TEAM_VIEW_MOCKS).reduce<Record<string, () => TeamViewData>>((acc, [tid, periods]) => {
    for (const p of ['week', 'month', 'quarter', 'year'] as PeriodValue[]) {
      acc[`GET /api/v1/analytics/views/team/${tid}?period=${p}`] = (): TeamViewData => periods[p];
    }
    return acc;
  }, {}),
  // IC Dashboard — flat paths per personId + period (wildcards ignored by mock framework)
  ...IC_MOCK_ENTRIES,
  // Team View drills — one entry per drill ID per period
  'GET /api/v1/analytics/views/team/drill/team-tasks?period=week':    (): DrillData => TEAM_DRILLS['team-tasks']!,
  'GET /api/v1/analytics/views/team/drill/team-tasks?period=month':   (): DrillData => TEAM_DRILLS['team-tasks']!,
  'GET /api/v1/analytics/views/team/drill/team-tasks?period=quarter': (): DrillData => TEAM_DRILLS['team-tasks']!,
  'GET /api/v1/analytics/views/team/drill/team-tasks?period=year':    (): DrillData => TEAM_DRILLS['team-tasks']!,
  'GET /api/v1/analytics/views/team/drill/team-dev-time?period=week':    (): DrillData => TEAM_DRILLS['team-dev-time']!,
  'GET /api/v1/analytics/views/team/drill/team-dev-time?period=month':   (): DrillData => TEAM_DRILLS['team-dev-time']!,
  'GET /api/v1/analytics/views/team/drill/team-dev-time?period=quarter': (): DrillData => TEAM_DRILLS['team-dev-time']!,
  'GET /api/v1/analytics/views/team/drill/team-dev-time?period=year':    (): DrillData => TEAM_DRILLS['team-dev-time']!,
  'GET /api/v1/analytics/views/team/drill/team-build?period=week':    (): DrillData => TEAM_DRILLS['team-build']!,
  'GET /api/v1/analytics/views/team/drill/team-build?period=month':   (): DrillData => TEAM_DRILLS['team-build']!,
  'GET /api/v1/analytics/views/team/drill/team-build?period=quarter': (): DrillData => TEAM_DRILLS['team-build']!,
  'GET /api/v1/analytics/views/team/drill/team-build?period=year':    (): DrillData => TEAM_DRILLS['team-build']!,
  'GET /api/v1/analytics/views/team/drill/team-prs?period=week':    (): DrillData => TEAM_DRILLS['team-prs']!,
  'GET /api/v1/analytics/views/team/drill/team-prs?period=month':   (): DrillData => TEAM_DRILLS['team-prs']!,
  'GET /api/v1/analytics/views/team/drill/team-prs?period=quarter': (): DrillData => TEAM_DRILLS['team-prs']!,
  'GET /api/v1/analytics/views/team/drill/team-prs?period=year':    (): DrillData => TEAM_DRILLS['team-prs']!,
  'GET /api/v1/analytics/views/team/drill/team-pr-cycle?period=week':    (): DrillData => TEAM_DRILLS['team-pr-cycle']!,
  'GET /api/v1/analytics/views/team/drill/team-pr-cycle?period=month':   (): DrillData => TEAM_DRILLS['team-pr-cycle']!,
  'GET /api/v1/analytics/views/team/drill/team-pr-cycle?period=quarter': (): DrillData => TEAM_DRILLS['team-pr-cycle']!,
  'GET /api/v1/analytics/views/team/drill/team-pr-cycle?period=year':    (): DrillData => TEAM_DRILLS['team-pr-cycle']!,
  'GET /api/v1/analytics/views/team/drill/team-bugs?period=week':    (): DrillData => TEAM_DRILLS['team-bugs']!,
  'GET /api/v1/analytics/views/team/drill/team-bugs?period=month':   (): DrillData => TEAM_DRILLS['team-bugs']!,
  'GET /api/v1/analytics/views/team/drill/team-bugs?period=quarter': (): DrillData => TEAM_DRILLS['team-bugs']!,
  'GET /api/v1/analytics/views/team/drill/team-bugs?period=year':    (): DrillData => TEAM_DRILLS['team-bugs']!,
  'GET /api/v1/analytics/views/team/drill/team-focus?period=week':    (): DrillData => TEAM_DRILLS['team-focus']!,
  'GET /api/v1/analytics/views/team/drill/team-focus?period=month':   (): DrillData => TEAM_DRILLS['team-focus']!,
  'GET /api/v1/analytics/views/team/drill/team-focus?period=quarter': (): DrillData => TEAM_DRILLS['team-focus']!,
  'GET /api/v1/analytics/views/team/drill/team-focus?period=year':    (): DrillData => TEAM_DRILLS['team-focus']!,
  'GET /api/v1/analytics/views/team/drill/team-ai-active?period=week':    (): DrillData => TEAM_DRILLS['team-ai-active']!,
  'GET /api/v1/analytics/views/team/drill/team-ai-active?period=month':   (): DrillData => TEAM_DRILLS['team-ai-active']!,
  'GET /api/v1/analytics/views/team/drill/team-ai-active?period=quarter': (): DrillData => TEAM_DRILLS['team-ai-active']!,
  'GET /api/v1/analytics/views/team/drill/team-ai-active?period=year':    (): DrillData => TEAM_DRILLS['team-ai-active']!,
  'GET /api/v1/analytics/views/team/drill/team-ai-loc?period=week':    (): DrillData => TEAM_DRILLS['team-ai-loc']!,
  'GET /api/v1/analytics/views/team/drill/team-ai-loc?period=month':   (): DrillData => TEAM_DRILLS['team-ai-loc']!,
  'GET /api/v1/analytics/views/team/drill/team-ai-loc?period=quarter': (): DrillData => TEAM_DRILLS['team-ai-loc']!,
  'GET /api/v1/analytics/views/team/drill/team-ai-loc?period=year':    (): DrillData => TEAM_DRILLS['team-ai-loc']!,
  'GET /api/v1/analytics/views/team/drill/team-reopen?period=week':    (): DrillData => TEAM_DRILLS['team-reopen']!,
  'GET /api/v1/analytics/views/team/drill/team-reopen?period=month':   (): DrillData => TEAM_DRILLS['team-reopen']!,
  'GET /api/v1/analytics/views/team/drill/team-reopen?period=quarter': (): DrillData => TEAM_DRILLS['team-reopen']!,
  'GET /api/v1/analytics/views/team/drill/team-reopen?period=year':    (): DrillData => TEAM_DRILLS['team-reopen']!,
  'GET /api/v1/analytics/views/team/drill/team-members?period=week':    (): DrillData => TEAM_DRILLS['team-members']!,
  'GET /api/v1/analytics/views/team/drill/team-members?period=month':   (): DrillData => TEAM_DRILLS['team-members']!,
  'GET /api/v1/analytics/views/team/drill/team-members?period=quarter': (): DrillData => TEAM_DRILLS['team-members']!,
  'GET /api/v1/analytics/views/team/drill/team-members?period=year':    (): DrillData => TEAM_DRILLS['team-members']!,
  // IC Dashboard drills — flat paths per personId + drillId
  ...IC_DRILL_ENTRIES,
} satisfies MockMap;

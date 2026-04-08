/**
 * Executive View — API contract reference
 *
 * Reference API response for:
 *   GET /views/executive?period=month
 *
 * This file defines the exact JSON shape the backend must return.
 * Do not modify field names or structure without updating the API contract.
 */

import type { ExecTeamRow, OrgKpis, ExecViewConfig } from '../../../types';

export const EXEC_TEAMS_MONTH: ExecTeamRow[] = [
  { team_id: 'platform', team_name: 'Platform', headcount: 12, tasks_closed: 48, bugs_fixed: 18, build_success_pct: 94, focus_time_pct: 72, ai_adoption_pct: 68, ai_loc_share_pct: 22, pr_cycle_time_h: 18, status: 'good' },
  { team_id: 'mobile', team_name: 'Mobile', headcount: 8, tasks_closed: 29, bugs_fixed: 11, build_success_pct: 87, focus_time_pct: 54, ai_adoption_pct: 42, ai_loc_share_pct: 12, pr_cycle_time_h: 26, status: 'warn' },
  { team_id: 'data', team_name: 'Data', headcount: 6, tasks_closed: 22, bugs_fixed: 8, build_success_pct: 91, focus_time_pct: 65, ai_adoption_pct: 71, ai_loc_share_pct: 28, pr_cycle_time_h: 20, status: 'good' },
  { team_id: 'frontend', team_name: 'Frontend', headcount: 10, tasks_closed: 38, bugs_fixed: 14, build_success_pct: 82, focus_time_pct: 48, ai_adoption_pct: 35, ai_loc_share_pct: 9, pr_cycle_time_h: 31, status: 'bad' },
  { team_id: 'devops', team_name: 'DevOps', headcount: 5, tasks_closed: 19, bugs_fixed: 7, build_success_pct: 96, focus_time_pct: 78, ai_adoption_pct: 80, ai_loc_share_pct: 31, pr_cycle_time_h: 14, status: 'good' },
  { team_id: 'qa', team_name: 'QA', headcount: 7, tasks_closed: 25, bugs_fixed: 21, build_success_pct: 88, focus_time_pct: 61, ai_adoption_pct: 55, ai_loc_share_pct: 18, pr_cycle_time_h: 22, status: 'warn' },
];

export const EXEC_ORG_KPIS_MONTH: OrgKpis = {
  avgBuildSuccess: 90,
  avgAiAdoption: 59,
  avgFocus: 63,
  bugResolutionScore: 78,
  prCycleScore: 65,
};

export const EXEC_VIEW_CONFIG: ExecViewConfig = {
  column_thresholds: [
    { metric_key: 'build_success_pct', threshold: 90 },
    { metric_key: 'focus_time_pct',    threshold: 60 },
    { metric_key: 'ai_adoption_pct',   threshold: 60 },
  ],
};

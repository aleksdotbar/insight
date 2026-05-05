/**
 * insightMockMap — wires factories.ts mock builders into the URL routes
 * the analytics-api gateway exposes. Consumed by `InsightApiService`
 * when `VITE_ENABLE_MOCKS=true`.
 *
 * Handlers parse OData `$filter` from the POST body so that per-team and
 * per-person drilldowns return scoped data (otherwise navigating into
 * a different team / person renders the default persona's numbers
 * everywhere — silently wrong).
 *
 * Keys are full request paths; method prefix is required. We register
 * both absolute (`/api/analytics/v1/metrics/...`) and baseURL-relative
 * (`/metrics/...`) variants because hai3's `RestMockPlugin` matches
 * against `context.url`, which can be either form depending on axios
 * configuration.
 */

import type { MockMap } from '@hai3/react';
import { METRIC_REGISTRY } from '../metricRegistry';
import {
  mockExecRows,
  mockTeamMemberRow,
  mockTeamMemberRows,
  mockTeamMemberRowsForTeam,
  mockTeamBulletSection,
  mockIcAggregateRow,
  mockIcBulletSection,
  mockLocTrendSeries,
  mockDeliveryTrendSeries,
} from './factories';
import { PEOPLE, PEOPLE_BY_ID } from './registry';

const wrap = <T>(items: T[]) => ({
  items,
  page_info: { has_next: false, cursor: null },
});

const defaultPersonId = PEOPLE[0]?.person_id ?? 'bob.park@example.com';

// ---------------------------------------------------------------------------
// OData $filter parsing
// ---------------------------------------------------------------------------

type ODataBody = { $filter?: string };

const parseFilter = (body: unknown): { personId?: string; teamId?: string } => {
  const f = (body as ODataBody | undefined)?.$filter ?? '';
  // The analytics-api gateway accepts standard OData equality predicates.
  // We extract the two scopes the FE actually sends — `org_unit_id eq '<id>'`
  // (team-view) and `person_id eq '<email>'` (IC drill / member roster).
  const teamMatch   = /\borg_unit_id\s+eq\s+'([^']+)'/i.exec(f);
  const personMatch = /\bperson_id\s+eq\s+'([^']+)'/i.exec(f);
  return { teamId: teamMatch?.[1], personId: personMatch?.[1] };
};

// Stable hash so each persona's bullets vary deterministically without
// needing per-person hard-coded values.
const seedOf = (s: string | undefined): number => {
  if (!s) return 0;
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) | 0;
  return Math.abs(h);
};

// ---------------------------------------------------------------------------
// Per-metric handlers (body-aware)
// ---------------------------------------------------------------------------

type Handler = MockMap[string];

const handlers: Record<string, Handler> = {
  // Executive: org-wide aggregate, no scoping.
  [METRIC_REGISTRY.EXEC_SUMMARY]: () => wrap(mockExecRows()),

  // Team members: roster-mode sends `person_id eq '<email>'` per person; the
  // legacy bulk path sends `org_unit_id eq '<team>'`.
  [METRIC_REGISTRY.TEAM_MEMBER]: (body) => {
    const { personId, teamId } = parseFilter(body);
    if (personId) {
      const p = PEOPLE_BY_ID[personId.toLowerCase()] ?? PEOPLE_BY_ID[personId];
      if (!p) return wrap([]);
      const i = seedOf(p.person_id) % 7;
      return wrap([mockTeamMemberRow({
        person_id: p.person_id,
        display_name: p.name,
        seniority: p.seniority,
        ai_tools: p.ai_tools,
        tasks_closed: Math.max(1, 5 + (i % 9)),
        bugs_fixed: Math.max(0, 1 + (i % 5)),
        dev_time_h: Math.max(8, 10 + (i % 12)),
        prs_merged: Math.max(1, 3 + (i % 7)),
        build_success_pct: 85 + (i % 12),
        focus_time_pct: 50 + (i % 35),
        ai_loc_share_pct: p.ai_tools.length > 0 ? 12 + (i % 18) : 0,
      })]);
    }
    if (teamId) return wrap(mockTeamMemberRowsForTeam(teamId));
    return wrap(mockTeamMemberRows());
  },

  // Team bullet sections: scoped by team_id seed so bars subtly vary
  // when navigating between teams (matches user expectation that "QA"
  // and "Backend" don't show identical numbers).
  [METRIC_REGISTRY.TEAM_BULLET_DELIVERY]: (body) => {
    const { teamId } = parseFilter(body);
    return wrap(mockTeamBulletSection('task_delivery', seedOf(teamId)));
  },
  [METRIC_REGISTRY.TEAM_BULLET_QUALITY]: (body) => {
    const { teamId } = parseFilter(body);
    return wrap(mockTeamBulletSection('code_quality', seedOf(teamId)));
  },
  [METRIC_REGISTRY.TEAM_BULLET_COLLAB]: (body) => {
    const { teamId } = parseFilter(body);
    return wrap(mockTeamBulletSection('collaboration', seedOf(teamId)));
  },
  [METRIC_REGISTRY.TEAM_BULLET_AI]: (body) => {
    const { teamId } = parseFilter(body);
    return wrap(mockTeamBulletSection('ai_adoption', seedOf(teamId)));
  },

  // IC dashboard: scoped by person_id so each member's drill shows their
  // own deterministic figures.
  [METRIC_REGISTRY.IC_KPIS]: (body) => {
    const { personId } = parseFilter(body);
    return wrap([mockIcAggregateRow({ person_id: personId ?? defaultPersonId })]);
  },
  [METRIC_REGISTRY.IC_BULLET_DELIVERY]: (body) => {
    const { personId } = parseFilter(body);
    return wrap(mockIcBulletSection('task_delivery', seedOf(personId)));
  },
  [METRIC_REGISTRY.IC_BULLET_COLLAB]: (body) => {
    const { personId } = parseFilter(body);
    return wrap(mockIcBulletSection('collab', seedOf(personId)));
  },
  [METRIC_REGISTRY.IC_BULLET_AI]: (body) => {
    const { personId } = parseFilter(body);
    return wrap(mockIcBulletSection('ai_tools', seedOf(personId)));
  },
  [METRIC_REGISTRY.IC_BULLET_GIT]: (body) => {
    const { personId } = parseFilter(body);
    return wrap(mockIcBulletSection('git_output', seedOf(personId)));
  },
  [METRIC_REGISTRY.IC_CHART_LOC]:      () => wrap(mockLocTrendSeries(8)),
  [METRIC_REGISTRY.IC_CHART_DELIVERY]: () => wrap(mockDeliveryTrendSeries(8)),
  [METRIC_REGISTRY.IC_DRILL]:          () => wrap([]),
  [METRIC_REGISTRY.IC_TIMEOFF]:        () => wrap([]),
};

const map: MockMap = {};
for (const [id, fn] of Object.entries(handlers)) {
  map[`POST /api/analytics/v1/metrics/${id}/query`] = fn;
  map[`POST /metrics/${id}/query`] = fn;
}

export const insightMockMap: MockMap = map;

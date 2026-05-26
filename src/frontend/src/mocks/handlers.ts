import { http, HttpResponse } from "msw";

import { METRIC_REGISTRY } from "@/api/metric-registry";

import {
  mockCrmBulletSection,
  mockCrmFlowSeries,
  mockCrmKpis,
  mockDeliveryTrendSeries,
  mockExecRows,
  mockIcAggregateRow,
  mockIcBulletSection,
  mockLocTrendSeries,
  mockTeamBulletSection,
  mockTeamMemberRow,
  mockTeamMemberRows,
  mockTeamMemberRowsForTeam,
} from "./factories";
import { buildIdentityTree, PEOPLE, PEOPLE_BY_ID } from "./registry";

const wrap = <T>(items: T[]) => ({
  items,
  page_info: { has_next: false, cursor: null as string | null },
});

const defaultPersonId = PEOPLE[0]?.person_id ?? "bob.park@example.com";

type ODataBody = { $filter?: string };

function parseFilter(body: unknown): { personId?: string; teamId?: string } {
  const f = (body as ODataBody | undefined)?.$filter ?? "";
  const teamMatch = /\borg_unit_id\s+eq\s+'([^']+)'/i.exec(f);
  const personMatch = /\bperson_id\s+eq\s+'([^']+)'/i.exec(f);
  return { teamId: teamMatch?.[1], personId: personMatch?.[1] };
}

function seedOf(s: string | undefined): number {
  if (!s) return 0;
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) | 0;
  return Math.abs(h);
}

type Handler = (body: unknown) => Record<string, unknown> | unknown[];

const metricHandlers: Record<string, Handler> = {
  [METRIC_REGISTRY.EXEC_SUMMARY]: () => wrap(mockExecRows()),

  [METRIC_REGISTRY.TEAM_MEMBER]: (body) => {
    const { personId, teamId } = parseFilter(body);
    if (personId) {
      const p =
        PEOPLE_BY_ID[personId.toLowerCase()] ?? PEOPLE_BY_ID[personId];
      if (!p) return wrap([]);
      const i = seedOf(p.person_id) % 7;
      return wrap([
        mockTeamMemberRow({
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
        }),
      ]);
    }
    if (teamId) return wrap(mockTeamMemberRowsForTeam(teamId));
    return wrap(mockTeamMemberRows());
  },

  [METRIC_REGISTRY.TEAM_BULLET_DELIVERY]: (body) => {
    const { teamId } = parseFilter(body);
    return wrap(mockTeamBulletSection("task_delivery", seedOf(teamId)));
  },
  [METRIC_REGISTRY.TEAM_BULLET_QUALITY]: (body) => {
    const { teamId } = parseFilter(body);
    return wrap(mockTeamBulletSection("code_quality", seedOf(teamId)));
  },
  [METRIC_REGISTRY.TEAM_BULLET_COLLAB]: (body) => {
    const { teamId } = parseFilter(body);
    return wrap(mockTeamBulletSection("collaboration", seedOf(teamId)));
  },
  [METRIC_REGISTRY.TEAM_BULLET_AI]: (body) => {
    const { teamId } = parseFilter(body);
    return wrap(mockTeamBulletSection("ai_adoption", seedOf(teamId)));
  },

  [METRIC_REGISTRY.IC_KPIS]: (body) => {
    const { personId } = parseFilter(body);
    return wrap([
      mockIcAggregateRow({ person_id: personId ?? defaultPersonId }),
    ]);
  },
  [METRIC_REGISTRY.IC_BULLET_DELIVERY]: (body) => {
    const { personId } = parseFilter(body);
    return wrap(mockIcBulletSection("task_delivery", seedOf(personId)));
  },
  [METRIC_REGISTRY.IC_BULLET_COLLAB]: (body) => {
    const { personId } = parseFilter(body);
    return wrap(mockIcBulletSection("collab", seedOf(personId)));
  },
  [METRIC_REGISTRY.IC_BULLET_AI]: (body) => {
    const { personId } = parseFilter(body);
    return wrap(mockIcBulletSection("ai_tools", seedOf(personId)));
  },
  [METRIC_REGISTRY.IC_BULLET_GIT]: (body) => {
    const { personId } = parseFilter(body);
    return wrap(mockIcBulletSection("git_output", seedOf(personId)));
  },
  [METRIC_REGISTRY.IC_CHART_LOC]: () => wrap(mockLocTrendSeries(8)),
  [METRIC_REGISTRY.IC_CHART_DELIVERY]: () => wrap(mockDeliveryTrendSeries(8)),
  [METRIC_REGISTRY.IC_DRILL]: () => wrap([]),
  [METRIC_REGISTRY.IC_TIMEOFF]: () => wrap([]),

  [METRIC_REGISTRY.CRM_KPIS]: (body) => {
    const { personId } = parseFilter(body);
    return wrap([mockCrmKpis(personId ?? defaultPersonId)]);
  },
  [METRIC_REGISTRY.CRM_CHART_FLOW]: (body) => {
    const { personId } = parseFilter(body);
    return wrap(mockCrmFlowSeries(8, personId ?? defaultPersonId));
  },
  [METRIC_REGISTRY.CRM_BULLET_QUALITY]: (body) => {
    const { personId } = parseFilter(body);
    return wrap(mockCrmBulletSection("quality", personId ?? defaultPersonId));
  },
  [METRIC_REGISTRY.CRM_BULLET_ACTIVITY]: (body) => {
    const { personId } = parseFilter(body);
    return wrap(mockCrmBulletSection("activity", personId ?? defaultPersonId));
  },
};

export const handlers = [
  http.post(
    "/api/analytics/v1/metrics/:metricId/query",
    async ({ params, request }) => {
      const metricId = params.metricId as string;
      const handler = metricHandlers[metricId];
      if (!handler) return HttpResponse.json(wrap([]));
      const body = await request.json().catch(() => ({}));
      return HttpResponse.json(handler(body));
    },
  ),
  http.get(
    "/api/identity/v1/persons/:email",
    ({ params }) => {
      const email = decodeURIComponent(params.email as string);
      const tree = buildIdentityTree(email);
      if (!tree) {
        return HttpResponse.json(
          { type: "urn:insight:error:person_not_found" },
          { status: 404 },
        );
      }
      return HttpResponse.json(tree);
    },
  ),
];

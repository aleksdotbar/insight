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
import {
  mockHistogramBins,
  mockKpiPeerMedians,
  mockPeerCohortStats,
  mockSectionTrend,
} from "./v2/factories";

const wrap = <T>(items: T[]) => ({
  items,
  page_info: { has_next: false, cursor: null as string | null },
});

const defaultPersonId = PEOPLE[0]?.person_id ?? "bob.park@example.com";

type ODataBody = { $filter?: string };

function parseFilter(body: unknown): {
  personId?: string;
  teamId?: string;
  metricKey?: string;
  kind?: string;
  sectionId?: string;
  cohortSeed?: string;
  periodDays: number;
  dateFrom?: string;
  dateTo?: string;
} {
  const f = (body as ODataBody | undefined)?.$filter ?? "";
  const teamMatch = /\borg_unit_id\s+eq\s+'([^']+)'/i.exec(f);
  const personMatch = /\bperson_id\s+eq\s+'([^']+)'/i.exec(f);
  const metricMatch = /\bmetric_key\s+eq\s+'([^']+)'/i.exec(f);
  const kindMatch = /\bkind\s+eq\s+'([^']+)'/i.exec(f);
  const sectionMatch = /\bsection_id\s+eq\s+'([^']+)'/i.exec(f);
  const cohortMatch = /\bcohort_seed\s+eq\s+'([^']+)'/i.exec(f);
  const periodDaysMatch = /\bperiod_days\s+eq\s+(\d+)/i.exec(f);
  const dateFromMatch = /\bmetric_date\s+ge\s+'(\d{4}-\d{2}-\d{2})'/i.exec(f);
  const dateToMatch = /\bmetric_date\s+l[et]\s+'(\d{4}-\d{2}-\d{2})'/i.exec(f);
  const dateFrom = dateFromMatch?.[1];
  const dateTo = dateToMatch?.[1];
  let periodDays = 30;
  if (periodDaysMatch) {
    periodDays = Number(periodDaysMatch[1]);
  } else if (dateFrom && dateTo) {
    const from = new Date(dateFrom).getTime();
    const to = new Date(dateTo).getTime();
    periodDays = Math.max(
      1,
      Math.round((to - from) / 86_400_000) + 1,
    );
  }
  return {
    teamId: teamMatch?.[1],
    personId: personMatch?.[1],
    metricKey: metricMatch?.[1],
    kind: kindMatch?.[1],
    sectionId: sectionMatch?.[1],
    cohortSeed: cohortMatch?.[1],
    periodDays,
    dateFrom,
    dateTo,
  };
}

function periodScale(periodDays: number): number {
  return periodDays / 30;
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
    const { personId, teamId, periodDays } = parseFilter(body);
    const scale = periodScale(periodDays);
    if (personId) {
      const p =
        PEOPLE_BY_ID[personId.toLowerCase()] ?? PEOPLE_BY_ID[personId];
      if (!p) return wrap([]);
      const s = seedOf(p.person_id);
      return wrap([
        mockTeamMemberRow({
          person_id: p.person_id,
          display_name: p.name,
          seniority: p.seniority,
          ai_tools: p.ai_tools,
          tasks_closed: Math.max(1, Math.round((3 + (s % 17)) * scale)),
          bugs_fixed: Math.max(0, Math.round(((s % 11) >> 1) * scale)),
          dev_time_h: Math.max(8, Math.round((10 + ((s >> 3) % 18)) * scale)),
          prs_merged: Math.max(1, Math.round((2 + ((s >> 5) % 11)) * scale)),
          build_success_pct: 72 + ((s >> 7) % 26),
          focus_time_pct: 30 + ((s >> 11) % 55),
          ai_loc_share_pct: p.ai_tools.length > 0 ? 5 + ((s >> 13) % 35) : 0,
        }),
      ]);
    }
    if (teamId) return wrap(mockTeamMemberRowsForTeam(teamId));
    return wrap(mockTeamMemberRows());
  },

  [METRIC_REGISTRY.TEAM_BULLET_DELIVERY]: (body) => {
    const { teamId, periodDays } = parseFilter(body);
    return wrap(
      mockTeamBulletSection("task_delivery", seedOf(teamId), periodDays),
    );
  },
  [METRIC_REGISTRY.TEAM_BULLET_QUALITY]: (body) => {
    const { teamId, periodDays } = parseFilter(body);
    return wrap(
      mockTeamBulletSection("code_quality", seedOf(teamId), periodDays),
    );
  },
  [METRIC_REGISTRY.TEAM_BULLET_COLLAB]: (body) => {
    const { teamId, periodDays } = parseFilter(body);
    return wrap(
      mockTeamBulletSection("collaboration", seedOf(teamId), periodDays),
    );
  },
  [METRIC_REGISTRY.TEAM_BULLET_AI]: (body) => {
    const { teamId, periodDays } = parseFilter(body);
    return wrap(
      mockTeamBulletSection("ai_adoption", seedOf(teamId), periodDays),
    );
  },

  [METRIC_REGISTRY.IC_KPIS]: (body) => {
    const { personId, periodDays } = parseFilter(body);
    const scale = periodScale(periodDays);
    const jitter = seedOf(`${personId ?? "p"}|${periodDays}`) % 1000;
    const ratio = (base: number, range: number) =>
      Math.max(1, Math.min(99, Math.round(base + ((jitter / 1000) - 0.5) * range)));
    return wrap([
      mockIcAggregateRow({
        person_id: personId ?? defaultPersonId,
        loc: Math.round(12000 * scale),
        prs_merged: Math.max(1, Math.round(9 * scale)),
        tasks_closed: Math.max(1, Math.round(12 * scale)),
        bugs_fixed: Math.max(0, Math.round(23 * scale)),
        ai_sessions: Math.max(1, Math.round(42 * scale)),
        ai_loc_share_pct: ratio(22, 18),
        focus_time_pct: ratio(65, 24),
        build_success_pct: ratio(92, 10),
        pr_cycle_time_h: Math.max(4, Math.round(18 + ((jitter / 1000) - 0.5) * 16)),
      }),
    ]);
  },
  [METRIC_REGISTRY.IC_BULLET_DELIVERY]: (body) => {
    const { personId, periodDays } = parseFilter(body);
    return wrap(
      mockIcBulletSection("task_delivery", seedOf(personId), periodDays),
    );
  },
  [METRIC_REGISTRY.IC_BULLET_COLLAB]: (body) => {
    const { personId, periodDays } = parseFilter(body);
    return wrap(mockIcBulletSection("collab", seedOf(personId), periodDays));
  },
  [METRIC_REGISTRY.IC_BULLET_AI]: (body) => {
    const { personId, periodDays } = parseFilter(body);
    return wrap(mockIcBulletSection("ai_tools", seedOf(personId), periodDays));
  },
  [METRIC_REGISTRY.IC_BULLET_GIT]: (body) => {
    const { personId, periodDays } = parseFilter(body);
    return wrap(
      mockIcBulletSection("git_output", seedOf(personId), periodDays),
    );
  },
  [METRIC_REGISTRY.IC_CHART_LOC]: (body) => {
    const { periodDays } = parseFilter(body);
    const weeks = Math.max(2, Math.round(periodDays / 7));
    return wrap(mockLocTrendSeries(weeks));
  },
  [METRIC_REGISTRY.IC_CHART_DELIVERY]: (body) => {
    const { periodDays } = parseFilter(body);
    const weeks = Math.max(2, Math.round(periodDays / 7));
    return wrap(mockDeliveryTrendSeries(weeks));
  },
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

  [METRIC_REGISTRY.V2_IC_HISTOGRAM]: (body) => {
    const { personId, metricKey, periodDays } = parseFilter(body);
    if (!metricKey) return wrap([]);
    return wrap(
      mockHistogramBins(personId ?? defaultPersonId, metricKey, periodDays),
    );
  },
  [METRIC_REGISTRY.V2_PEER_COHORT_STATS]: (body) => {
    const { teamId, kind, periodDays } = parseFilter(body);
    const seed = teamId ?? kind ?? "org";
    return wrap(mockPeerCohortStats(seed, periodDays));
  },
  [METRIC_REGISTRY.V2_IC_SECTION_TREND]: (body) => {
    const { personId, sectionId, periodDays } = parseFilter(body);
    if (!personId || !sectionId) return wrap([]);
    return wrap(mockSectionTrend(personId, sectionId, periodDays ?? 30));
  },
  [METRIC_REGISTRY.V2_IC_KPI_PEER_MEDIAN]: (body) => {
    const { cohortSeed, periodDays } = parseFilter(body);
    return wrap(mockKpiPeerMedians(cohortSeed ?? "supervisor", periodDays));
  },
};

interface BatchQueryRequest {
  queries?: Array<{
    id?: string;
    metric_id: string;
    $filter?: string;
  }>;
}

export const handlers = [
  http.post(
    "/api/analytics/v1/metrics/queries",
    async ({ request }) => {
      const body = (await request.json().catch(() => ({}))) as BatchQueryRequest;
      const queries = body.queries ?? [];
      const results = queries.map((q) => {
        const handler = metricHandlers[q.metric_id];
        const payload = handler
          ? (handler({ $filter: q.$filter }) as { items: unknown[] })
          : { items: [] };
        return {
          status: "ok" as const,
          id: q.id,
          metric_id: q.metric_id,
          items: payload.items,
          page_info: { has_next: false, cursor: null as string | null },
        };
      });
      return HttpResponse.json({ results });
    },
  ),
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

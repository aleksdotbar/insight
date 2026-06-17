import { keepPreviousData, useQuery, type UseQueryResult } from "@tanstack/react-query";

import {
  queryBatchWithRange,
  type BatchQueryResult,
} from "@/api/analytics-client";
import type { CatalogResponse } from "@/api/catalog-client";
import { METRIC_REGISTRY } from "@/api/metric-registry";
import { odataEscapeValue } from "@/api/odata";
import {
  previousPeriodRange,
  type DateRange,
} from "@/api/period-to-date-range";
import type { RawBulletAggregateRow } from "@/api/raw-types";
import { transformBulletMetrics } from "@/api/transforms";
import { useCatalog } from "@/api/use-catalog";
import type { DeptCohorts, DeptStatsMap, PeerStats } from "@/lib/peers";
import type { BulletMetric, PeriodValue } from "@/types/insight";

const SECTION_METRIC_IDS = [
  { sectionId: "task_delivery", metricId: METRIC_REGISTRY.V2_MEMBER_VALUES_DELIVERY },
  { sectionId: "git_output", metricId: METRIC_REGISTRY.V2_MEMBER_VALUES_GIT },
  { sectionId: "collaboration", metricId: METRIC_REGISTRY.V2_MEMBER_VALUES_COLLAB },
  { sectionId: "ai_adoption", metricId: METRIC_REGISTRY.V2_MEMBER_VALUES_AI },
] as const;

/** Per-person long row from a `V2_MEMBER_VALUES_*` metric. */
type RawMemberValueRow = {
  person_id: string;
  metric_key: string;
  value: number | null;
};

/**
 * Fetch every member's per-section metric values in one request per section
 * (`person_id in (roster)`), instead of one request per member × section.
 * The metrics carry no cohort — the heatmap + needs-attention surfaces color
 * client-side from the displayed roster. We feed `transformBulletMetrics` the
 * roster's own min/max per `metric_key` as the bullet range so its
 * hours→days auto-scale makes the same per-metric decision it made off the
 * department cohort, keeping displayed units identical.
 */
async function fetchMemberBullets(
  memberIds: string[],
  range: DateRange,
  period: PeriodValue,
  catalog: CatalogResponse | undefined,
): Promise<Map<string, BulletMetric[]>> {
  if (memberIds.length === 0) return new Map();
  const ids = memberIds
    .map((id) => `'${odataEscapeValue(id.toLowerCase())}'`)
    .join(", ");
  const items = SECTION_METRIC_IDS.map((s) => ({
    id: s.sectionId,
    metric_id: s.metricId,
    $filter: `person_id in (${ids})`,
    $top: 5000,
  }));
  const resp = await queryBatchWithRange<RawMemberValueRow>(range, items);
  const byMember = new Map<string, BulletMetric[]>();
  for (const r of resp.results as BatchQueryResult<RawMemberValueRow>[]) {
    if (!r.id) continue;
    if (r.status !== "ok") {
      throw new Error(`Failed to load ${r.id} member values`);
    }
    const sectionId = r.id;
    const rangeByKey = new Map<string, { min: number; max: number }>();
    for (const row of r.items) {
      const v = Number(row.value);
      if (!Number.isFinite(v)) continue;
      const cur = rangeByKey.get(row.metric_key);
      if (cur) {
        if (v < cur.min) cur.min = v;
        if (v > cur.max) cur.max = v;
      } else {
        rangeByKey.set(row.metric_key, { min: v, max: v });
      }
    }
    const byPerson = new Map<string, RawBulletAggregateRow[]>();
    for (const row of r.items) {
      const pid = row.person_id.toLowerCase();
      const rng = rangeByKey.get(row.metric_key);
      const enriched: RawBulletAggregateRow = {
        metric_key: row.metric_key,
        value: row.value as number,
        median: null,
        range_min: rng?.min ?? null,
        range_max: rng?.max ?? null,
        p25: null,
        p75: null,
        n: null,
      };
      const arr = byPerson.get(pid);
      if (arr) arr.push(enriched);
      else byPerson.set(pid, [enriched]);
    }
    for (const [pid, prows] of byPerson) {
      const transformed = transformBulletMetrics(
        prows,
        sectionId,
        period,
        undefined,
        "ic",
        catalog,
      );
      const existing = byMember.get(pid) ?? [];
      existing.push(...transformed);
      byMember.set(pid, existing);
    }
  }
  return byMember;
}

export function useTeamMemberBullets(
  memberIds: string[],
  period: PeriodValue,
  range: DateRange,
): UseQueryResult<Map<string, BulletMetric[]>> {
  const { data: catalog } = useCatalog();
  const catalogKey = catalog?.generated_at ?? null;
  return useQuery({
    queryKey: [
      "v2",
      "team-member-bullets",
      memberIds.join(","),
      period,
      range.from,
      range.to,
      catalogKey,
    ],
    enabled: memberIds.length > 0,
    placeholderData: keepPreviousData,
    queryFn: () => fetchMemberBullets(memberIds, range, period, catalog),
  });
}

const DEPT_DIST_BULLET_IDS = [
  METRIC_REGISTRY.V2_DEPT_DIST_DELIVERY,
  METRIC_REGISTRY.V2_DEPT_DIST_COLLAB,
  METRIC_REGISTRY.V2_DEPT_DIST_GIT,
  METRIC_REGISTRY.V2_DEPT_DIST_AI,
] as const;

const DEPT_DIST_METRIC_IDS = [
  ...DEPT_DIST_BULLET_IDS,
  METRIC_REGISTRY.V2_DEPT_DIST_KPIS,
] as const;

/** Per-(department, metric) distribution long row from a `V2_DEPT_DIST_*` metric. */
type RawDeptDistRow = {
  org_unit_id: string;
  metric_key: string;
  p25: number | null;
  median: number | null;
  p75: number | null;
  range_min: number | null;
  range_max: number | null;
  n: number | null;
};

function foldDeptDistRows(target: DeptStatsMap, rows: RawDeptDistRow[]): void {
  for (const row of rows) {
    const n = Number(row.n);
    if (!Number.isFinite(n) || n <= 0) continue;
    const p25 = Number(row.p25);
    const p50 = Number(row.median);
    const p75 = Number(row.p75);
    if (
      !Number.isFinite(p25) ||
      !Number.isFinite(p50) ||
      !Number.isFinite(p75)
    ) {
      continue;
    }
    const stats: PeerStats = {
      p25,
      p50,
      p75,
      min: Number.isFinite(Number(row.range_min)) ? Number(row.range_min) : p25,
      max: Number.isFinite(Number(row.range_max)) ? Number(row.range_max) : p75,
      n,
    };
    let byMetric = target.get(row.org_unit_id);
    if (!byMetric) {
      byMetric = new Map();
      target.set(row.org_unit_id, byMetric);
    }
    byMetric.set(row.metric_key, stats);
  }
}

/**
 * Fetch every roster department's per-metric distribution in one request per
 * `V2_DEPT_DIST_*` family (`org_unit_id in (depts)`), folded into the two
 * source-family maps of [`DeptCohorts`]: `kpi` from the ic_kpis distribution
 * (team_row heatmap columns) and `bullet` from the three bullet-rows
 * distributions (member bullet comparisons). Rows without a usable cohort
 * (`n`/quartiles missing) are skipped. `median→p50`, `range_min→min`,
 * `range_max→max`.
 */
async function fetchDeptDistributions(
  orgUnitIds: string[],
  range: DateRange,
): Promise<DeptCohorts> {
  const empty: DeptCohorts = { kpi: new Map(), bullet: new Map() };
  if (orgUnitIds.length === 0) return empty;
  const ids = orgUnitIds
    .map((id) => `'${odataEscapeValue(id)}'`)
    .join(", ");
  const items = DEPT_DIST_METRIC_IDS.map((metricId) => ({
    id: metricId,
    metric_id: metricId,
    $filter: `org_unit_id in (${ids})`,
    $top: 5000,
  }));
  const resp = await queryBatchWithRange<RawDeptDistRow>(range, items);
  const out = empty;
  const bulletIds = new Set<string>(DEPT_DIST_BULLET_IDS);
  for (const r of resp.results as BatchQueryResult<RawDeptDistRow>[]) {
    if (r.status !== "ok") {
      throw new Error(`Failed to load ${r.metric_id} department distribution`);
    }
    if (!r.id) continue;
    foldDeptDistRows(bulletIds.has(r.id) ? out.bullet : out.kpi, r.items);
  }
  return out;
}

export function useDeptDistributions(
  orgUnitIds: string[],
  period: PeriodValue,
  range: DateRange,
  opts?: { enabled?: boolean },
): UseQueryResult<DeptCohorts> {
  return useQuery({
    queryKey: [
      "v2",
      "dept-distributions",
      [...orgUnitIds].sort().join(","),
      period,
      range.from,
      range.to,
    ],
    enabled: orgUnitIds.length > 0 && (opts?.enabled ?? true),
    placeholderData: keepPreviousData,
    queryFn: () => fetchDeptDistributions(orgUnitIds, range),
  });
}

export function useTeamMemberBulletsPrevious(
  memberIds: string[],
  period: PeriodValue,
  range: DateRange,
): UseQueryResult<Map<string, BulletMetric[]>> {
  const prev = previousPeriodRange(range, period);
  const { data: catalog } = useCatalog();
  const catalogKey = catalog?.generated_at ?? null;
  return useQuery({
    queryKey: [
      "v2",
      "team-member-bullets-prev",
      memberIds.join(","),
      period,
      prev.from,
      prev.to,
      catalogKey,
    ],
    enabled: memberIds.length > 0,
    placeholderData: keepPreviousData,
    queryFn: () => fetchMemberBullets(memberIds, prev, period, catalog),
  });
}

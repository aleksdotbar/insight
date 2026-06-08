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
import type { BulletMetric, PeriodValue } from "@/types/insight";

const SECTION_METRIC_IDS = [
  { sectionId: "task_delivery", metricId: METRIC_REGISTRY.V2_MEMBER_VALUES_DELIVERY },
  { sectionId: "git_output", metricId: METRIC_REGISTRY.V2_MEMBER_VALUES_GIT },
  { sectionId: "collaboration", metricId: METRIC_REGISTRY.V2_MEMBER_VALUES_COLLAB },
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

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
  { sectionId: "task_delivery", metricId: METRIC_REGISTRY.IC_BULLET_DELIVERY },
  { sectionId: "git_output", metricId: METRIC_REGISTRY.IC_BULLET_GIT },
  { sectionId: "collaboration", metricId: METRIC_REGISTRY.IC_BULLET_COLLAB },
] as const;

async function fetchMemberBullets(
  memberIds: string[],
  range: DateRange,
  period: PeriodValue,
  catalog: CatalogResponse,
): Promise<Map<string, BulletMetric[]>> {
  const items = memberIds.flatMap((id) =>
    SECTION_METRIC_IDS.map((s) => ({
      id: `${id.toLowerCase()}|${s.sectionId}`,
      metric_id: s.metricId,
      $filter: `person_id eq '${odataEscapeValue(id.toLowerCase())}'`,
    })),
  );
  const resp = await queryBatchWithRange<RawBulletAggregateRow>(range, items);
  const byMember = new Map<string, BulletMetric[]>();
  for (const r of resp.results as BatchQueryResult<RawBulletAggregateRow>[]) {
    if (r.status !== "ok" || !r.id) continue;
    const [personId, sectionId] = r.id.split("|");
    if (!personId || !sectionId) continue;
    const transformed = transformBulletMetrics(
      r.items,
      sectionId,
      period,
      undefined,
      "ic",
      catalog,
    );
    const existing = byMember.get(personId) ?? [];
    existing.push(...transformed);
    byMember.set(personId, existing);
  }
  return byMember;
}

export function useTeamMemberBullets(
  memberIds: string[],
  period: PeriodValue,
  range: DateRange,
): UseQueryResult<Map<string, BulletMetric[]>> {
  const { data: catalog } = useCatalog();
  return useQuery({
    queryKey: [
      "v2",
      "team-member-bullets",
      memberIds.join(","),
      period,
      range.from,
      range.to,
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
  return useQuery({
    queryKey: [
      "v2",
      "team-member-bullets-prev",
      memberIds.join(","),
      period,
      prev.from,
      prev.to,
    ],
    enabled: memberIds.length > 0,
    placeholderData: keepPreviousData,
    queryFn: () => fetchMemberBullets(memberIds, prev, period, catalog),
  });
}

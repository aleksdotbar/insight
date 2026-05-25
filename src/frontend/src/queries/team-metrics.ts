import { useQueries } from "@tanstack/react-query";

import { queryMetric } from "@/api/analytics-client";
import { METRIC_REGISTRY } from "@/api/metric-registry";
import { odataEscapeValue } from "@/api/odata";
import type { DateRange } from "@/api/period-to-date-range";
import type { RawBulletAggregateRow } from "@/api/raw-types";
import type { TeamMember } from "@/types/insight";

export type TeamMetricsSectionId =
  | "task_delivery"
  | "collaboration"
  | "ai_adoption"
  | "git_output";

const SECTIONS: ReadonlyArray<{
  id: TeamMetricsSectionId;
  metricId: string;
}> = [
  { id: "task_delivery", metricId: METRIC_REGISTRY.IC_BULLET_DELIVERY },
  { id: "collaboration", metricId: METRIC_REGISTRY.IC_BULLET_COLLAB },
  { id: "ai_adoption", metricId: METRIC_REGISTRY.IC_BULLET_AI },
  { id: "git_output", metricId: METRIC_REGISTRY.IC_BULLET_GIT },
];

export interface TeamMetricsEntry {
  personId: string;
  sectionId: TeamMetricsSectionId;
  rows: RawBulletAggregateRow[] | undefined;
}

export interface TeamMetricsResult {
  entries: TeamMetricsEntry[];
  isPending: boolean;
  isError: boolean;
}

export interface UseTeamMetricsOptions {
  enabled?: boolean;
}

/**
 * Fans out one bullet-aggregate query per (member, section) pair using
 * `useQueries`. Returns the raw `RawBulletAggregateRow[]` per entry —
 * consumers handle pivoting / column derivation themselves. Cache keys are
 * shared with IC dashboard so revisiting a member who was just viewed
 * resolves from cache.
 */
export function useTeamMetrics(
  members: TeamMember[],
  range: DateRange,
  options: UseTeamMetricsOptions = {},
): TeamMetricsResult {
  const enabled = options.enabled ?? true;

  const pairs = members.flatMap((m) =>
    SECTIONS.map((s) => ({
      personId: m.person_id,
      sectionId: s.id,
      metricId: s.metricId,
    })),
  );

  const queries = useQueries({
    queries: pairs.map((p) => ({
      queryKey: [
        "team-metrics",
        p.metricId,
        p.personId.toLowerCase(),
        range.from,
        range.to,
      ] as const,
      queryFn: () =>
        queryMetric<RawBulletAggregateRow>(p.metricId, range, {
          $filter: `person_id eq '${odataEscapeValue(p.personId.toLowerCase())}'`,
        }),
      enabled: enabled && Boolean(p.personId),
      staleTime: 5 * 60_000,
    })),
  });

  const entries: TeamMetricsEntry[] = pairs.map((p, i) => ({
    personId: p.personId,
    sectionId: p.sectionId,
    rows: queries[i]?.data?.items,
  }));

  const isPending = enabled && queries.some((q) => q.isPending);
  const isError = queries.some((q) => q.isError);

  return { entries, isPending, isError };
}

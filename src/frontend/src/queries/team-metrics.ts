import { METRIC_REGISTRY } from "@/api/metric-registry";
import { odataEscapeValue } from "@/api/odata";
import type { DateRange } from "@/api/period-to-date-range";
import type { RawBulletAggregateRow } from "@/api/raw-types";
import { useBatchedMetrics } from "@/queries/batched-metrics";
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
      key: `${m.person_id.toLowerCase()}:${s.id}`,
    })),
  );

  const query = useBatchedMetrics<RawBulletAggregateRow>(
    pairs.map((p) => ({
      id: p.key,
      metricId: p.metricId,
      filter: `person_id eq '${odataEscapeValue(p.personId.toLowerCase())}'`,
    })),
    range,
    { enabled, keyPrefix: "team-metrics" },
  );

  const entries: TeamMetricsEntry[] = pairs.map((p) => {
    const result = query.data?.get(p.key);
    return {
      personId: p.personId,
      sectionId: p.sectionId,
      rows: result && result.status === "ok" ? result.items : undefined,
    };
  });

  return {
    entries,
    isPending: enabled && pairs.length > 0 && query.isPending,
    isError: query.isError,
  };
}

import { useQuery, type UseQueryResult } from "@tanstack/react-query";

import { queryMetric } from "@/api/analytics-client";
import { METRIC_REGISTRY } from "@/api/metric-registry";
import type { DateRange } from "@/api/period-to-date-range";
import type { RawExecSummaryRow } from "@/api/raw-types";
import { transformExecRows } from "@/api/transforms";
import type { ExecColumnThreshold, ExecTeamRow, OrgKpis } from "@/types/insight";

function withValue<T>(vals: (T | null)[]): T[] {
  return vals.filter((v): v is T => v !== null);
}

function avg(arr: number[]): number | null {
  if (arr.length === 0) return null;
  return Math.round(arr.reduce((a, b) => a + b, 0) / arr.length);
}

export interface ExecSummary {
  teams: ExecTeamRow[];
  orgKpis: OrgKpis;
}

export function useExecSummary(
  range: DateRange,
  columnThresholds: ExecColumnThreshold[],
): UseQueryResult<ExecSummary> {
  return useQuery({
    // `columnThresholds` is part of the cache key so a tenant override flowing
    // from the catalog re-derives team-level status without a stale paint.
    // TanStack's structural deep-equality keeps cache hits stable across
    // renders that produce the same threshold values from `useCatalog`.
    queryKey: ["exec", "summary", range.from, range.to, columnThresholds],
    queryFn: async () => {
      const resp = await queryMetric<RawExecSummaryRow>(
        METRIC_REGISTRY.EXEC_SUMMARY,
        range,
        { $orderby: "org_unit_name asc", $top: 200 },
      );
      const teams = transformExecRows(resp.items, columnThresholds);
      const orgKpis: OrgKpis = {
        avgBuildSuccess: avg(withValue(teams.map((t) => t.build_success_pct))),
        avgAiAdoption: avg(withValue(teams.map((t) => t.ai_adoption_pct))),
        avgFocus: avg(withValue(teams.map((t) => t.focus_time_pct))),
      };
      return { teams, orgKpis };
    },
  });
}

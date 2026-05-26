import { useQuery, type UseQueryResult } from "@tanstack/react-query";

import {
  queryBatchWithRange,
  type BatchQueryResult,
} from "@/api/analytics-client";
import type { DateRange } from "@/api/period-to-date-range";

export interface BatchedMetricSpec {
  id: string;
  metricId: string;
  filter?: string;
  top?: number;
  orderby?: string;
}

export interface UseBatchedMetricsOptions {
  enabled?: boolean;
  staleTime?: number;
  keyPrefix?: string;
}

export function useBatchedMetrics<T>(
  specs: BatchedMetricSpec[],
  range: DateRange,
  options: UseBatchedMetricsOptions = {},
): UseQueryResult<Map<string, BatchQueryResult<T>>> {
  const { enabled = true, staleTime = 5 * 60_000, keyPrefix = "batched" } =
    options;

  const specsKey = specs.map(
    (s) => `${s.id}|${s.metricId}|${s.filter ?? ""}|${s.top ?? ""}|${s.orderby ?? ""}`,
  );

  return useQuery({
    queryKey: [keyPrefix, range.from, range.to, specsKey],
    enabled: enabled && specs.length > 0,
    staleTime,
    queryFn: async () => {
      const resp = await queryBatchWithRange<T>(
        range,
        specs.map((s) => ({
          id: s.id,
          metric_id: s.metricId,
          $filter: s.filter,
          $top: s.top,
          $orderby: s.orderby,
        })),
      );
      const byId = new Map<string, BatchQueryResult<T>>();
      for (const r of resp.results) {
        if (r.id !== undefined) byId.set(r.id, r);
      }
      return byId;
    },
  });
}

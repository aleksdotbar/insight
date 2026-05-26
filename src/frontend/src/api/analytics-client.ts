import { fetchWithAuth } from "@/api/fetch-with-auth";
import { andFilters, odataDateFilter } from "@/api/odata";
import type { DateRange } from "@/api/period-to-date-range";
import type { ODataParams, ODataResponse } from "@/types/insight";

const BASE = (import.meta.env.VITE_API_BASE as string | undefined) ??
  "/api/analytics/v1";

export class AnalyticsApiError extends Error {
  status: number;
  body: unknown;

  constructor(status: number, body: unknown) {
    super(`Analytics API ${status}`);
    this.name = "AnalyticsApiError";
    this.status = status;
    this.body = body;
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetchWithAuth(`${BASE}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
  });
  if (!res.ok) {
    const body = await res.json().catch(() => null);
    throw new AnalyticsApiError(res.status, body);
  }
  try {
    return (await res.json()) as T;
  } catch {
    throw new AnalyticsApiError(res.status, { error: "invalid_json" });
  }
}

/**
 * Execute a metric query bound to a period. Every dashboard-facing metric is
 * period-aware: the gold bullet/aggregate views aggregate over a time window,
 * so a query without a `metric_date` filter implicitly returns "all-time" —
 * almost never what a screen wants. This method makes the period mandatory
 * and ANDs it with any caller-supplied `$filter`.
 */
export async function queryMetric<T>(
  metricId: string,
  range: DateRange,
  params?: ODataParams,
): Promise<ODataResponse<T>> {
  const filter = andFilters(odataDateFilter(range), params?.$filter);
  return request<ODataResponse<T>>(`/metrics/${metricId}/query`, {
    method: "POST",
    body: JSON.stringify({ ...params, $filter: filter }),
  });
}

/**
 * Escape hatch for period-independent metric queries. Prefer `queryMetric`
 * for every dashboard call — forgetting the period silently returns all-time
 * data and breaks any "this week / month" expectation on the UI.
 */
export async function queryMetricRaw<T>(
  metricId: string,
  params: ODataParams,
): Promise<ODataResponse<T>> {
  return request<ODataResponse<T>>(`/metrics/${metricId}/query`, {
    method: "POST",
    body: JSON.stringify(params),
  });
}

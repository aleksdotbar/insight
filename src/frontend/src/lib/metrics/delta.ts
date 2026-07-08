import type {
  MetricComputation,
  MetricFormat,
} from "@/api/metric-results-client";

export type MetricDelta =
  | { kind: "percent_change"; value: number }
  | { kind: "pp_change"; value: number };

/**
 * Period-over-period delta semantics derive from the computation tag, not
 * per-metric code: a percent-formatted ratio compares in percentage points
 * (77% vs 72% is "+5 pp", not "+6.9%"); everything else compares relatively.
 */
export function computeDelta(
  current: number | null | undefined,
  previous: number | null | undefined,
  computation: MetricComputation,
  format: MetricFormat,
): MetricDelta | null {
  if (
    current == null ||
    previous == null ||
    !Number.isFinite(current) ||
    !Number.isFinite(previous)
  ) {
    return null;
  }

  if (computation === "ratio" && format === "percent") {
    return { kind: "pp_change", value: current - previous };
  }

  if (previous === 0) return null;
  return {
    kind: "percent_change",
    value: ((current - previous) / Math.abs(previous)) * 100,
  };
}

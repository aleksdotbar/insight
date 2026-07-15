import type { MetricFormat } from "@/api/metric-results-client";
import { formatMetricValue, formatPp } from "@/lib/format";

/**
 * The single "scale of divergence" formatter shared by every surface that
 * shows how far a value sits from its peer median — the drilldown peer story,
 * the KPI tiles, and the needs-attention list. Only the surrounding wording
 * differs per surface (verbose in the drilldown, compact on the dashboard);
 * the magnitude itself is computed once, here.
 *
 * At/above `GAP_MULTIPLE_THRESHOLD`× the median a signed percent runs away
 * (300%, 500%…), so the far-above side reads as a multiple ("5.6×"); nearer
 * gaps and the whole below-median side (bounded to −100%) stay a signed
 * percent; a sub-unit gap with no usable median falls back to a signed
 * absolute delta.
 */
const GAP_MULTIPLE_THRESHOLD = 2;

function formatMultiple(ratio: number): string {
  const rounded = ratio >= 10 ? Math.round(ratio) : Math.round(ratio * 10) / 10;
  return `${rounded}×`;
}

function formatGapPct(gap: number): string {
  const pct = Math.round(Math.abs(gap) * 100);
  if (pct === 0) return "0%";
  return `${gap >= 0 ? "+" : "-"}${pct}%`;
}

export interface GapInput {
  value: number;
  /** Peer-cohort median; null when suppressed. */
  median: number | null;
  /** Arithmetic (value − median) / |median|; null when median ~ 0. */
  gapPct: number | null;
  /** Arithmetic value − median. */
  gapDelta: number;
  format: MetricFormat;
  unit: string | null;
}

export function formatGapMagnitude({
  value,
  median,
  gapPct,
  gapDelta,
  format,
  unit,
}: GapInput): string {
  // Percentage metrics diverge in percentage POINTS, not a ratio: a relative
  // "−39%" beside a "90%" median reads as points and misstates the gap, and a
  // multiple ("0.6×") is meaningless. gapDelta is already the point spread —
  // render it like the period delta pill.
  if (format === "percent") {
    return formatPp(gapDelta);
  }
  if (
    median != null &&
    Math.abs(median) > 1e-9 &&
    value / median >= GAP_MULTIPLE_THRESHOLD
  ) {
    return formatMultiple(value / median);
  }
  if (gapPct != null) return formatGapPct(gapPct);
  const sign = gapDelta >= 0 ? "+" : "-";
  return `${sign}${formatMetricValue(Math.abs(gapDelta), format, unit)}`;
}

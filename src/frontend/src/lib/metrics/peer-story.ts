import type {
  MetricDirection,
  MetricFormat,
} from "@/api/metric-results-client";
import {
  forEntity,
  type MetricCollectionConfig,
  type NormalizedMetricResult,
  type PeerEntityStats,
} from "@/lib/metrics/collection";
import {
  peerStatusVsQuartiles,
  type FocusMode,
  type PeerStats,
  type PeerStatusWithNeutral,
} from "@/lib/peers";

/**
 * The peer story ranks a collection's metrics by how far the person sits
 * from their cohort: outliers (top/bottom quartile) ordered by severity,
 * everything else folded as supporting. Pure data — layout lives in
 * `components/widgets/metric-views/peer-story.tsx`.
 */
export type PeerStoryEntry = {
  key: string;
  label: string;
  sublabel?: string;
  value: number;
  unit: string | null;
  format: MetricFormat;
  higherIsBetter: boolean;
  neutral: boolean;
  /** False when the person has no observations (zero-filled own total). */
  observed: boolean;
  stats: PeerStats | null;
  status: PeerStatusWithNeutral;
  gapPct: number | null;
  gapDelta: number;
  severity: number;
};

export function toPeerStats(row: PeerEntityStats | null): PeerStats | null {
  if (
    row?.p25 == null ||
    row.median == null ||
    row.p75 == null ||
    row.min == null ||
    row.max == null
  ) {
    return null;
  }
  return {
    p25: row.p25,
    p50: row.median,
    p75: row.p75,
    min: row.min,
    max: row.max,
    n: row.n,
  };
}

function peerSpread(stats: PeerStats): number {
  const iqr = Math.abs(stats.p75 - stats.p25);
  if (iqr > 1e-9) return iqr;
  const range = Math.abs(stats.max - stats.min);
  if (range > 1e-9) return range;
  return 1;
}

function toStoryEntry(
  metric: NormalizedMetricResult,
  value: number,
  stats: PeerStats | null,
  observed: boolean,
): PeerStoryEntry {
  const direction: MetricDirection = metric.direction;
  const neutral = direction === "neutral";
  const higherIsBetter = direction !== "lower_is_better";
  const usePeerRanking =
    !neutral && observed && stats != null && Number.isFinite(value);
  const status = usePeerRanking
    ? peerStatusVsQuartiles(value, stats, higherIsBetter)
    : "neutral";
  const rawDelta = stats ? value - stats.p50 : 0;
  const gapDelta = higherIsBetter ? rawDelta : -rawDelta;
  const gapPct =
    stats && Math.abs(stats.p50) > 1e-9 ? gapDelta / Math.abs(stats.p50) : null;
  return {
    key: metric.metric_key,
    label: metric.label,
    sublabel: metric.description,
    value,
    unit: metric.unit,
    format: metric.format,
    higherIsBetter,
    neutral,
    observed,
    stats,
    status,
    gapPct,
    gapDelta,
    severity:
      usePeerRanking && stats
        ? Math.abs(gapPct ?? gapDelta / peerSpread(stats))
        : 0,
  };
}

/**
 * Entries in collection order; metrics without a period value drop out.
 * Ranking additionally requires an OBSERVED value: the period view zero-fills
 * a person's own total, but `peer.target_value` is null when the person has
 * no observations (absence is indistinguishable from lack of source
 * coverage), and an unmeasured person has no peer standing — the entry stays
 * neutral instead of being branded a bottom-quartile outlier.
 */
export function buildPeerStoryEntries(
  collection: MetricCollectionConfig,
  byKey: Map<string, NormalizedMetricResult>,
  entityId: string,
): PeerStoryEntry[] {
  return collection.metrics.flatMap((metricConfig) => {
    const metric = byKey.get(metricConfig.key);
    if (!metric) return [];
    const data = forEntity(metric, entityId);
    if (data.value == null || !Number.isFinite(data.value)) return [];
    const observed = data.peer == null || data.peer.target_value != null;
    return [
      toStoryEntry(metric, data.value, toPeerStats(data.peer), observed),
    ];
  });
}

export interface PeerStoryPartition {
  /** Outliers under the active focus mode, severity-ranked. */
  focusedOutliers: PeerStoryEntry[];
  hero: PeerStoryEntry | null;
  sideCards: PeerStoryEntry[];
  chips: PeerStoryEntry[];
  folded: PeerStoryEntry[];
}

export function partitionPeerStory(
  entries: PeerStoryEntry[],
  focusMode: FocusMode,
): PeerStoryPartition {
  const bottom = entries
    .filter((entry) => entry.status === "bottom")
    .sort((a, b) => b.severity - a.severity);
  const top = entries
    .filter((entry) => entry.status === "top")
    .sort((a, b) => b.severity - a.severity);
  const folded = entries
    .filter((entry) => entry.status === "in_pack" || entry.status === "neutral")
    .sort((a, b) => {
      if (a.status === b.status) return 0;
      return a.status === "neutral" ? 1 : -1;
    });

  const focusedOutliers =
    focusMode === "critical"
      ? bottom
      : focusMode === "rewards"
        ? top
        : focusMode === "all"
          ? bottom.length > 0
            ? [...bottom, ...top]
            : top
          : [];
  const hero = focusedOutliers[0] ?? null;

  return {
    focusedOutliers,
    hero,
    sideCards: hero ? focusedOutliers.slice(1, 4) : [],
    chips: hero ? focusedOutliers.slice(4) : [],
    folded,
  };
}

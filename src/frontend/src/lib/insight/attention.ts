import { formatMetricValue } from "@/lib/format";
import { formatGapMagnitude } from "@/lib/metrics/gap";
import type { MetricGroup, GroupId } from "@/lib/insight/groups";
import {
  bulletCatalogKey,
  type CatalogByKey,
} from "@/lib/insight/v2/peer-status";
import {
  forEntity,
  type NormalizedMetricResult,
} from "@/lib/metrics/collection";
import { toPeerStats } from "@/lib/metrics/peer-standing";
import { peerStatusVsQuartiles } from "@/lib/peers";
import type { BulletMetric } from "@/types/insight";

/**
 * One "needs attention" row: a metric sitting in the bottom quartile of its
 * cohort, display-ready. Selectors below feed the shared surface from both
 * data paths; ranking (`relGap` descending) happens in the component.
 */
export interface AttentionItem {
  key: string;
  group: GroupId;
  label: string;
  valueText: string;
  /** Formatted peer-median value only (no label); the view frames it. */
  medianText: string | null;
  /** Scale of divergence from the median ("16×", "−40%"); null at the median. */
  gapText: string | null;
  relGap: number;
}

export interface LegacyAttentionGroup {
  id: GroupId;
  rows: BulletMetric[];
}

/** Legacy bullet rows + catalog direction → attention items. */
export function legacyAttentionItems(
  groups: LegacyAttentionGroup[],
  byMetricKey: CatalogByKey
): AttentionItem[] {
  const items: AttentionItem[] = [];
  for (const group of groups) {
    for (const row of group.rows) {
      // schema_status='error' rows never trigger the attention surface — we
      // can't compare a broken metric to peers. Missing-id rows likewise
      // collapse out (no catalog row → no higher_is_better signal).
      if (row.schema_error) continue;
      const value = Number(row.value);
      if (!Number.isFinite(value)) continue;
      const stats = row.peer;
      if (!stats) continue;
      const catalogRow = byMetricKey(bulletCatalogKey(row));
      if (!catalogRow) continue;
      const higherIsBetter = catalogRow.higher_is_better;
      if (peerStatusVsQuartiles(value, stats, higherIsBetter) !== "bottom") {
        continue;
      }
      const median = stats.p50;
      const denom = Math.abs(median) > 1e-9 ? Math.abs(median) : 1;
      const relGap = higherIsBetter
        ? (median - value) / denom
        : (value - median) / denom;
      const gapDelta = value - median;
      items.push({
        key: row.metric_key,
        group: group.id,
        label: row.label,
        valueText: `${row.value}${row.unit ? ` ${row.unit}` : ""}`,
        medianText: `${Math.round(median * 10) / 10}${row.unit ? ` ${row.unit}` : ""}`,
        gapText: formatGapMagnitude({
          value,
          median,
          gapPct: Math.abs(median) > 1e-9 ? gapDelta / Math.abs(median) : null,
          gapDelta,
          format: "decimal",
          unit: row.unit ?? null,
        }),
        relGap,
      });
    }
  }
  return items;
}

/** Metric-collection results → attention items; direction rides the wire. */
export function metricAttentionItems(
  def: MetricGroup,
  byKey: Map<string, NormalizedMetricResult>,
  entityId: string
): AttentionItem[] {
  const items: AttentionItem[] = [];
  for (const metricConfig of def.collection.metrics) {
    const metric = byKey.get(metricConfig.key);
    if (!metric || metric.direction === "neutral") continue;
    const data = forEntity(metric, entityId);
    const value = data.value;
    if (value == null || !Number.isFinite(value)) continue;
    // Unmeasured people have no peer standing: the period view zero-fills
    // the own total, but a null peer target_value means no observations.
    if (data.peer?.target_value == null) continue;
    const stats = toPeerStats(data.peer);
    if (!stats) continue;
    const higherIsBetter = metric.direction !== "lower_is_better";
    if (peerStatusVsQuartiles(value, stats, higherIsBetter) !== "bottom") {
      continue;
    }
    const median = stats.p50;
    const denom = Math.abs(median) > 1e-9 ? Math.abs(median) : 1;
    const relGap = higherIsBetter
      ? (median - value) / denom
      : (value - median) / denom;
    const gapDelta = value - median;
    items.push({
      key: metric.metric_key,
      group: def.id,
      label: metric.label,
      valueText: formatMetricValue(value, metric.format, metric.unit),
      medianText: formatMetricValue(median, metric.format, metric.unit),
      gapText: formatGapMagnitude({
        value,
        median,
        gapPct: Math.abs(median) > 1e-9 ? gapDelta / Math.abs(median) : null,
        gapDelta,
        format: metric.format,
        unit: metric.unit,
      }),
      relGap,
    });
  }
  return items;
}

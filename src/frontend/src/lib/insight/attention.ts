import { formatMetricValue } from "@/lib/format";
import type { MetricGroup, GroupId } from "@/lib/insight/groups";
import {
  bulletCatalogKey,
  type CatalogByKey,
} from "@/lib/insight/v2/peer-status";
import {
  forEntity,
  type NormalizedMetricResult,
} from "@/lib/metrics/collection";
import { toPeerStats } from "@/lib/metrics/peer-story";
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
  medianText: string | null;
  relGap: number;
}

export interface LegacyAttentionGroup {
  id: GroupId;
  rows: BulletMetric[];
}

/** Legacy bullet rows + catalog direction → attention items. */
export function legacyAttentionItems(
  groups: LegacyAttentionGroup[],
  byMetricKey: CatalogByKey,
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
      items.push({
        key: row.metric_key,
        group: group.id,
        label: row.label,
        valueText: `${row.value}${row.unit ? ` ${row.unit}` : ""}`,
        medianText: `Median ${Math.round(median * 10) / 10}${row.unit ? ` ${row.unit}` : ""}`,
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
  entityId: string,
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
    items.push({
      key: metric.metric_key,
      group: def.id,
      label: metric.label,
      valueText: formatMetricValue(value, metric.format, metric.unit),
      medianText: `Median ${formatMetricValue(median, metric.format, metric.unit)}`,
      relGap,
    });
  }
  return items;
}

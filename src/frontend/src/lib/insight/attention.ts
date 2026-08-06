import {
  formatMetricNumber,
  formatMetricValue,
  metricDisplayUnit,
} from "@/lib/format";
import { formatGapMagnitude } from "@/lib/metrics/gap";
import { KPI_ROW, type MetricGroup, type GroupId } from "@/lib/insight/groups";
import {
  forEntity,
  type NormalizedMetricResult,
} from "@/lib/metrics/collection";
import { toPeerStats } from "@/lib/metrics/peer-standing";
import { peerStatusVsQuartiles } from "@/lib/peers";

/**
 * One "needs attention" row: a metric sitting in the bottom quartile of its
 * cohort, display-ready. Ranking (`relGap` descending) happens in the
 * component.
 */
/**
 * How far a metric must move against its own past to count as a change rather
 * than noise: a period boundary shifts most counters a little.
 */
const ADVERSE_MOVE_MIN = 0.1;

export interface AttentionItem {
  key: string;
  group: GroupId;
  label: string;
  valueText: string;
  /** The number alone, and its unit — so a list can align digits on the right
   *  and units on the left, and have both columns start together. */
  valueNumber: string;
  valueUnit: string;
  /** Formatted peer-median value only (no label); the view frames it. */
  medianText: string | null;
  /** Scale of divergence from the median ("16×", "−40%"); null at the median. */
  gapText: string | null;
  relGap: number;
}

/**
 * Metric-collection results → attention items; direction rides the wire.
 *
 * Only metrics the section card does NOT already show. A card carries its
 * `preview` rows in the same colours and a "N behind peers" badge over them, so
 * repeating those rows above put every such finding on the screen twice — and
 * a reader counts red marks, not facts. Two numbers for one problem read as two
 * problems: the person-page screenshot that started this had eleven red marks
 * over six distinct findings.
 *
 * What is left is what the cards cannot tell you: a metric outside the preview
 * that is nonetheless bottom-quartile. That is the whole job of this block —
 * the thing you would otherwise miss.
 *
 * The card's SUMMARY line counts as shown too. It is the most prominent thing
 * on the card and it is chosen from every metric of the group, not from the
 * three the card lists — so excluding only the preview left the lead metric
 * appearing twice, which is how "Lines added: 143 lines · −98% vs median"
 * managed to be both a card headline and the first attention row.
 */
export function metricAttentionItems(
  def: MetricGroup,
  byKey: Map<string, NormalizedMetricResult>,
  previousByKey: Map<string, NormalizedMetricResult> | null,
  entityId: string
): AttentionItem[] {
  const items: AttentionItem[] = [];
  // KPI_ROW is a candidate list; the row renders the ones this person is
  // measured on, and any of them may be up there.
  const onHeadlineRow = new Set<string>(KPI_ROW);
  for (const metricConfig of def.collection.metrics) {
    if (onHeadlineRow.has(metricConfig.key)) continue;
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
    // Below the cohort AND moving the wrong way.
    //
    // "Below the cohort" alone is a standing, not an event: a lead measured
    // against the developers reporting to them is below on commits every
    // month, by the shape of the job, and a block that repeats that forever
    // teaches the reader to skip it. A structural gap is flat; a regression is
    // not, so requiring the move keeps the standing out and lets the change
    // through.
    //
    // No previous value means no claim about direction — the item is left out
    // rather than asserted on one period of data.
    const before = previousByKey?.get(metricConfig.key);
    const previous = before ? forEntity(before, entityId).value : null;
    if (previous == null || !Number.isFinite(previous)) continue;
    const movedAdversely = higherIsBetter ? value < previous : value > previous;
    if (!movedAdversely) continue;
    const moveScale = Math.abs(previous) > 1e-9 ? Math.abs(previous) : 1;
    if (Math.abs(value - previous) / moveScale < ADVERSE_MOVE_MIN) continue;
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
      valueNumber: formatMetricNumber(value, metric.format),
      valueUnit: metricDisplayUnit(metric.format, metric.unit) ?? "",
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

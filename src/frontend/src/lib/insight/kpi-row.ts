import { formatMetricNumber, formatMetricValue } from "@/lib/format";
import {
  KPI_ROW,
  KPI_ROW_MAX,
  groupIdForMetricKey,
  type GroupId,
} from "@/lib/insight/groups";
import {
  entityObserved,
  forEntity,
  type NormalizedMetricResult,
} from "@/lib/metrics/collection";
import { peerStatusToStatus } from "@/lib/insight/peer-status";
import { formatGapMagnitude } from "@/lib/metrics/gap";
import { derivePeerStanding } from "@/lib/metrics/peer-standing";
import {
  computeDelta,
  deltaStatus,
  formatTileDelta,
} from "@/lib/metrics/delta";
import type { FocusMode } from "@/lib/peers";
import { applyFocusStatus, type Status } from "@/lib/status";

/**
 * Display-ready KPI tile input: selectors own all formatting and scoring, so
 * the tile renders a value without knowing how it was computed.
 */
export interface KpiTileData {
  key: string;
  label: string;
  value: string;
  valueStatus: Status;
  delta: { text: string; status: Status; down: boolean } | null;
  medianLabel: string | null;
  /**
   * Scale of divergence from the peer median ("3.5×", "−39%", "−35 pp"), shown
   * beside the median; null at the median or without an honest comparison.
   */
  gapText: string | null;
  /**
   * Always neutral today: the value carries the peer verdict, and repeating it
   * under the number doubles the red without adding a fact. Kept as a field so
   * a future rule (a real threshold on the gap itself) has somewhere to land.
   */
  gapStatus: Status;
  /** Secondary context line, shown when explanations are enabled. */
  context: string | null;
  groupId: GroupId | null;
}

/** Metric-collection results → tiles, in `KPI_ROW` order. */
export function metricKpiTiles(
  byKey: Map<string, NormalizedMetricResult>,
  previousByKey: Map<string, NormalizedMetricResult> | null,
  entityId: string,
  focusMode: FocusMode
): KpiTileData[] {
  const tiles = KPI_ROW.flatMap((metricKey) => {
    const metric = byKey.get(metricKey);
    if (!metric) return [];
    // Never observed for this person = no connector feeds it for them, which
    // is not a headline — it is an empty slot in the most valuable space on the
    // page. A measured zero survives: `entityObserved` reads the peer target,
    // so 0 stays and only null drops.
    if (!entityObserved(metric, entityId)) return [];

    const data = forEntity(metric, entityId);
    const value = data.value;
    const median = data.peer?.median ?? null;
    // Eligibility (observed / suppressed / flat pool / neutral direction)
    // and the quartile rank come from the shared standing derivation; the
    // color follows the same rank mapping as every card and the peer story
    // — red means bottom quartile, in-pack is normal and stays uncolored.
    const standing = derivePeerStanding(metric.direction, data);
    const valueStatus = applyFocusStatus(
      peerStatusToStatus(standing.rank),
      focusMode
    );

    const previousMetric = previousByKey?.get(metricKey) ?? null;
    const previousValue = previousMetric
      ? forEntity(previousMetric, entityId).value
      : null;
    const rawDelta = computeDelta(
      value,
      previousValue,
      metric.computation,
      metric.format
    );
    const deltaText = rawDelta ? formatTileDelta(rawDelta) : null;
    const delta =
      rawDelta && deltaText
        ? {
            text: deltaText,
            status: applyFocusStatus(
              deltaStatus(rawDelta, metric.direction),
              focusMode
            ),
            down: rawDelta.value < 0,
          }
        : null;

    // Divergence magnitude vs the median — only for an eligible standing with
    // a real gap (at the median there's nothing to scream about).
    const gapText =
      standing.eligible && value != null && Math.abs(standing.gapDelta) > 1e-9
        ? formatGapMagnitude({
            value,
            median,
            gapPct: standing.gapPct,
            gapDelta: standing.gapDelta,
            format: metric.format,
            unit: metric.unit,
          })
        : null;

    return [
      {
        key: metric.metric_key,
        label: metric.label,
        value:
          value == null
            ? "—"
            : metric.format === "percent"
              ? formatMetricValue(value, metric.format, metric.unit)
              : formatMetricNumber(value, metric.format),
        valueStatus,
        delta,
        medianLabel:
          median != null
            ? `median ${
                metric.format === "percent"
                  ? formatMetricValue(median, metric.format, metric.unit)
                  : formatMetricNumber(median, metric.format)
              }`
            : null,
        gapText,
        // Neutral on purpose: the gap EXPLAINS the value, it does not judge it
        // a second time. Painting both left one finding wearing two red marks,
        // and a reader counts marks — a person page with six findings read as
        // eleven problems.
        gapStatus: "neutral" as Status,
        context: metric.description ?? null,
        groupId: groupIdForMetricKey(metric.metric_key),
      },
    ];
  });
  return tiles.slice(0, KPI_ROW_MAX);
}

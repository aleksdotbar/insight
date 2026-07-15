import { formatKpiValue } from "@/api/transforms";
import {
  formatMetricNumber,
  formatMetricValue,
  formatPp,
} from "@/lib/format";
import {
  KPI_ROW,
  groupIdForMetricKey,
  type GroupId,
} from "@/lib/insight/groups";
import type { CatalogByKey } from "@/lib/insight/v2/peer-status";
import {
  forEntity,
  type NormalizedMetricResult,
} from "@/lib/metrics/collection";
import { derivePeerStanding } from "@/lib/metrics/peer-standing";
import { computeDelta, type MetricDelta } from "@/lib/metrics/delta";
import type { FocusMode } from "@/lib/peers";
import { applyFocusStatus, statusVsMedian, type Status } from "@/lib/status";
import type { IcKpi } from "@/types/insight";

/**
 * Display-ready KPI tile input: selectors own all formatting and scoring, so
 * the tile renders both legacy-batch and metric-collection KPIs without
 * knowing which is which.
 */
export interface KpiTileData {
  key: string;
  label: string;
  value: string;
  valueStatus: Status;
  delta: { text: string; status: Status; down: boolean } | null;
  medianLabel: string | null;
  /** Secondary context line, shown when explanations are enabled. */
  context: string | null;
  groupId: GroupId | null;
}

const IC_KPI_PREFIX = "ic_kpis.";

/** Legacy KPI batch rows → tiles (logic lifted verbatim from the old tile). */
export function legacyKpiTiles(
  kpis: IcKpi[],
  byMetricKey: CatalogByKey,
  focusMode: FocusMode,
): KpiTileData[] {
  const byKey = new Map(kpis.map((kpi) => [kpi.metric_key, kpi]));
  return KPI_ROW.flatMap((source) => {
    if (source.kind !== "legacy") return [];
    const kpi = byKey.get(source.key);
    if (!kpi) return [];

    const catalogRow = byMetricKey(`${IC_KPI_PREFIX}${kpi.metric_key}`);
    const isSchemaError = catalogRow?.schema_status === "error";
    const fmt = catalogRow?.format;
    const isCountMetric =
      catalogRow !== undefined && fmt !== "percent" && fmt !== "hours";
    const rawValue = kpi.raw_value ?? (isCountMetric ? 0 : null);
    const peerMedian = kpi.peer_median ?? null;
    const hasMedian =
      peerMedian != null && Number.isFinite(peerMedian) && peerMedian > 0;
    const isPercent = kpi.unit === "%";
    const value =
      kpi.value ?? (isCountMetric ? formatKpiValue(0, fmt) : "—");

    const valueStatus = applyFocusStatus(
      !isSchemaError && catalogRow && rawValue !== null && hasMedian
        ? statusVsMedian(rawValue, peerMedian, catalogRow.higher_is_better)
        : "neutral",
      focusMode,
    );

    const showDelta = kpi.delta !== "" && kpi.delta_type !== "neutral";
    const delta = showDelta
      ? {
          // Percent-valued metrics deltas are percentage points; drop the `%`
          // so "+5" doesn't read as "5% of 86%".
          text: isPercent ? kpi.delta.replace(/%$/, "") : kpi.delta,
          status: applyFocusStatus(kpi.delta_type, focusMode),
          down: kpi.delta.trim().startsWith("-"),
        }
      : null;

    const medianLabel =
      hasMedian && catalogRow !== undefined && !isSchemaError
        ? `Median ${formatKpiValue(peerMedian, catalogRow.format)}${isPercent ? "%" : ""}`
        : null;

    return [
      {
        key: kpi.metric_key,
        label: kpi.label,
        value: `${value}${isPercent && value !== "—" ? "%" : ""}`,
        valueStatus,
        delta,
        medianLabel,
        context: catalogRow?.source_tags.length
          ? catalogRow.source_tags.join(", ")
          : null,
        groupId: source.groupId,
      },
    ];
  });
}

function deltaStatus(
  delta: MetricDelta,
  direction: NormalizedMetricResult["direction"],
): Status {
  if (direction === "neutral" || delta.value === 0) return "neutral";
  const favorable =
    direction === "lower_is_better" ? delta.value < 0 : delta.value > 0;
  return favorable ? "good" : "bad";
}

/** Display-rounded delta; null when it rounds to zero (no "+0%" badges). */
function formatTileDelta(delta: MetricDelta): string | null {
  if (delta.kind === "pp_change") {
    return Math.abs(delta.value) < 0.05 ? null : formatPp(delta.value);
  }
  const rounded = Math.round(delta.value);
  if (rounded === 0) return null;
  return `${rounded > 0 ? "+" : ""}${rounded}%`;
}

/** Metric-collection results → tiles, in `KPI_ROW` order. */
export function metricKpiTiles(
  byKey: Map<string, NormalizedMetricResult>,
  previousByKey: Map<string, NormalizedMetricResult> | null,
  entityId: string,
  focusMode: FocusMode,
): KpiTileData[] {
  return KPI_ROW.flatMap((source) => {
    if (source.kind !== "metric") return [];
    const metric = byKey.get(source.metricKey);
    if (!metric) return [];

    const data = forEntity(metric, entityId);
    const value = data.value;
    const median = data.peer?.median ?? null;
    // Eligibility (observed / suppressed / flat pool / neutral direction)
    // and the median judgment come from the shared standing derivation.
    // Only a strictly favorable/unfavorable median side earns a color —
    // at-median is "on par", not praise (an all-idle cohort must not paint
    // idleness green).
    const standing = derivePeerStanding(metric.direction, data);
    const valueStatus = applyFocusStatus(
      standing.medianSide === "favorable"
        ? "good"
        : standing.medianSide === "unfavorable"
          ? "bad"
          : "neutral",
      focusMode,
    );

    const previousMetric = previousByKey?.get(source.metricKey) ?? null;
    const previousValue = previousMetric
      ? forEntity(previousMetric, entityId).value
      : null;
    const rawDelta = computeDelta(
      value,
      previousValue,
      metric.computation,
      metric.format,
    );
    const deltaText = rawDelta ? formatTileDelta(rawDelta) : null;
    const delta =
      rawDelta && deltaText
        ? {
            text: deltaText,
            status: applyFocusStatus(
              deltaStatus(rawDelta, metric.direction),
              focusMode,
            ),
            down: rawDelta.value < 0,
          }
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
            ? `Median ${
                metric.format === "percent"
                  ? formatMetricValue(median, metric.format, metric.unit)
                  : formatMetricNumber(median, metric.format)
              }`
            : null,
        context: metric.description ?? null,
        groupId: groupIdForMetricKey(metric.metric_key),
      },
    ];
  });
}

/** All KPI tiles in `KPI_ROW` display order. */
export function kpiRowTiles(
  legacy: KpiTileData[],
  metric: KpiTileData[],
): KpiTileData[] {
  const byKey = new Map(
    [...legacy, ...metric].map((tile) => [tile.key, tile]),
  );
  return KPI_ROW.flatMap((source) => {
    const key = source.kind === "legacy" ? source.key : source.metricKey;
    const tile = byKey.get(key);
    return tile ? [tile] : [];
  });
}

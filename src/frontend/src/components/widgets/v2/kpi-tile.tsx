import { Sparkles, TrendingDownIcon, TrendingUpIcon } from "lucide-react";

import { formatKpiValue } from "@/api/transforms";
import { useCatalog } from "@/api/use-catalog";
import { Badge } from "@/components/ui/badge";
import {
  Card,
  CardAction,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { useSettings } from "@/hooks/use-settings";
import { STATUS_TEXT_CLASS, applyFocusStatus, type Status } from "@/lib/status";
import { cn } from "@/lib/utils";
import type { IcKpi } from "@/types/insight";

export interface KpiTileProps {
  kpi: IcKpi;
  onClick?: (metricKey: string) => void;
}

const CARD_SURFACE = "@container/card";

function peerStatusVsMedian(
  value: number,
  median: number,
  higherIsBetter: boolean
): Status {
  if (!Number.isFinite(value) || !Number.isFinite(median) || median === 0) {
    return "neutral";
  }
  const meetsTarget = higherIsBetter ? value >= median : value <= median;
  return meetsTarget ? "good" : "bad";
}

export function KpiTile({ kpi, onClick }: KpiTileProps) {
  const { focusMode, showExplanations } = useSettings();
  const { byMetricKey } = useCatalog();
  // Wire key is `ic_kpis.<bare>`; `kpi.metric_key` here is the bare form
  // because `transformIcKpis` strips the `ic_kpis.` prefix.
  const catalogRow = byMetricKey(`ic_kpis.${kpi.metric_key}`);
  const sourceTags = catalogRow?.source_tags.length
    ? catalogRow.source_tags.join(", ")
    : null;
  const isSchemaError = catalogRow?.schema_status === "error";
  const hasValue = kpi.raw_value !== null;
  // Department peer median folded onto the KPI row by the IC_KPIS query_ref.
  const peerMedian = kpi.peer_median ?? null;
  const hasMedian =
    peerMedian != null && Number.isFinite(peerMedian) && peerMedian > 0;
  const value = kpi.value ?? "—";
  // Units are implied by the card label, so we drop them — except `%`, which
  // reads as part of the number and renders at the value's size.
  const isPercent = kpi.unit === "%";

  // Value coloring: how the result sits against the peer median. Suppressed
  // (neutral) for schema_status='error' / missing-id / no-median rows.
  const valueStatus = applyFocusStatus(
    !isSchemaError && catalogRow && hasValue && hasMedian && peerMedian != null
      ? peerStatusVsMedian(
          kpi.raw_value as number,
          peerMedian,
          catalogRow.higher_is_better
        )
      : "neutral",
    focusMode
  );

  // Trend coloring: whether the period-over-period move is favorable
  // (`delta_type` already folds in `higher_is_better`).
  const trendStatus = applyFocusStatus(kpi.delta_type, focusMode);
  const showDelta = kpi.delta !== "" && kpi.delta_type !== "neutral";
  const trendDown = kpi.delta.trim().startsWith("-");
  // For percent-valued metrics the delta is in percentage points, not a
  // relative %; drop the `%` so "+5" doesn't read as "5% of 86%".
  const deltaText = isPercent ? kpi.delta.replace(/%$/, "") : kpi.delta;

  const medianLabel =
    hasMedian &&
    hasValue &&
    catalogRow !== undefined &&
    !isSchemaError &&
    peerMedian != null
      ? `Median ${formatKpiValue(peerMedian, catalogRow.format)}${isPercent ? "%" : ""}`
      : null;

  const interactive = Boolean(onClick);

  return (
    <Card
      className={cn(
        CARD_SURFACE,
        interactive && "text-left transition-colors hover:bg-accent/50"
      )}
      render={
        interactive ? (
          <button
            type="button"
            onClick={() => onClick?.(kpi.metric_key)}
            aria-label={`Open ${kpi.label} details`}
          />
        ) : undefined
      }
    >
      <CardHeader>
        <CardDescription className="flex flex-col gap-0.5">
          <span className="truncate">{kpi.label}</span>
          {showExplanations && sourceTags ? (
            <span className="truncate font-normal text-muted-foreground/70">
              {sourceTags}
            </span>
          ) : null}
        </CardDescription>
        <CardTitle
          className={cn(
            "text-2xl font-semibold tabular-nums @[250px]/card:text-3xl",
            valueStatus !== "neutral" && STATUS_TEXT_CLASS[valueStatus]
          )}
        >
          {value}
          {isPercent && value !== "—" ? "%" : null}
        </CardTitle>
        {showDelta ? (
          <CardAction>
            <Badge variant="outline" className={STATUS_TEXT_CLASS[trendStatus]}>
              {trendDown ? <TrendingDownIcon /> : <TrendingUpIcon />}
              {deltaText}
            </Badge>
          </CardAction>
        ) : null}
      </CardHeader>
      <CardFooter className="text-sm text-muted-foreground">
        {medianLabel ?? "No peer data"}
      </CardFooter>
    </Card>
  );
}

export function KpiTilePlaceholder({ label }: { label: string }) {
  return (
    <Card className={CARD_SURFACE}>
      <CardHeader>
        <CardDescription className="truncate">{label}</CardDescription>
        <CardTitle className="text-2xl font-semibold tabular-nums">—</CardTitle>
      </CardHeader>
      <CardFooter className="gap-1.5 text-sm text-muted-foreground">
        <Sparkles className="size-3.5 shrink-0" aria-hidden />
        Coming soon
      </CardFooter>
    </Card>
  );
}

import {
  MetricInfoIcon,
  MetricSublabel,
} from "@/components/widgets/v2/metric-sublabel";
import { Card } from "@/components/ui/card";
import { useSettings } from "@/hooks/use-settings";
import { IC_KPI_DEFS_BY_KEY } from "@/lib/insight/v2/kpi-defs";
import {
  STATUS_BG,
  STATUS_TEXT,
  applyFocusStatus,
  type Status,
} from "@/lib/status";
import { cn } from "@/lib/utils";
import type { IcKpi } from "@/types/insight";

export interface KpiPeerMedian {
  p50: number;
  n: number;
}

export interface KpiTileProps {
  kpi: IcKpi;
  median?: KpiPeerMedian | null;
  onClick?: (metricKey: string) => void;
}

function peerStatusVsMedian(
  value: number,
  median: number,
  higherIsBetter: boolean,
): Status {
  if (!Number.isFinite(value) || !Number.isFinite(median) || median === 0) {
    return "neutral";
  }
  const meetsTarget = higherIsBetter ? value >= median : value <= median;
  return meetsTarget ? "good" : "bad";
}

export function KpiTile({ kpi, median, onClick }: KpiTileProps) {
  const { focusMode } = useSettings();
  const def = IC_KPI_DEFS_BY_KEY[kpi.metric_key];
  const hasValue = kpi.raw_value !== null;
  const hasMedian =
    median != null && Number.isFinite(median.p50) && median.p50 > 0;

  const rawStatus: Status =
    def && hasValue && hasMedian
      ? peerStatusVsMedian(
          kpi.raw_value as number,
          median.p50,
          def.higher_is_better,
        )
      : "neutral";
  const status = applyFocusStatus(rawStatus, focusMode);
  const value = kpi.value ?? "—";

  const showMedian = hasMedian && hasValue && def !== undefined;
  const fillPct = showMedian
    ? Math.max(0, Math.min(1, (kpi.raw_value as number) / median.p50))
    : 0;
  const interactive = Boolean(onClick);
  const medianLabel = showMedian
    ? `vs median ${median.p50}${kpi.unit ? ` ${kpi.unit}` : ""} · ${median.n} peer${median.n === 1 ? "" : "s"}`
    : null;

  return (
    <Card
      data-size="sm"
      render={
        interactive ? (
          <button
            type="button"
            onClick={() => onClick?.(kpi.metric_key)}
            aria-label={`Open ${kpi.label} details`}
          />
        ) : undefined
      }
      className={cn(
        "flex flex-col items-start gap-2 px-4 py-4 text-left",
        interactive && "transition-colors hover:bg-accent",
      )}
    >
      <div className="flex w-full min-w-0 flex-col gap-0.5">
        <div className="flex items-center gap-1">
          <span className="truncate text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
            {kpi.label}
          </span>
          <MetricInfoIcon description={def?.description} />
        </div>
        <MetricSublabel description={def?.description} />
      </div>
      <div className="flex items-baseline gap-1">
        <span
          className={cn(
            "text-2xl font-semibold tabular-nums",
            STATUS_TEXT[status],
          )}
        >
          {value}
        </span>
        {kpi.unit && value !== "—" ? (
          <span className="text-sm text-muted-foreground">{kpi.unit}</span>
        ) : null}
      </div>
      {showMedian && medianLabel ? (
        <div className="mt-auto flex w-full flex-col gap-1.5 pt-1">
          <div className="h-[3px] w-full overflow-hidden rounded-full bg-muted">
            <div
              className={cn("h-full", STATUS_BG[status])}
              style={{ width: `${fillPct * 100}%` }}
            />
          </div>
          <p className="text-[11px] tabular-nums text-muted-foreground">
            {medianLabel}
          </p>
        </div>
      ) : null}
    </Card>
  );
}

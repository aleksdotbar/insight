import {
  MetricInfoIcon,
  MetricSublabel,
} from "@/components/widgets/v2/metric-sublabel";
import { useSettings } from "@/hooks/use-settings";
import { BULLET_DEFS_BY_KEY } from "@/lib/insight/v2/bullet-defs";
import {
  applyFocus,
  PEER_FILL,
  PEER_TEXT,
  peerStatusVsQuartiles,
  type PeerStats,
  type PeerStatusWithNeutral,
} from "@/lib/peers";
import { STATUS_SURFACE } from "@/lib/status";
import { cn } from "@/lib/utils";
import type { BulletMetric } from "@/types/insight";

export type PeerCohortLabel = "team" | "org";

export interface BulletRowProps {
  row: BulletMetric;
  cohortStats?: PeerStats | null;
  cohortLabel?: PeerCohortLabel;
}

function positionText(
  status: PeerStatusWithNeutral,
  cohortLabel: PeerCohortLabel,
): string {
  if (status === "top") return `top 25% in ${cohortLabel}`;
  if (status === "bottom") return `bottom 25% in ${cohortLabel}`;
  if (status === "in_pack") return `middle 50% in ${cohortLabel}`;
  return "no peer data";
}

export function BulletRow({
  row,
  cohortStats,
  cohortLabel = "team",
}: BulletRowProps) {
  const { focusMode } = useSettings();
  const def = BULLET_DEFS_BY_KEY[row.metric_key];
  const higherIsBetter = def?.higher_is_better ?? true;
  const numericValue = Number(row.value);
  const hasNumericValue = Number.isFinite(numericValue);
  const rawPeerStatus: PeerStatusWithNeutral =
    cohortStats && hasNumericValue
      ? peerStatusVsQuartiles(numericValue, cohortStats, higherIsBetter)
      : "neutral";
  const status = applyFocus(rawPeerStatus, focusMode);

  return (
    <div className="grid grid-cols-[1fr_auto] items-baseline gap-x-3 gap-y-1 md:grid-cols-[minmax(0,1fr)_120px_220px]">
      <div className="flex flex-col gap-0.5 md:col-start-1">
        <div className="flex items-center gap-1">
          <p className="text-sm font-medium leading-tight">{row.label}</p>
          <MetricInfoIcon description={def?.description} />
        </div>
        <MetricSublabel description={def?.description} className="md:hidden" />
      </div>
      <span className="text-right text-sm font-semibold tabular-nums md:col-start-2">
        {row.value}
        {row.unit && row.unit !== "" ? (
          <span className="ml-1 text-xs font-normal text-muted-foreground">
            {row.unit}
          </span>
        ) : null}
      </span>
      <div className="col-span-2 md:col-span-1 md:col-start-3">
        {cohortStats && hasNumericValue ? (
          <PeerStrip
            value={numericValue}
            stats={cohortStats}
            higherIsBetter={higherIsBetter}
            status={status}
          />
        ) : (
          <PeerBar
            left={row.bar_left_pct}
            width={row.bar_width_pct}
            median={row.median_left_pct}
            fillClass={PEER_FILL[status]}
          />
        )}
        <p
          className={cn(
            "mt-1 text-[11px] tabular-nums",
            PEER_TEXT[status],
          )}
        >
          {positionText(status, cohortLabel)}
          {row.median_label ? (
            <span className="ml-1 text-muted-foreground">
              · {row.median_label}
            </span>
          ) : null}
        </p>
      </div>
      <div className="hidden md:col-start-1 md:row-start-2 md:block">
        <MetricSublabel description={def?.description} />
      </div>
    </div>
  );
}

function PeerBar({
  left,
  width,
  median,
  fillClass,
}: {
  left: number;
  width: number;
  median: number;
  fillClass: string;
}) {
  return (
    <div
      role="img"
      aria-label={`Position ${left.toFixed(0)}% to ${(left + width).toFixed(0)}%, median ${median.toFixed(0)}%`}
      className="relative h-1.5 w-full rounded-full bg-muted"
    >
      <div
        className={cn("absolute h-full rounded-full", fillClass)}
        style={{ left: `${left}%`, width: `${Math.max(width, 2)}%` }}
      />
      <span
        className="absolute -top-0.5 h-2.5 w-px bg-foreground/60"
        style={{ left: `${median}%` }}
        aria-hidden
      />
    </div>
  );
}

function PeerStrip({
  value,
  stats,
  higherIsBetter,
  status,
}: {
  value: number;
  stats: PeerStats;
  higherIsBetter: boolean;
  status: PeerStatusWithNeutral;
}) {
  const span = Math.max(1e-9, stats.max - stats.min);
  const pct = (v: number) =>
    ((Math.max(stats.min, Math.min(stats.max, v)) - stats.min) / span) * 100;
  const valueLeft = pct(value);
  const p25Left = pct(stats.p25);
  const p50Left = pct(stats.p50);
  const p75Left = pct(stats.p75);

  const bottomZoneClass = higherIsBetter
    ? STATUS_SURFACE.bad
    : STATUS_SURFACE.good;
  const topZoneClass = higherIsBetter
    ? STATUS_SURFACE.good
    : STATUS_SURFACE.bad;

  return (
    <div
      role="img"
      aria-label={`P25 ${stats.p25.toFixed(1)}, P50 ${stats.p50.toFixed(1)}, P75 ${stats.p75.toFixed(1)}, n=${stats.n}`}
      className="relative h-3.5 w-full select-none"
    >
      <div className="absolute inset-x-0 top-1/2 h-3 -translate-y-1/2 overflow-hidden rounded-sm bg-muted">
        <span
          className={cn("absolute inset-y-0 left-0", bottomZoneClass)}
          style={{ width: `${p25Left}%` }}
        />
        <span
          className={cn("absolute inset-y-0", topZoneClass)}
          style={{ left: `${p75Left}%`, right: 0 }}
        />
      </div>
      <span
        className={cn(
          "absolute top-1/2 h-1.5 -translate-y-1/2 rounded-sm",
          PEER_FILL[status],
        )}
        style={{ left: 0, width: `${valueLeft}%` }}
      />
      <span
        className="absolute top-1/2 h-3.5 w-0.5 -translate-x-1/2 -translate-y-1/2 bg-foreground/80"
        style={{ left: `${p50Left}%` }}
        aria-hidden
      />
    </div>
  );
}

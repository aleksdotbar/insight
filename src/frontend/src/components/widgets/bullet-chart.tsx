import { Minus, TrendingDown, TrendingUp } from "lucide-react";
import { memo, type KeyboardEvent } from "react";

import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { getPeriodSuffix } from "@/lib/insight/period-suffix";
import type { BulletMetric } from "@/types/insight";

import { ComingSoon } from "./coming-soon";
import { ProgressTrack } from "./progress-track";

type KnownStatus = "good" | "warn" | "bad";

const STATUS_BAR_CLASS: Record<KnownStatus, string> = {
  good: "bg-success",
  warn: "bg-warning",
  bad: "bg-destructive",
};

const STATUS_BADGE_CLASS: Record<KnownStatus, string> = {
  good: "bg-success/10 text-success",
  warn: "bg-warning/10 text-warning",
  bad: "bg-destructive/10 text-destructive",
};

const STATUS_ARROW: Record<KnownStatus, typeof TrendingUp> = {
  good: TrendingUp,
  warn: Minus,
  bad: TrendingDown,
};

export interface BulletChartProps {
  metric: BulletMetric & { period?: string };
  onDrillClick?: (drillId: string) => void;
  mode?: "chart" | "tile";
  personName?: string;
}

function BulletChartImpl({
  metric,
  onDrillClick,
  mode = "chart",
  personName,
}: BulletChartProps) {
  const {
    label,
    sublabel,
    value,
    unit,
    bar_left_pct,
    bar_width_pct,
    median_left_pct,
    median_label,
    range_min,
    range_max,
    status,
    drill_id,
    period,
  } = metric;

  const suffix = getPeriodSuffix(unit, period);
  const isDrillable = Boolean(drill_id);

  const handleDrillClick = (): void => {
    if (isDrillable && onDrillClick) onDrillClick(drill_id);
  };

  const handleKeyDown = (e: KeyboardEvent): void => {
    if ((e.key === "Enter" || e.key === " ") && isDrillable) {
      e.preventDefault();
      handleDrillClick();
    }
  };

  const fullSublabel = sublabel
    ? personName
      ? `${sublabel} · ${personName}`
      : sublabel
    : undefined;

  const isUnavailable = status === "unavailable";
  const knownStatus: KnownStatus = isUnavailable ? "warn" : status;

  if (mode === "tile") {
    return (
      <div
        className={cn(
          "bg-muted/50 rounded-lg px-3.5 py-3",
          isDrillable
            ? "focus-visible:ring-ring cursor-pointer focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:outline-none"
            : "cursor-default",
        )}
        role={isDrillable ? "button" : undefined}
        tabIndex={isDrillable ? 0 : undefined}
        onClick={isDrillable ? handleDrillClick : undefined}
        onKeyDown={isDrillable ? handleKeyDown : undefined}
        aria-label={isDrillable ? `${label}: ${value} ${unit}` : undefined}
      >
        <div className="text-muted-foreground mb-0.5 text-sm font-medium">
          {label}
        </div>
        <div className="mb-1 flex items-baseline gap-0.5">
          <span className="text-foreground text-2xl font-extrabold tracking-tight">
            {value}
          </span>
          {unit ? (
            <span className="text-muted-foreground text-sm">{unit}</span>
          ) : null}
          {suffix ? (
            <span className="text-muted-foreground text-xs">{suffix}</span>
          ) : null}
        </div>
        {isUnavailable ? (
          <ComingSoon variant="chip" />
        ) : (
          <Badge
            variant="secondary"
            className={cn("gap-1 text-sm font-semibold", STATUS_BADGE_CLASS[knownStatus])}
          >
            {(() => {
              const Icon = STATUS_ARROW[knownStatus];
              return <Icon className="size-3.5" />;
            })()}
            {median_label}
          </Badge>
        )}
      </div>
    );
  }

  return (
    <div>
      <div className="flex items-start justify-between">
        <div>
          <span className="text-foreground text-base leading-snug font-semibold">
            {label}
          </span>
          {fullSublabel ? (
            <div className="text-muted-foreground text-xs leading-tight font-normal">
              {fullSublabel}
            </div>
          ) : null}
        </div>
        <div className="text-right">
          <span
            className={cn(
              "text-foreground text-xl leading-none font-extrabold",
              isDrillable
                ? "hover:decoration-primary focus-visible:ring-ring cursor-pointer rounded-sm hover:underline hover:decoration-dotted hover:underline-offset-4 focus-visible:ring-2 focus-visible:ring-offset-1 focus-visible:outline-none"
                : "cursor-default",
            )}
            role={isDrillable ? "button" : undefined}
            tabIndex={isDrillable ? 0 : undefined}
            onClick={isDrillable ? handleDrillClick : undefined}
            onKeyDown={isDrillable ? handleKeyDown : undefined}
            aria-label={isDrillable ? `Drill into ${label}` : undefined}
          >
            {value}
          </span>
          {unit ? (
            <span className="text-muted-foreground ml-1 text-sm">{unit}</span>
          ) : null}
          {suffix ? (
            <span className="text-muted-foreground ml-0.5 text-xs">
              {suffix}
            </span>
          ) : null}
        </div>
      </div>

      {isUnavailable ? (
        <div className="mt-1">
          <ComingSoon variant="row" />
        </div>
      ) : (
        <>
          <ProgressTrack
            barLeftPct={bar_left_pct}
            barWidthPct={bar_width_pct}
            medianLeftPct={median_left_pct}
            barColorClass={STATUS_BAR_CLASS[knownStatus]}
          />
          <div className="text-muted-foreground mt-0.5 flex justify-between text-xs">
            <span>{range_min}</span>
            <span className="text-foreground font-medium">{median_label}</span>
            <span>{range_max}</span>
          </div>
        </>
      )}
    </div>
  );
}

export const BulletChart = memo(BulletChartImpl);

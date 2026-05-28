import { useState } from "react";
import { ChevronDown, ChevronRight } from "lucide-react";

import { ComingSoon } from "@/components/widgets/coming-soon";
import { MetricSublabel } from "@/components/widgets/v2/metric-sublabel";
import { Card, CardContent } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { useSettings } from "@/hooks/use-settings";
import { STATUS_TEXT, applyFocusStatus, type Status } from "@/lib/status";
import { SECTION_STRIPE } from "@/lib/scoring";
import { cn } from "@/lib/utils";

export interface BreakdownItem {
  label: string;
  value: number;
  unit?: string;
}

export interface SummaryWithBreakdownProps {
  label: string;
  description?: string;
  value: number;
  unit?: string;
  status?: Status;
  breakdown: BreakdownItem[];
  breakdownLabel?: string;
  defaultOpen?: boolean;
  isPending?: boolean;
  isError?: boolean;
  onRetry?: () => void;
}

const SEGMENT_CLASSES = [
  "bg-chart-1",
  "bg-chart-2",
  "bg-chart-3",
  "bg-chart-4",
  "bg-chart-5",
];

function formatNum(v: number, unit?: string): string {
  if (!Number.isFinite(v)) return "—";
  if (unit === "h" || unit === "d") {
    return v >= 10 ? Math.round(v).toString() : v.toFixed(1).replace(/\.0$/, "");
  }
  return Math.round(v).toString();
}

export function SummaryWithBreakdown({
  label,
  description,
  value,
  unit,
  status: statusProp = "neutral",
  breakdown,
  breakdownLabel = "Breakdown",
  defaultOpen = false,
  isPending,
  isError,
  onRetry,
}: SummaryWithBreakdownProps) {
  const [open, setOpen] = useState(defaultOpen);
  const { focusMode } = useSettings();
  const status = applyFocusStatus(statusProp, focusMode);
  const breakdownTotal = breakdown.reduce((s, b) => s + b.value, 0) || 1;
  const stripeClass =
    status === "neutral" ? "border-l-border" : SECTION_STRIPE[status];

  if (isPending) {
    return <Skeleton className="h-32 w-full rounded-lg" />;
  }
  if (isError) {
    return (
      <ComingSoon
        state="error"
        label={`${label} — unable to load`}
        onRetry={onRetry}
      />
    );
  }

  return (
    <Card className={cn("border-l-2", stripeClass)}>
      <CardContent className="flex flex-col gap-3 pt-6">
        <div className="flex items-baseline justify-between gap-4">
          <div className="flex min-w-0 flex-col gap-1">
            <span className="truncate text-sm font-semibold">{label}</span>
            <MetricSublabel description={description} />
            <span
              className={cn(
                "text-xs uppercase tracking-wider",
                STATUS_TEXT[status],
              )}
            >
              {status === "neutral" ? "no data" : status}
            </span>
          </div>
          <span className="flex items-baseline gap-1 tabular-nums">
            <span className="text-3xl font-semibold sm:text-4xl">
              {formatNum(value, unit)}
            </span>
            {unit ? (
              <span className="text-sm text-muted-foreground">{unit}</span>
            ) : null}
          </span>
        </div>

        {breakdown.length > 0 ? (
          <>
            <button
              type="button"
              onClick={() => setOpen((v) => !v)}
              className="flex items-center gap-1.5 text-left text-xs font-medium text-muted-foreground transition-colors hover:text-foreground"
              aria-expanded={open}
            >
              {open ? (
                <ChevronDown className="size-3.5" />
              ) : (
                <ChevronRight className="size-3.5" />
              )}
              <span>{breakdownLabel}</span>
            </button>
            {open ? (
              <div className="flex flex-col gap-2">
                <div className="flex h-3 w-full overflow-hidden rounded-sm bg-muted">
                  {breakdown.map((b, i) => {
                    const width = (b.value / breakdownTotal) * 100;
                    return (
                      <span
                        key={b.label}
                        className={cn(
                          "h-full",
                          SEGMENT_CLASSES[i % SEGMENT_CLASSES.length],
                        )}
                        style={{ width: `${width}%` }}
                        title={`${b.label}: ${formatNum(b.value, b.unit)}${b.unit ? ` ${b.unit}` : ""}`}
                      />
                    );
                  })}
                </div>
                <ul className="flex flex-col gap-1 text-xs">
                  {breakdown.map((b, i) => (
                    <li
                      key={b.label}
                      className="flex items-center justify-between gap-2"
                    >
                      <span className="flex min-w-0 items-center gap-1.5">
                        <span
                          className={cn(
                            "inline-block size-2.5 shrink-0 rounded-sm",
                            SEGMENT_CLASSES[i % SEGMENT_CLASSES.length],
                          )}
                        />
                        <span className="truncate">{b.label}</span>
                      </span>
                      <span className="shrink-0 whitespace-nowrap font-medium tabular-nums text-muted-foreground">
                        {formatNum(b.value, b.unit)}
                        {b.unit ? ` ${b.unit}` : ""}
                      </span>
                    </li>
                  ))}
                </ul>
              </div>
            ) : null}
          </>
        ) : null}
      </CardContent>
    </Card>
  );
}

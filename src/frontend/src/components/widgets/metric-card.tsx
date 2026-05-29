import { memo } from "react";

import { Card, CardContent } from "@/components/ui/card";
import { cn } from "@/lib/utils";
import type { BulletMetric } from "@/types/insight";

import { BulletChart } from "./bullet-chart";
import { ComingSoon } from "./coming-soon";

export interface MetricColumnGroup {
  heading: string;
  metrics: Array<BulletMetric & { period?: string }>;
}

export interface MetricCardProps {
  title: string;
  metrics: Array<BulletMetric & { period?: string }>;
  columns?: 1 | 2 | 3;
  /**
   * Pre-filtered, headed column groups for chart mode (e.g. collaboration's
   * Chat / Email / Meetings / Files). When set, overrides the round-robin
   * `columns` split. Ignored in tile mode.
   */
  groups?: MetricColumnGroup[];
  onDrillClick?: (drillId: string) => void;
  mode?: "chart" | "tile";
  personName?: string;
  errored?: boolean;
  loading?: boolean;
  revalidating?: boolean;
  onRetry?: () => void;
}

const GRID_COLS_CLASS = {
  1: "grid-cols-1",
  2: "grid-cols-1 sm:grid-cols-2",
  3: "grid-cols-1 sm:grid-cols-2 md:grid-cols-3",
  4: "grid-cols-1 sm:grid-cols-2 md:grid-cols-4",
} as const;

function ChartLegend() {
  return (
    <div className="mt-2 flex items-center gap-4">
      <div className="flex items-center gap-1">
        <div className="bg-foreground/60 h-3 w-[2px] rounded" />
        <span className="text-muted-foreground text-xs">Team median</span>
      </div>
      <div className="flex items-center gap-1">
        <div className="from-success via-warning to-destructive h-1.5 w-4 rounded bg-gradient-to-r" />
        <span className="text-muted-foreground text-xs">
          Result · color = vs target
        </span>
      </div>
    </div>
  );
}

function MetricCardImpl({
  title,
  metrics,
  columns = 1,
  groups,
  onDrillClick,
  mode = "chart",
  personName,
  errored = false,
  loading = false,
  revalidating = false,
  onRetry,
}: MetricCardProps) {
  const revalidateClass = revalidating
    ? "opacity-70 transition-opacity duration-300"
    : "opacity-100 transition-opacity duration-300";
  const placeholderState = loading ? "loading" : errored ? "error" : "empty";
  const isEmpty = metrics.length === 0;

  if (mode === "tile") {
    return (
      <Card>
        <CardContent className={cn("px-3.5 py-3", revalidateClass)}>
          <div className="text-muted-foreground mb-2 text-xs font-bold tracking-wide uppercase">
            {title}
          </div>
          {isEmpty ? (
            <ComingSoon
              variant="card"
              state={placeholderState}
              onRetry={onRetry}
            />
          ) : (
            <div className="grid grid-cols-2 gap-2 sm:grid-cols-3">
              {metrics.map((metric) => (
                <BulletChart
                  key={metric.metric_key}
                  metric={metric}
                  onDrillClick={onDrillClick}
                  mode="tile"
                  personName={personName}
                />
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    );
  }

  const cols = columns ?? 1;
  const columnGroups: Array<Array<BulletMetric & { period?: string }>> =
    Array.from({ length: cols }, () => []);
  metrics.forEach((m, i) => {
    columnGroups[i % cols]!.push(m);
  });

  return (
    <Card>
      <CardContent className={cn("px-3.5 py-3", revalidateClass)}>
        <div className="flex items-start justify-between">
          <span className="text-muted-foreground text-xs font-bold tracking-wide uppercase">
            {title}
          </span>
          <span className="text-muted-foreground text-xs">vs team range</span>
        </div>

        {!isEmpty ? <ChartLegend /> : null}

        {isEmpty ? (
          <div className="mt-3">
            <ComingSoon
              variant="card"
              state={placeholderState}
              onRetry={onRetry}
            />
          </div>
        ) : groups ? (
          <div
            className={cn(
              "mt-3 grid gap-x-3.5 gap-y-4",
              GRID_COLS_CLASS[
                Math.min(groups.length, 4) as 1 | 2 | 3 | 4
              ],
            )}
          >
            {groups.map((group) => (
              <div key={group.heading} className="flex flex-col gap-4">
                <div className="text-muted-foreground text-xs font-bold tracking-wide uppercase">
                  {group.heading}
                </div>
                {group.metrics.map((metric) => (
                  <BulletChart
                    key={metric.metric_key}
                    metric={metric}
                    onDrillClick={onDrillClick}
                    mode="chart"
                    personName={personName}
                  />
                ))}
              </div>
            ))}
          </div>
        ) : (
          <div className={cn("mt-3 grid gap-3.5", GRID_COLS_CLASS[cols])}>
            {columnGroups.map((colMetrics, colIdx) => (
              <div key={colIdx} className="flex flex-col gap-4">
                {colMetrics.map((metric) => (
                  <BulletChart
                    key={metric.metric_key}
                    metric={metric}
                    onDrillClick={onDrillClick}
                    mode="chart"
                    personName={personName}
                  />
                ))}
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  );
}

export const MetricCard = memo(MetricCardImpl);

import {
  Bar,
  BarChart,
  CartesianGrid,
  ReferenceLine,
  XAxis,
  YAxis,
} from "recharts";

import { MetricSublabel } from "@/components/widgets/v2/metric-sublabel";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "@/components/ui/chart";
import { useSettings } from "@/hooks/use-settings";
import { BULLET_DEFS_BY_KEY } from "@/lib/insight/v2/bullet-defs";
import {
  applyFocus,
  PEER_FILL,
  PEER_TEXT,
  peerStatusVsQuartiles,
  type PeerCohortLabel,
  type PeerStats,
  type PeerStatusWithNeutral,
} from "@/lib/peers";
import { STATUS_SURFACE } from "@/lib/status";
import { cn } from "@/lib/utils";
import type { HistogramBin } from "@/queries/v2/ic-extras";
import type { BulletMetric } from "@/types/insight";

const CHART_CONFIG: ChartConfig = {
  count: { label: "Count", color: "var(--chart-1)" },
};

function positionText(
  status: PeerStatusWithNeutral,
  cohortLabel: PeerCohortLabel,
): string {
  if (status === "top") return `top 25% in ${cohortLabel}`;
  if (status === "bottom") return `bottom 25% in ${cohortLabel}`;
  if (status === "in_pack") return `middle 50% in ${cohortLabel}`;
  return "no peer data";
}

export interface DistributionStripProps {
  row: BulletMetric;
  bins?: HistogramBin[] | null;
  cohortStats?: PeerStats | null;
  cohortLabel?: PeerCohortLabel;
}

export function DistributionStrip({
  row,
  bins,
  cohortStats,
  cohortLabel = "team",
}: DistributionStripProps) {
  const { focusMode } = useSettings();
  const def = BULLET_DEFS_BY_KEY[row.metric_key];
  const title = def?.label ?? row.label;
  const unit = row.unit || def?.unit || "";
  const medianRaw = Number(row.median);
  const median = Number.isFinite(medianRaw) ? medianRaw : undefined;

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-semibold">{title}</CardTitle>
        {median != null ? (
          <CardDescription className="text-xs">
            Median {median}
            {unit}
          </CardDescription>
        ) : null}
        <MetricSublabel description={def?.description} className="text-xs" />
      </CardHeader>
      <CardContent>
        {bins && bins.length > 0 ? (
          <HistogramBody bins={bins} unit={unit} median={median} />
        ) : (
          <PeerBody
            row={row}
            unit={unit}
            cohortStats={cohortStats}
            cohortLabel={cohortLabel}
            focusMode={focusMode}
          />
        )}
      </CardContent>
    </Card>
  );
}

function HistogramBody({
  bins,
  unit,
  median,
}: {
  bins: HistogramBin[];
  unit: string;
  median?: number;
}) {
  const data = bins.map((b) => ({
    bucket: `${b.bin}${unit}`,
    bin: b.bin,
    count: b.count,
  }));
  return (
    <ChartContainer config={CHART_CONFIG} className="h-32 w-full">
      <BarChart data={data} margin={{ top: 4, right: 4, left: 0, bottom: 0 }}>
        <CartesianGrid vertical={false} stroke="var(--border)" />
        <XAxis
          dataKey="bucket"
          tickLine={false}
          axisLine={false}
          tick={{ fontSize: 10, fill: "var(--muted-foreground)" }}
        />
        <YAxis hide />
        <ChartTooltip content={<ChartTooltipContent hideIndicator />} />
        <Bar dataKey="count" fill="var(--color-count)" radius={3} />
        {median != null ? (
          <ReferenceLine
            x={`${median}${unit}`}
            stroke="var(--foreground)"
            strokeDasharray="3 3"
          />
        ) : null}
      </BarChart>
    </ChartContainer>
  );
}

function PeerBody({
  row,
  unit,
  cohortStats,
  cohortLabel,
  focusMode,
}: {
  row: BulletMetric;
  unit: string;
  cohortStats?: PeerStats | null;
  cohortLabel: PeerCohortLabel;
  focusMode: ReturnType<typeof useSettings>["focusMode"];
}) {
  const def = BULLET_DEFS_BY_KEY[row.metric_key];
  const higherIsBetter = def?.higher_is_better ?? true;
  const numericValue = Number(row.value);
  const hasNumericValue = Number.isFinite(numericValue);
  const rawStatus: PeerStatusWithNeutral =
    cohortStats && hasNumericValue
      ? peerStatusVsQuartiles(numericValue, cohortStats, higherIsBetter)
      : "neutral";
  const status = applyFocus(rawStatus, focusMode);

  return (
    <div className="flex h-32 flex-col justify-center gap-3">
      <div className="flex items-baseline gap-1">
        <span className="text-2xl font-semibold tabular-nums">{row.value}</span>
        {unit ? (
          <span className="text-sm text-muted-foreground">{unit}</span>
        ) : null}
      </div>
      {cohortStats && hasNumericValue ? (
        <div className="flex flex-col gap-1">
          <PeerStrip
            value={numericValue}
            stats={cohortStats}
            higherIsBetter={higherIsBetter}
            status={status}
          />
          <p className={cn("text-[11px] tabular-nums", PEER_TEXT[status])}>
            {positionText(status, cohortLabel)}
          </p>
        </div>
      ) : (
        <p className="text-[11px] text-muted-foreground">no peer data</p>
      )}
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
  const p50Left = pct(stats.p50);
  const p25Left = pct(stats.p25);
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

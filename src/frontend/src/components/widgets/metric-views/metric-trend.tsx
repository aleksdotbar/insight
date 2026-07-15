import { format } from "date-fns";

import type { MetricBucket, MetricFormat } from "@/api/metric-results-client";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  BarChart,
  CartesianGrid,
  ChartBar,
  ChartContainer,
  ChartLine,
  ChartTooltip,
  ChartTooltipContent,
  LineChart,
  XAxis,
  YAxis,
  type ChartConfig,
} from "@/components/ui/chart";
import {
  dimensionColorSeed,
  dimensionLabel,
  dimensionSeriesKey,
  safeSeriesKey,
} from "@/components/widgets/metric-views/dimension-series";
import { formatMetricNumber } from "@/lib/format";
import { forEntity, type NormalizedMetricResult } from "@/lib/metrics/collection";
import { percentShareLabels } from "@/lib/metrics/shares";
import { seriesColors } from "@/lib/series-colors";

export interface MetricTrendProps {
  /** One metric → one series per dimension group; several → one per metric. */
  metrics: NormalizedMetricResult[];
  entityId: string;
  chart: "line" | "stacked-bar";
}

type TrendPoint = {
  date: string;
  label: string;
  tooltipLabel: string;
  [series: string]: number | string;
};

type TrendSeries = {
  key: string;
  colorSeed: string;
  label: string;
  total: number;
};

function num(value: number | null | undefined): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function parseRangeDate(value: string): Date | null {
  const parts = value.split("-").map(Number);
  if (parts.length !== 3) return null;
  const [year, month, day] = parts;
  if (!year || !month || !day) return null;
  const date = new Date(year, month - 1, day);
  return date.getFullYear() === year &&
    date.getMonth() === month - 1 &&
    date.getDate() === day
    ? date
    : null;
}

function formatDateLabel(value: string, pattern: string): string {
  const date = parseRangeDate(value);
  return date ? format(date, pattern) : value;
}

const BUCKET_LABEL: Record<MetricBucket, string> = {
  day: "Daily",
  week: "Weekly",
  month: "Monthly",
};

function labelPatterns(bucket: MetricBucket): { tick: string; tooltip: string } {
  return bucket === "month"
    ? { tick: "MMM yyyy", tooltip: "MMMM yyyy" }
    : { tick: "MMM d", tooltip: "MMMM d, yyyy" };
}

interface FlatSeries {
  key: string;
  colorSeed: string;
  label: string;
  points: Array<{ bucket_start: string; value: number | null }>;
}

/**
 * Flatten the block's metrics into chart series: a single metric splits by
 * its response dimension groups; multiple metrics chart one series each
 * (dimension groups folded into the metric total).
 */
function flatten(
  metrics: NormalizedMetricResult[],
  entityId: string,
): { flat: FlatSeries[]; bucket: MetricBucket; dimensionKeys: string[] } {
  const multi = metrics.length > 1;
  const flat: FlatSeries[] = [];
  let bucket: MetricBucket = "day";
  const dimensionKeys = new Set<string>();

  for (const metric of metrics) {
    const data = forEntity(metric, entityId);
    if (data.bucket) bucket = data.bucket;
    if (multi) {
      const merged = new Map<string, number>();
      for (const item of data.series) {
        for (const point of item.points) {
          if (point.value == null) continue;
          merged.set(
            point.bucket_start,
            (merged.get(point.bucket_start) ?? 0) + point.value,
          );
        }
      }
      flat.push({
        key: safeSeriesKey(metric.metric_key),
        colorSeed: metric.metric_key,
        label: metric.label,
        points: [...merged.entries()].map(([bucket_start, value]) => ({
          bucket_start,
          value,
        })),
      });
      continue;
    }
    for (const item of data.series) {
      for (const dimension of item.dimensions) dimensionKeys.add(dimension.key);
      flat.push({
        key: dimensionSeriesKey(item.dimensions),
        colorSeed: dimensionColorSeed(item.dimensions),
        // A dimensionless single series would read "Total"; name it after the
        // metric so the tooltip says "Commits", not "Total".
        label:
          item.dimensions.length > 0
            ? dimensionLabel(item.dimensions)
            : metric.label,
        points: item.points,
      });
    }
  }

  return { flat, bucket, dimensionKeys: [...dimensionKeys] };
}

function buildSeries(flat: FlatSeries[]): TrendSeries[] {
  const totals = new Map<string, TrendSeries>();
  for (const item of flat) {
    const entry = totals.get(item.key) ?? {
      key: item.key,
      colorSeed: item.colorSeed,
      label: item.label,
      total: 0,
    };
    entry.total += item.points.reduce((sum, point) => sum + num(point.value), 0);
    totals.set(item.key, entry);
  }
  return [...totals.values()].sort((a, b) => b.total - a.total);
}

function buildPoints(
  flat: FlatSeries[],
  bucket: MetricBucket,
): TrendPoint[] {
  const { tick, tooltip } = labelPatterns(bucket);
  // A weekly bucket_start is the start of the week, not a single day — say so
  // in the tooltip so "April 13" doesn't read as one day's total.
  const tooltipPrefix = bucket === "week" ? "Week of " : "";
  const byDate = new Map<string, TrendPoint>();
  for (const item of flat) {
    for (const value of item.points) {
      // The wire distinguishes null ("not measured/derivable") from 0; a
      // null point renders as a gap, never a fabricated zero.
      if (value.value == null) continue;
      let point = byDate.get(value.bucket_start);
      if (!point) {
        point = {
          date: value.bucket_start,
          label: formatDateLabel(value.bucket_start, tick),
          tooltipLabel: `${tooltipPrefix}${formatDateLabel(value.bucket_start, tooltip)}`,
        };
        byDate.set(value.bucket_start, point);
      }
      point[item.key] = num(point[item.key] as number) + value.value;
    }
  }
  return [...byDate.values()].sort((a, b) =>
    String(a.date) < String(b.date) ? -1 : 1,
  );
}

export function MetricTrend({ metrics, entityId, chart }: MetricTrendProps) {
  const title =
    metrics.length === 1
      ? `${metrics[0]?.label} over time`
      : metrics.map((metric) => metric.label).join(" & ");

  const { flat, bucket, dimensionKeys } = flatten(metrics, entityId);
  const series = buildSeries(flat);
  const data = buildPoints(flat, bucket);

  if (data.length === 0 || series.length === 0) {
    return (
      <Card className="min-h-80 shrink-0">
        <CardHeader>
          <CardTitle className="text-sm font-semibold">{title}</CardTitle>
          <CardDescription>No trend data yet.</CardDescription>
        </CardHeader>
      </Card>
    );
  }

  const colorsBySeed = seriesColors(series.map((item) => item.colorSeed));
  const config: ChartConfig = Object.fromEntries(
    series.map((item) => [
      item.key,
      { label: item.label, color: colorsBySeed[item.colorSeed] },
    ]),
  );
  const description =
    dimensionKeys.length > 0
      ? `${BUCKET_LABEL[bucket]} by ${dimensionKeys.join(" / ")}`
      : BUCKET_LABEL[bucket];

  // A single metric split by a dimension is a composition — its series are
  // parts of one whole, so the legend can carry each part's period total and
  // share (folding in the standalone breakdown chart). Multiple metrics are
  // not a whole (no honest %), so they get labels only.
  const isComposition = metrics.length === 1 && dimensionKeys.length > 0;
  const compositionTotal = series.reduce((sum, item) => sum + item.total, 0);
  const legendFormat: MetricFormat = metrics[0]?.format ?? "integer";
  const legendUnit = metrics[0]?.unit ? ` ${metrics[0].unit}` : "";
  // For a composition, each part gets a share label summing to exactly 100
  // (largest-remainder rounding — the legend must never read 99%/101%; a
  // nonzero part never reads 0%, gaining a decimal instead). legendShares[i]
  // labels series[i]. A non-composition has no whole → [].
  const legendShares = isComposition
    ? percentShareLabels(series.map((item) => item.total))
    : [];

  const axes = (
    <>
      <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
      <XAxis
        dataKey="label"
        tick={{ fontSize: 10, fill: "var(--muted-foreground)" }}
        tickLine={false}
        axisLine={false}
        interval="preserveStartEnd"
      />
      <YAxis
        tick={{ fontSize: 10, fill: "var(--muted-foreground)" }}
        tickLine={false}
        axisLine={false}
      />
      <ChartTooltip
        content={
          <ChartTooltipContent
            className="min-w-48"
            labelFormatter={(_, payload) =>
              String(payload?.[0]?.payload?.tooltipLabel ?? "")
            }
          />
        }
      />
    </>
  );

  return (
    <Card className="shrink-0">
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-semibold">{title}</CardTitle>
        <CardDescription className="text-xs">{description}</CardDescription>
      </CardHeader>
      <CardContent>
        <ChartContainer config={config} className="h-56 min-h-56 w-full">
          {chart === "stacked-bar" ? (
            <BarChart
              data={data}
              margin={{ top: 8, right: 8, left: 0, bottom: 0 }}
            >
              {axes}
              {series.map((item) => (
                <ChartBar
                  key={item.key}
                  dataKey={item.key}
                  stackId="metric"
                  fill={`var(--color-${item.key})`}
                  name={item.label}
                  radius={[2, 2, 0, 0]}
                />
              ))}
            </BarChart>
          ) : (
            <LineChart
              data={data}
              margin={{ top: 8, right: 8, left: 0, bottom: 0 }}
            >
              {axes}
              {series.map((item) => (
                <ChartLine
                  key={item.key}
                  type="monotone"
                  dataKey={item.key}
                  stroke={`var(--color-${item.key})`}
                  strokeWidth={2}
                  dot={false}
                  name={item.label}
                />
              ))}
            </LineChart>
          )}
        </ChartContainer>
        {series.length > 1 ? (
          <ul className="mt-3 flex flex-wrap gap-x-6 gap-y-1.5">
            {series.map((item, index) => (
              <li key={item.key} className="flex items-center gap-2 text-xs">
                <span
                  aria-hidden
                  className="size-2.5 shrink-0 rounded-[3px]"
                  style={{ backgroundColor: colorsBySeed[item.colorSeed] }}
                />
                <span className="font-medium">{item.label}</span>
                {isComposition ? (
                  <span className="tabular-nums text-muted-foreground">
                    {formatMetricNumber(item.total, legendFormat)}
                    {legendUnit}
                    {compositionTotal > 0 ? ` · ${legendShares[index]}%` : ""}
                  </span>
                ) : null}
              </li>
            ))}
          </ul>
        ) : null}
      </CardContent>
    </Card>
  );
}

import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  dimensionColorSeed,
  dimensionLabel,
  dimensionSeriesKey,
} from "@/components/widgets/metric-views/dimension-series";
import { formatMetricValue } from "@/lib/format";
import { forEntity, type NormalizedMetricResult } from "@/lib/metrics/collection";
import { swatchPalette } from "@/lib/swatch-palette";

export interface MetricBreakdownProps {
  metric: NormalizedMetricResult;
  entityId: string;
}

function num(value: number | null | undefined): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

/** Proportional composition strip over the breakdown view's dimension groups. */
export function MetricBreakdown({ metric, entityId }: MetricBreakdownProps) {
  const rows = forEntity(metric, entityId)
    .breakdown.filter((row) => num(row.value) > 0)
    .map((row) => ({
      key: dimensionSeriesKey(row.dimensions),
      colorSeed: dimensionColorSeed(row.dimensions),
      label: dimensionLabel(row.dimensions),
      value: num(row.value),
    }))
    .sort((a, b) => b.value - a.value);

  if (rows.length === 0) {
    return (
      <Card className="min-h-40 shrink-0">
        <CardHeader>
          <CardTitle className="text-sm font-semibold">{metric.label}</CardTitle>
          <CardDescription>No composition data yet.</CardDescription>
        </CardHeader>
      </Card>
    );
  }

  const dimensions = metric.breakdown?.dimensions ?? [];
  const colorsBySeed = swatchPalette(rows.map((row) => row.colorSeed));
  const total = rows.reduce((sum, row) => sum + row.value, 0);

  return (
    <Card className="shrink-0">
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-semibold">{metric.label}</CardTitle>
        <CardDescription className="text-xs">
          {dimensions.length > 0
            ? `Period total by ${dimensions.join(" / ")}`
            : "Period total"}
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="flex h-24 w-full overflow-hidden rounded-md bg-muted md:h-28">
          {rows.map((row) => {
            const pct = total > 0 ? (row.value / total) * 100 : 0;
            const formatted = formatMetricValue(
              row.value,
              metric.format,
              metric.unit,
            );
            return (
              <div
                key={row.key}
                className="min-w-0 border-r-2 border-background p-3 last:border-r-0"
                style={{
                  width: `${pct}%`,
                  backgroundColor: colorsBySeed[row.colorSeed],
                  color: "var(--swatch-fg)",
                }}
                title={`${row.label}: ${formatted}`}
              >
                <div className="truncate text-sm font-semibold">{row.label}</div>
                <div className="mt-1 text-xs leading-4 opacity-90">
                  {Math.round(pct)}%
                </div>
                <div className="text-xs leading-4 opacity-90">{formatted}</div>
              </div>
            );
          })}
        </div>
      </CardContent>
    </Card>
  );
}

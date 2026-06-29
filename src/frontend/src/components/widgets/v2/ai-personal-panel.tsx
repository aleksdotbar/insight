import { format } from "date-fns";
import type { DateRange } from "@/api/period-to-date-range";

import { ComingSoon } from "@/components/widgets/coming-soon";
import type { CatalogMetric } from "@/api/catalog-client";
import { useCatalog } from "@/api/use-catalog";
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
  ChartLegend,
  ChartLegendContent,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
  XAxis,
  YAxis,
} from "@/components/ui/chart";
import { Skeleton } from "@/components/ui/skeleton";
import {
  PeerStorySection,
  type PeerStoryInput,
} from "@/components/widgets/v2/peer-story-section";
import type { PeerStats } from "@/lib/peers";
import { swatchPalette } from "@/lib/swatch-palette";
import {
  useIcAiPeerCounters,
  useIcAiToolSummary,
  useIcAiToolTrend,
  type AiPeerCounterRow,
  type AiToolSummaryRow,
  type AiToolTrendRow,
} from "@/queries/v2/ic-extras";

interface AiPersonalPanelProps {
  personId: string | null | undefined;
  range: DateRange | null | undefined;
}

function num(v: number | null | undefined): number {
  return typeof v === "number" && Number.isFinite(v) ? v : 0;
}

type AiTool = {
  key: string;
  name: string;
};

function toolsByAcceptedLines(rows: AiToolSummaryRow[]): AiTool[] {
  return [...rows]
    .sort((a, b) => num(b.accepted_lines_added) - num(a.accepted_lines_added))
    .map((r) => ({ key: r.tool, name: r.tool_name }));
}

type AiAcceptedTrendPoint = {
  date: string;
  label: string;
  tooltipLabel: string;
  [tool: string]: number | string;
};

type TrendGrain = "day" | "week" | "month";

function dateKey(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function addDays(date: Date, days: number): Date {
  const next = new Date(date);
  next.setUTCDate(next.getUTCDate() + days);
  return next;
}

function parseRangeDate(value: string): Date | null {
  const date = new Date(`${value}T00:00:00.000Z`);
  return Number.isNaN(date.getTime()) ? null : date;
}

function daysInRange(range: DateRange): number {
  const start = parseRangeDate(range.from);
  const end = parseRangeDate(range.to);
  if (!start || !end || start > end) return 0;
  return Math.floor((end.getTime() - start.getTime()) / 86_400_000) + 1;
}

function trendGrain(range: DateRange): TrendGrain {
  const days = daysInRange(range);
  if (days <= 100) return "day";
  return "month";
}

function weekStartKey(value: string): string {
  const date = parseRangeDate(value);
  if (!date) return value;
  const day = date.getUTCDay();
  const daysSinceMonday = (day + 6) % 7;
  return dateKey(addDays(date, -daysSinceMonday));
}

function monthStartKey(value: string): string {
  const date = parseRangeDate(value);
  if (!date) return value;
  return dateKey(new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1)));
}

function trendBucketKey(value: string, grain: TrendGrain): string {
  if (grain === "week") return weekStartKey(value);
  if (grain === "month") return monthStartKey(value);
  return value;
}

function enumerateBucketKeys(range: DateRange, grain: TrendGrain): string[] {
  const start = new Date(`${range.from}T00:00:00.000Z`);
  const end = new Date(`${range.to}T00:00:00.000Z`);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) return [];

  const keys: string[] = [];
  const seen = new Set<string>();
  for (let day = start; day <= end; day = addDays(day, 1)) {
    const key = trendBucketKey(dateKey(day), grain);
    if (!seen.has(key)) {
      seen.add(key);
      keys.push(key);
    }
  }
  return keys;
}

function formatDateLabel(value: string, pattern: string): string {
  const date = parseRangeDate(value);
  return date ? format(date, pattern) : value;
}

function trendLabelPattern(grain: TrendGrain): string {
  return grain === "month" ? "MMM yyyy" : "MMM d";
}

function trendTooltipPattern(grain: TrendGrain): string {
  return grain === "month" ? "MMMM yyyy" : "MMMM d, yyyy";
}

function trendData(
  rows: AiToolTrendRow[],
  range: DateRange,
  tools: AiTool[]
): AiAcceptedTrendPoint[] {
  const grain = trendGrain(range);
  const labelPattern = trendLabelPattern(grain);
  const tooltipPattern = trendTooltipPattern(grain);
  const byDate = new Map<string, AiAcceptedTrendPoint>();
  for (const key of enumerateBucketKeys(range, grain)) {
    const point: AiAcceptedTrendPoint = {
      date: key,
      label: formatDateLabel(key, labelPattern),
      tooltipLabel: formatDateLabel(key, tooltipPattern),
    };
    for (const tool of tools) point[tool.key] = 0;
    byDate.set(key, point);
  }
  for (const row of rows) {
    const key = trendBucketKey(row.metric_date, grain);
    let point = byDate.get(key);
    if (!point) {
      point = {
        date: key,
        label: formatDateLabel(key, labelPattern),
        tooltipLabel: formatDateLabel(key, tooltipPattern),
      };
      for (const tool of tools) point[tool.key] = 0;
      byDate.set(key, point);
    }
    point[row.tool] = num(point[row.tool] as number) + num(row.accepted_lines_added);
  }
  return [...byDate.values()].sort((a, b) =>
    String(a.date) < String(b.date) ? -1 : 1
  );
}

function chartConfig(
  tools: AiTool[],
  colors: Record<string, string>
): ChartConfig {
  return Object.fromEntries(
    tools.map((tool) => [
      tool.key,
      { label: tool.name, color: colors[tool.key] },
    ])
  );
}

const AI_COUNTER_ORDER = [
  "ai_person_counter_daily.ai_accepted_lines",
  "ai_person_counter_daily.ai_active_days",
  "ai_person_counter_daily.ai_assistant_messages",
  "ai_person_counter_daily.ai_cost_cents",
  "ai_person_counter_daily.ai_accepted_edit_actions",
  "ai_person_counter_daily.ai_tool_acceptance_rate",
  "ai_person_counter_daily.ai_removed_lines",
  "ai_person_counter_daily.ai_assistant_actions",
] as const;

const AI_COUNTER_ORDER_INDEX = new Map<string, number>(
  AI_COUNTER_ORDER.map((key, index) => [key, index]),
);

function counterStats(row: AiPeerCounterRow): PeerStats | null {
  const { p25, median, p75, range_min, range_max, n } = row;
  if (
    p25 == null ||
    median == null ||
    p75 == null ||
    range_min == null ||
    range_max == null ||
    n == null
  ) {
    return null;
  }
  if (
    !Number.isFinite(p25) ||
    !Number.isFinite(median) ||
    !Number.isFinite(p75) ||
    !Number.isFinite(range_min) ||
    !Number.isFinite(range_max) ||
    !Number.isFinite(n)
  ) {
    return null;
  }
  return {
    p25,
    p50: median,
    p75,
    min: range_min,
    max: range_max,
    n,
  };
}

function aiPeerStoryEntries(
  rows: AiPeerCounterRow[],
  byMetricKey: (metricKey: string) => CatalogMetric | undefined,
): PeerStoryInput[] {
  const entries = rows.flatMap((row) => {
    if (!AI_COUNTER_ORDER_INDEX.has(row.metric_key)) return [];
    const catalog = byMetricKey(row.metric_key);
    if (!catalog || row.value == null || !Number.isFinite(row.value)) return [];
    return [
      {
        key: row.metric_key,
        label: catalog.label,
        sublabel: catalog.sublabel,
        value: row.value,
        unit: catalog.unit,
        format: catalog.format,
        higherIsBetter: catalog.higher_is_better,
        stats: counterStats(row),
      },
    ];
  });
  return entries.sort(
    (a, b) =>
      (AI_COUNTER_ORDER_INDEX.get(a.key) ?? Number.MAX_SAFE_INTEGER) -
      (AI_COUNTER_ORDER_INDEX.get(b.key) ?? Number.MAX_SAFE_INTEGER),
  );
}

interface DailyAcceptedLinesChartProps {
  tools: AiTool[];
  colors: Record<string, string>;
  data: AiAcceptedTrendPoint[];
  grain: TrendGrain;
  isPending?: boolean;
  isError?: boolean;
  onRetry?: () => void;
}

function DailyAcceptedLinesChart({
  tools,
  colors,
  data,
  grain,
  isPending,
  isError,
  onRetry,
}: DailyAcceptedLinesChartProps) {
  const grainLabel =
    grain === "month" ? "Monthly" : grain === "week" ? "Weekly" : "Daily";
  const title = "AI-added lines over time";
  if (isPending) return <Skeleton className="h-56 w-full rounded-lg" />;
  if (isError) {
    return (
      <ComingSoon
        state="error"
        label={`${title} — unable to load`}
        onRetry={onRetry}
      />
    );
  }
  if (data.length === 0 || tools.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-semibold">{title}</CardTitle>
          <CardDescription>No trend data yet.</CardDescription>
        </CardHeader>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-semibold">{title}</CardTitle>
        <CardDescription className="text-xs">{grainLabel} by tool</CardDescription>
      </CardHeader>
      <CardContent>
        <ChartContainer config={chartConfig(tools, colors)} className="h-56 w-full">
          <BarChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
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
            <ChartLegend content={<ChartLegendContent />} />
            {tools.map((tool) => (
              <ChartBar
                key={tool.key}
                dataKey={tool.key}
                stackId="accepted-lines"
                fill={`var(--color-${tool.key})`}
                name={tool.name}
                radius={[2, 2, 0, 0]}
              />
            ))}
          </BarChart>
        </ChartContainer>
      </CardContent>
    </Card>
  );
}

interface AcceptedLinesShareChartProps {
  rows: { label: string; value: number; tool: string }[];
  colors: Record<string, string>;
  isPending?: boolean;
  isError?: boolean;
  onRetry?: () => void;
}

function AcceptedLinesShareChart({
  rows,
  colors,
  isPending,
  isError,
  onRetry,
}: AcceptedLinesShareChartProps) {
  const title = "AI-added lines";
  if (isPending) return <Skeleton className="h-36 w-full rounded-lg" />;
  if (isError) {
    return (
      <ComingSoon
        state="error"
        label={`${title} — unable to load`}
        onRetry={onRetry}
      />
    );
  }
  if (rows.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-semibold">{title}</CardTitle>
          <CardDescription>No composition data yet.</CardDescription>
        </CardHeader>
      </Card>
    );
  }

  const total = rows.reduce((sum, row) => sum + row.value, 0);

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-semibold">{title}</CardTitle>
        <CardDescription className="text-xs">Period total by tool</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="flex h-24 w-full overflow-hidden rounded-md bg-muted md:h-28">
          {rows.map((row) => {
            const pct = total > 0 ? (row.value / total) * 100 : 0;
            return (
              <div
                key={row.tool}
                className="min-w-0 border-r-2 border-background p-3 last:border-r-0"
                style={{
                  width: `${pct}%`,
                  backgroundColor: colors[row.tool],
                  color: "var(--swatch-fg)",
                }}
                title={`${row.label}: ${row.value.toLocaleString()} lines`}
              >
                <div className="truncate text-sm font-semibold">{row.label}</div>
                <div className="mt-1 text-xs leading-4 opacity-90">{Math.round(pct)}%</div>
                <div className="text-xs leading-4 opacity-90">
                  {row.value.toLocaleString()} lines
                </div>
              </div>
            );
          })}
        </div>
      </CardContent>
    </Card>
  );
}

interface AiPeerCountersSectionProps {
  rows: AiPeerCounterRow[];
  byMetricKey: (metricKey: string) => CatalogMetric | undefined;
  isPending?: boolean;
  isError?: boolean;
  onRetry?: () => void;
}

function AiPeerCountersSection({
  rows,
  byMetricKey,
  isPending,
  isError,
  onRetry,
}: AiPeerCountersSectionProps) {
  if (isPending) return <Skeleton className="h-72 w-full rounded-lg" />;
  if (isError) {
    return (
      <ComingSoon
        state="error"
        label="AI counters — unable to load"
        onRetry={onRetry}
      />
    );
  }

  return (
    <PeerStorySection
      entries={aiPeerStoryEntries(rows, byMetricKey)}
      cohortLabel="department"
    />
  );
}

export function AiPersonalPanel({ personId, range }: AiPersonalPanelProps) {
  const canQuery = Boolean(personId && range);
  const fallbackRange = range ?? { from: "", to: "" };
  const summaryQ = useIcAiToolSummary(personId ?? "", fallbackRange);
  const trendQ = useIcAiToolTrend(personId ?? "", fallbackRange);
  const countersQ = useIcAiPeerCounters(personId ?? "", fallbackRange);
  const catalogQ = useCatalog();

  if (!canQuery) return null;

  const summary = summaryQ.data ?? [];
  const trend = trendQ.data ?? [];
  const tools = toolsByAcceptedLines(summary);
  const colors = swatchPalette(tools.map((tool) => tool.key));
  const acceptedShareRows = summary
    .filter((row) => num(row.accepted_lines_added) > 0)
    .sort((a, b) => num(b.accepted_lines_added) - num(a.accepted_lines_added))
    .map((row) => ({
      label: row.tool_name,
      value: num(row.accepted_lines_added),
      tool: row.tool,
    }));

  return (
    <div className="flex flex-col gap-4">
      <AcceptedLinesShareChart
        rows={acceptedShareRows}
        colors={colors}
        isPending={summaryQ.isPending}
        isError={summaryQ.isError}
        onRetry={() => summaryQ.refetch()}
      />
      <DailyAcceptedLinesChart
        tools={tools}
        colors={colors}
        data={trendData(trend, fallbackRange, tools)}
        grain={trendGrain(fallbackRange)}
        isPending={trendQ.isPending}
        isError={trendQ.isError}
        onRetry={() => trendQ.refetch()}
      />
      <AiPeerCountersSection
        rows={countersQ.data ?? []}
        byMetricKey={catalogQ.byMetricKey}
        isPending={countersQ.isPending || catalogQ.isLoading}
        isError={countersQ.isError || catalogQ.isError}
        onRetry={() => {
          void countersQ.refetch();
          catalogQ.refetch();
        }}
      />
    </div>
  );
}

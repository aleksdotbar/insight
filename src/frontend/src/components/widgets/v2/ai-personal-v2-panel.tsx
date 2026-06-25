import { useState } from "react";
import { ArrowUp, ChevronDown, ChevronRight } from "lucide-react";
import { format } from "date-fns";
import type { DateRange } from "@/api/period-to-date-range";

import { ComingSoon } from "@/components/widgets/coming-soon";
import type { CatalogMetric, PeerAnalysisMode } from "@/api/catalog-client";
import { useCatalog } from "@/api/use-catalog";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Bar,
  BarChart,
  CartesianGrid,
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
  PEER_FILL,
  PEER_TEXT,
  peerStatusVsQuartiles,
  type PeerStats,
  type PeerStatusWithNeutral,
} from "@/lib/peers";
import { STATUS_SURFACE_CLASS } from "@/lib/status";
import { swatchPalette } from "@/lib/swatch-palette";
import { cn } from "@/lib/utils";
import {
  useIcAiPeerCounters,
  useIcAiToolSummary,
  useIcAiToolTrend,
  type AiPeerCounterRow,
  type AiToolSummaryRow,
  type AiToolTrendRow,
} from "@/queries/v2/ic-extras";

interface AiPersonalV2PanelProps {
  personId: string | null | undefined;
  range: DateRange | null | undefined;
}

const TOOL_LABELS: Record<string, string> = {
  claude_code: "Claude Code",
  cursor: "Cursor",
  codex: "Codex",
  copilot: "GitHub Copilot",
  windsurf: "Windsurf",
};

function toolLabel(tool: string): string {
  return TOOL_LABELS[tool] ?? tool;
}

function num(v: number | null | undefined): number {
  return typeof v === "number" && Number.isFinite(v) ? v : 0;
}

function toolsByAcceptedLines(rows: AiToolSummaryRow[]): string[] {
  return [...rows]
    .sort((a, b) => num(b.accepted_lines_added) - num(a.accepted_lines_added))
    .map((r) => r.tool);
}

type AiAcceptedTrendPoint = {
  date: string;
  label: string;
  tooltipLabel: string;
  [tool: string]: number | string;
};

type TrendGrain = "day" | "week";

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
  return daysInRange(range) > 90 ? "week" : "day";
}

function weekStartKey(value: string): string {
  const date = parseRangeDate(value);
  if (!date) return value;
  const day = date.getUTCDay();
  const daysSinceMonday = (day + 6) % 7;
  return dateKey(addDays(date, -daysSinceMonday));
}

function trendBucketKey(value: string, grain: TrendGrain): string {
  return grain === "week" ? weekStartKey(value) : value;
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

function trendData(
  rows: AiToolTrendRow[],
  range: DateRange,
  tools: string[]
): AiAcceptedTrendPoint[] {
  const grain = trendGrain(range);
  const byDate = new Map<string, AiAcceptedTrendPoint>();
  for (const key of enumerateBucketKeys(range, grain)) {
    const point: AiAcceptedTrendPoint = {
      date: key,
      label: formatDateLabel(key, "d MMM"),
      tooltipLabel: formatDateLabel(key, "MMM d, yyyy"),
    };
    for (const tool of tools) point[tool] = 0;
    byDate.set(key, point);
  }
  for (const row of rows) {
    const key = trendBucketKey(row.metric_date, grain);
    let point = byDate.get(key);
    if (!point) {
      point = {
        date: key,
        label: formatDateLabel(key, "d MMM"),
        tooltipLabel: formatDateLabel(key, "MMM d, yyyy"),
      };
      for (const tool of tools) point[tool] = 0;
      byDate.set(key, point);
    }
    point[row.tool] = num(point[row.tool] as number) + num(row.accepted_lines_added);
  }
  return [...byDate.values()].sort((a, b) =>
    String(a.date) < String(b.date) ? -1 : 1
  );
}

function chartConfig(
  tools: string[],
  colors: Record<string, string>
): ChartConfig {
  return Object.fromEntries(
    tools.map((tool) => [
      tool,
      { label: toolLabel(tool), color: colors[tool] },
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

type AiCounterEntry = {
  row: AiPeerCounterRow;
  catalog: CatalogMetric;
  peerAnalysisMode: PeerAnalysisMode;
  status: PeerStatusWithNeutral;
  stats: PeerStats | null;
  value: number;
  gap: number;
};

function peerAnalysisMode(metric: CatalogMetric): PeerAnalysisMode {
  return (
    metric.peer_analysis_mode ??
    (metric.higher_is_better ? "higher_is_better" : "lower_is_better")
  );
}

function isRankedPeerMode(mode: PeerAnalysisMode): boolean {
  return mode !== "reference_only";
}

function higherIsBetterForMode(mode: PeerAnalysisMode): boolean {
  return mode !== "lower_is_better";
}

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

function counterEntries(
  rows: AiPeerCounterRow[],
  byMetricKey: (metricKey: string) => CatalogMetric | undefined,
): AiCounterEntry[] {
  const entries = rows.flatMap((row) => {
    const catalog = byMetricKey(row.metric_key);
    if (!catalog || row.value == null || !Number.isFinite(row.value)) return [];
    const mode = peerAnalysisMode(catalog);
    const isRanked = isRankedPeerMode(mode);
    const higherIsBetter = higherIsBetterForMode(mode);
    const stats = counterStats(row);
    const status = stats && isRanked
      ? peerStatusVsQuartiles(row.value, stats, higherIsBetter)
      : "neutral";
    const rawGap =
      stats && Math.abs(stats.p50) > 1e-9
        ? (row.value - stats.p50) / Math.abs(stats.p50)
        : 0;
    const gap = higherIsBetter ? rawGap : -rawGap;
    return [{ row, catalog, peerAnalysisMode: mode, status, stats, value: row.value, gap }];
  });
  return entries.sort(
    (a, b) =>
      (AI_COUNTER_ORDER_INDEX.get(a.row.metric_key) ?? Number.MAX_SAFE_INTEGER) -
      (AI_COUNTER_ORDER_INDEX.get(b.row.metric_key) ?? Number.MAX_SAFE_INTEGER),
  );
}

function formatCounterValue(
  value: number,
  unit: string | undefined,
  format: string | undefined,
): string {
  if (unit === "%" || format === "percent") {
    return value.toLocaleString(undefined, {
      maximumFractionDigits: 1,
    });
  }
  const rounded =
    unit === "days" && format !== "integer"
      ? Math.round(value * 10) / 10
      : Math.round(value);
  return rounded.toLocaleString();
}

function formatGapPct(gap: number): string {
  const sign = gap >= 0 ? "+" : "−";
  return `${sign}${Math.round(Math.abs(gap) * 100)}%`;
}

function formatStat(
  value: number,
  unit: string | undefined,
  format: string | undefined,
): string {
  const formatted = formatCounterValue(value, unit, format);
  if (!unit) return formatted;
  if (unit === "%") return `${formatted}${unit}`;
  return unit === "¢" ? `${formatted} ${unit}` : `${formatted} ${unit}`;
}

interface DailyAcceptedLinesChartProps {
  tools: string[];
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
  const title =
    grain === "week"
      ? "Weekly AI-added lines by tool"
      : "Daily AI-added lines by tool";
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
        <CardDescription className="text-xs">
          {grain === "week"
            ? "Weekly stacked accepted lines"
            : "Daily stacked accepted lines"}
        </CardDescription>
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
                  labelKey="tooltipLabel"
                />
              }
            />
            <ChartLegend content={<ChartLegendContent />} />
            {tools.map((tool) => (
              <Bar
                key={tool}
                dataKey={tool}
                stackId="accepted-lines"
                fill={`var(--color-${tool})`}
                name={toolLabel(tool)}
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
  const title = "AI-added lines by tool";
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
        <CardDescription className="text-xs">Period total</CardDescription>
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

function AiPeerZoneBar({ entry }: { entry: AiCounterEntry }) {
  const stats = entry.stats!;
  const span = Math.max(1e-9, stats.max - stats.min);
  const pct = (v: number) =>
    ((Math.max(stats.min, Math.min(stats.max, v)) - stats.min) / span) * 100;
  const p25Left = pct(stats.p25);
  const p50Left = pct(stats.p50);
  const p75Left = pct(stats.p75);
  const valueLeft = pct(entry.value);
  const higherIsBetter = higherIsBetterForMode(entry.peerAnalysisMode);
  const bottomZone = higherIsBetter
    ? STATUS_SURFACE_CLASS.bad
    : STATUS_SURFACE_CLASS.good;
  const topZone = higherIsBetter
    ? STATUS_SURFACE_CLASS.good
    : STATUS_SURFACE_CLASS.bad;

  return (
    <div className="mt-4">
      <div className="relative h-3.5 w-full select-none">
        <div className="absolute inset-x-0 top-1/2 h-2 -translate-y-1/2 overflow-hidden rounded-sm">
          <div
            className={cn("absolute inset-y-0 left-0", bottomZone)}
            style={{ width: `${p25Left}%` }}
          />
          <div
            className="absolute inset-y-0 bg-muted"
            style={{ left: `${p25Left}%`, width: `${p75Left - p25Left}%` }}
          />
          <div
            className={cn("absolute inset-y-0", topZone)}
            style={{ left: `${p75Left}%`, right: 0 }}
          />
        </div>
        <div
          className="absolute inset-y-0 w-px bg-foreground/60"
          style={{ left: `${p50Left}%` }}
          aria-hidden
        />
        <div
          className={cn(
            "absolute inset-y-0 w-[3px] -translate-x-1/2 rounded-sm ring-2 ring-background",
            PEER_FILL[entry.status],
          )}
          style={{ left: `${valueLeft}%` }}
        />
      </div>
      <div className="relative h-5">
        <ArrowUp
          className={cn("absolute top-1 size-4 -translate-x-1/2", PEER_TEXT[entry.status])}
          style={{ left: `${valueLeft}%` }}
          strokeWidth={3}
        />
      </div>
      <div className="mt-1 grid grid-cols-2 gap-3 text-[10px] tabular-nums">
        <span className="text-left text-muted-foreground">
          {formatStat(stats.min, entry.catalog.unit, entry.catalog.format)}
        </span>
        <span className="text-right text-muted-foreground">
          {formatStat(stats.max, entry.catalog.unit, entry.catalog.format)}
        </span>
      </div>
    </div>
  );
}

function AiCounterHero({ entry }: { entry: AiCounterEntry }) {
  const kind = entry.status === "bottom" ? "bad" : "good";
  const color = kind === "bad" ? "bottom" : "top";
  return (
    <Card
      className={cn(
        "flex min-h-72 flex-col gap-0 p-0",
        color === "bad"
          ? "shadow-[inset_0_3px_0_0_var(--destructive)]"
          : "shadow-[inset_0_3px_0_0_var(--success)]",
      )}
    >
      <div className="flex flex-1 flex-col gap-3 p-5 sm:p-6">
        <div className="flex items-center gap-1.5">
          <span className={cn("size-1.5 rounded-full", PEER_FILL[color])} />
          <span
            className={cn(
              "text-[10px] font-semibold uppercase tracking-widest",
              PEER_TEXT[color],
            )}
          >
            {kind === "bad" ? "Top issue" : "Top win"}
          </span>
        </div>
        <div>
          <h3 className="text-xl font-semibold tracking-tight sm:text-2xl">
            {entry.catalog.label}
          </h3>
          <p className="mt-1 text-sm text-muted-foreground">{entry.catalog.sublabel}</p>
        </div>
        <div className="flex flex-wrap items-baseline gap-x-5 gap-y-1.5">
          <span className="flex items-baseline gap-1">
            <span
              className={cn(
                "text-4xl font-semibold tabular-nums tracking-tight sm:text-[2.75rem]",
                PEER_TEXT[color],
              )}
            >
              {formatCounterValue(entry.value, entry.catalog.unit, entry.catalog.format)}
            </span>
            {entry.catalog.unit ? (
              <span className="text-base text-muted-foreground">{entry.catalog.unit}</span>
            ) : null}
          </span>
          {entry.stats ? (
            <span className="text-sm tabular-nums text-muted-foreground">
              gap{" "}
              <span className={cn("font-medium", PEER_TEXT[color])}>
                {formatGapPct(entry.gap)}
              </span>{" "}
              from department median{" "}
              <span className="text-foreground">
                {formatStat(entry.stats.p50, entry.catalog.unit, entry.catalog.format)}
              </span>
            </span>
          ) : null}
        </div>
        {entry.stats && entry.stats.max > entry.stats.min ? (
          <AiPeerZoneBar entry={entry} />
        ) : null}
        <span className={cn("mt-auto text-xs font-medium", PEER_TEXT[color])}>
          {kind === "bad" ? "Bottom 25% in department" : "Top 25% in department"}
        </span>
      </div>
    </Card>
  );
}

function AiCounterChips({ entries }: { entries: AiCounterEntry[] }) {
  if (entries.length === 0) return null;
  return (
    <div className="flex flex-wrap gap-1.5">
      {entries.map((entry) => (
        <span
          key={entry.row.metric_key}
          className={cn(
            "inline-flex items-center gap-1 rounded-full border px-2.5 py-1 text-xs",
            PEER_TEXT[entry.status],
          )}
        >
          <span className={cn("size-1.5 rounded-full", PEER_FILL[entry.status])} />
          {entry.catalog.label}
          <span className="font-mono tabular-nums">
            {formatStat(entry.value, entry.catalog.unit, entry.catalog.format)}
          </span>
        </span>
      ))}
    </div>
  );
}

function AiCounterPreviewGrid({ entries }: { entries: AiCounterEntry[] }) {
  if (entries.length === 0) return null;
  return (
    <div className="grid h-full gap-3 sm:grid-cols-2">
      {entries.slice(0, 6).map((entry) => {
        const isOutlier =
          isRankedPeerMode(entry.peerAnalysisMode) &&
          (entry.status === "bottom" || entry.status === "top");
        return (
          <Card
            key={entry.row.metric_key}
            className={cn(
              "min-h-28 overflow-hidden p-0",
              isOutlier && "border-current/20",
              isOutlier && PEER_TEXT[entry.status],
            )}
          >
            <div className="flex h-full">
              {isOutlier ? (
                <div className={cn("w-1 shrink-0", PEER_FILL[entry.status])} aria-hidden />
              ) : null}
              <div className="flex min-w-0 flex-1 flex-col justify-between gap-3 p-4">
                <div className="min-w-0">
                  <div className="truncate text-sm font-semibold text-muted-foreground">
                    {entry.catalog.label}
                  </div>
                  <div className="mt-0.5 truncate text-xs text-muted-foreground">
                    {entry.catalog.sublabel}
                  </div>
                </div>
                <div>
                  <div className="text-2xl font-semibold tabular-nums">
                    {formatCounterValue(entry.value, entry.catalog.unit, entry.catalog.format)}
                    {entry.catalog.unit ? (
                      <span className="ml-1 text-xs font-normal text-muted-foreground">
                        {entry.catalog.unit}
                      </span>
                    ) : null}
                  </div>
                  <div className="mt-1 truncate text-[11px] text-muted-foreground">
                    {isOutlier
                      ? entry.status === "bottom"
                        ? "Bottom 25%"
                        : "Top 25%"
                      : entry.stats
                        ? `median ${formatStat(entry.stats.p50, entry.catalog.unit, entry.catalog.format)}`
                        : "no peer data"}
                  </div>
                </div>
              </div>
            </div>
          </Card>
        );
      })}
    </div>
  );
}

function AiCounterSupportingFold({ entries }: { entries: AiCounterEntry[] }) {
  const [open, setOpen] = useState(false);
  if (entries.length === 0) return null;
  return (
    <div className="rounded-md border">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="flex w-full items-start justify-between gap-3 px-3 py-2 text-left transition-colors hover:bg-accent"
        aria-expanded={open}
      >
        <div className="min-w-0 flex-1">
          <div className="text-sm font-medium">
            {open ? "Hide" : "Show"} {entries.length} supporting and on-par metric
            {entries.length === 1 ? "" : "s"}
          </div>
          <div className="text-[11px] text-muted-foreground">
            Additional AI activity and peer context
          </div>
        </div>
        {open ? (
          <ChevronDown className="mt-0.5 size-4 shrink-0 text-muted-foreground" />
        ) : (
          <ChevronRight className="mt-0.5 size-4 shrink-0 text-muted-foreground" />
        )}
      </button>
      {open ? (
        <div className="border-t p-3">
          <div className="grid gap-x-8 gap-y-2 text-sm md:grid-cols-[minmax(180px,280px)_auto_1fr]">
            {entries.map((entry) => (
              <div key={entry.row.metric_key} className="contents">
                <div className="font-medium">{entry.catalog.label}</div>
                <div className="font-mono font-semibold tabular-nums">
                  {formatCounterValue(entry.value, entry.catalog.unit, entry.catalog.format)}
                  {entry.catalog.unit ? (
                    <span className="ml-1 font-sans text-xs font-normal text-muted-foreground">
                      {entry.catalog.unit}
                    </span>
                  ) : null}
                </div>
                <div className="text-muted-foreground">
                  {isRankedPeerMode(entry.peerAnalysisMode) ? (
                    <span>on par</span>
                  ) : null}
                  {entry.stats && isRankedPeerMode(entry.peerAnalysisMode) ? (
                    <span>
                      {" "}
                      · department median: {formatStat(entry.stats.p50, entry.catalog.unit, entry.catalog.format)}
                    </span>
                  ) : entry.stats ? (
                    <span>
                      department median: {formatStat(entry.stats.p50, entry.catalog.unit, entry.catalog.format)}
                    </span>
                  ) : isRankedPeerMode(entry.peerAnalysisMode) ? (
                    <span> · no peer data</span>
                  ) : (
                    <span>no peer data</span>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      ) : null}
    </div>
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

  const entries = counterEntries(rows, byMetricKey);
  const outliers = entries
    .filter(
      (entry) =>
        isRankedPeerMode(entry.peerAnalysisMode) &&
        (entry.status === "bottom" || entry.status === "top"),
    )
    .sort((a, b) => {
      if (a.status !== b.status) return a.status === "bottom" ? -1 : 1;
      return Math.abs(b.gap) - Math.abs(a.gap);
    });
  const supporting = entries.filter(
    (entry) =>
      !isRankedPeerMode(entry.peerAnalysisMode) ||
      (isRankedPeerMode(entry.peerAnalysisMode) &&
        (entry.status === "in_pack" || entry.status === "neutral")),
  ).sort((a, b) =>
    isRankedPeerMode(a.peerAnalysisMode) === isRankedPeerMode(b.peerAnalysisMode)
      ? 0
      : isRankedPeerMode(a.peerAnalysisMode) ? -1 : 1,
  );
  const hero = outliers[0] ?? null;
  const previewEntries = hero ? [...outliers.slice(1), ...supporting].slice(0, 6) : [];
  const previewKeys = new Set(previewEntries.map((entry) => entry.row.metric_key));
  const chips = outliers.slice(1).filter((entry) => !previewKeys.has(entry.row.metric_key));
  const foldedSupporting = supporting.filter(
    (entry) =>
      !previewKeys.has(entry.row.metric_key),
  );

  if (entries.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-semibold">Counters</CardTitle>
          <CardDescription>No counter data yet.</CardDescription>
        </CardHeader>
      </Card>
    );
  }

  return (
    <div className="flex flex-col gap-4">
      {hero ? (
        <div className="grid grid-cols-1 items-stretch gap-4 lg:grid-cols-2">
          <AiCounterHero entry={hero} />
          {previewEntries.length > 0 ? (
            <AiCounterPreviewGrid entries={previewEntries} />
          ) : null}
        </div>
      ) : null}
      <AiCounterChips entries={chips} />
      <AiCounterSupportingFold entries={foldedSupporting} />
    </div>
  );
}

export function AiPersonalV2Panel({ personId, range }: AiPersonalV2PanelProps) {
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
  const colors = swatchPalette(tools);
  const acceptedShareRows = summary
    .filter((row) => num(row.accepted_lines_added) > 0)
    .sort((a, b) => num(b.accepted_lines_added) - num(a.accepted_lines_added))
    .map((row) => ({
      label: toolLabel(row.tool),
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

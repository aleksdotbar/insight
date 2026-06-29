import { useState } from "react";
import { ArrowUp, ChevronDown, ChevronRight } from "lucide-react";

import {
  Card,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { useSettings } from "@/hooks/use-settings";
import {
  PEER_FILL,
  PEER_TEXT,
  peerStatusVsQuartiles,
  type PeerCohortLabel,
  type PeerStats,
  type PeerStatusWithNeutral,
} from "@/lib/peers";
import { STATUS_SURFACE_CLASS } from "@/lib/status";
import { cn } from "@/lib/utils";

export type PeerStoryInput = {
  key: string;
  label: string;
  sublabel?: string;
  value: number;
  unit?: string;
  format?: string;
  higherIsBetter: boolean;
  stats: PeerStats | null;
};

type PeerStoryEntry = PeerStoryInput & {
  status: PeerStatusWithNeutral;
  gapPct: number | null;
  gapDelta: number;
  severity: number;
};

interface PeerStorySectionProps {
  entries: PeerStoryInput[];
  cohortLabel?: PeerCohortLabel;
  emptyLabel?: string;
  className?: string;
}

function storyEntries(entries: PeerStoryInput[]): PeerStoryEntry[] {
  return entries.map((entry) => {
    const stats = entry.stats;
    const status =
      stats && Number.isFinite(entry.value)
        ? peerStatusVsQuartiles(entry.value, stats, entry.higherIsBetter)
        : "neutral";
    const rawDelta = stats ? entry.value - stats.p50 : 0;
    const gapDelta = entry.higherIsBetter ? rawDelta : -rawDelta;
    const gapPct =
      stats && Math.abs(stats.p50) > 1e-9
        ? gapDelta / Math.abs(stats.p50)
        : null;
    return {
      ...entry,
      status,
      gapPct,
      gapDelta,
      severity: gapPct == null ? Math.abs(gapDelta) : Math.abs(gapPct),
    };
  });
}

function formatValue(
  value: number,
  unit: string | undefined,
  format: string | undefined,
): string {
  if (unit === "%" || format === "percent") {
    return Math.round(value).toLocaleString();
  }
  const rounded =
    unit === "days" && format !== "integer"
      ? Math.round(value * 10) / 10
      : Math.round(value);
  return rounded.toLocaleString();
}

function formatStat(
  value: number,
  unit: string | undefined,
  format: string | undefined,
): string {
  const formatted = formatValue(value, unit, format);
  if (unit === "%" || format === "percent") return `${formatted}%`;
  if (!unit) return formatted;
  return `${formatted} ${unit}`;
}

function formatDisplayValue(
  value: number,
  unit: string | undefined,
  format: string | undefined,
): string {
  if (unit === "%" || format === "percent") return formatStat(value, unit, format);
  return formatValue(value, unit, format);
}

function displayUnit(
  unit: string | undefined,
  format: string | undefined,
): string | undefined {
  if (unit === "%" || format === "percent") return undefined;
  return unit;
}

function formatGapPct(gap: number): string {
  const sign = gap >= 0 ? "+" : "-";
  return `${sign}${Math.round(Math.abs(gap) * 100)}%`;
}

function formatGap(entry: PeerStoryEntry): string {
  if (entry.gapPct != null) return formatGapPct(entry.gapPct);
  const sign = entry.gapDelta >= 0 ? "+" : "-";
  return `${sign}${formatStat(Math.abs(entry.gapDelta), entry.unit, entry.format)}`;
}

function outlierText(status: PeerStatusWithNeutral): string {
  return status === "bottom" ? "Bottom 25%" : "Top 25%";
}

function sortedOutliers(entries: PeerStoryEntry[]) {
  const bottom = entries
    .filter((entry) => entry.status === "bottom")
    .sort((a, b) => b.severity - a.severity);
  const top = entries
    .filter((entry) => entry.status === "top")
    .sort((a, b) => b.severity - a.severity);
  return { bottom, top };
}

function supportEntries(entries: PeerStoryEntry[]) {
  return entries
    .filter((entry) => entry.status === "in_pack" || entry.status === "neutral")
    .sort((a, b) => {
      if (a.status === b.status) return 0;
      return a.status === "neutral" ? 1 : -1;
    });
}

function PeerZoneBar({ entry }: { entry: PeerStoryEntry }) {
  const stats = entry.stats!;
  const span = Math.max(1e-9, stats.max - stats.min);
  const pct = (value: number) =>
    ((Math.max(stats.min, Math.min(stats.max, value)) - stats.min) / span) * 100;
  const p25Left = pct(stats.p25);
  const p50Left = pct(stats.p50);
  const p75Left = pct(stats.p75);
  const valueLeft = pct(entry.value);
  const bottomZone = entry.higherIsBetter
    ? STATUS_SURFACE_CLASS.bad
    : STATUS_SURFACE_CLASS.good;
  const topZone = entry.higherIsBetter
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
          {formatStat(stats.min, entry.unit, entry.format)}
        </span>
        <span className="text-right text-muted-foreground">
          {formatStat(stats.max, entry.unit, entry.format)}
        </span>
      </div>
    </div>
  );
}

function HeroCard({
  entry,
  cohortLabel,
}: {
  entry: PeerStoryEntry;
  cohortLabel: PeerCohortLabel;
}) {
  const isBad = entry.status === "bottom";
  const color = isBad ? "bottom" : "top";
  const unit = displayUnit(entry.unit, entry.format);
  return (
    <Card
      className={cn(
        "flex h-full min-h-72 flex-col gap-0 p-0",
        isBad
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
            {isBad ? "Top issue" : "Top win"}
          </span>
        </div>
        <div>
          <h3 className="text-xl font-semibold tracking-tight sm:text-2xl">
            {entry.label}
          </h3>
          {entry.sublabel ? (
            <p className="mt-1 text-sm text-muted-foreground">{entry.sublabel}</p>
          ) : null}
        </div>
        <div className="flex flex-wrap items-baseline gap-x-5 gap-y-1.5">
          <span className="flex items-baseline gap-1">
            <span
              className={cn(
                "text-4xl font-semibold tabular-nums tracking-tight sm:text-[2.75rem]",
                PEER_TEXT[color],
              )}
            >
              {formatDisplayValue(entry.value, entry.unit, entry.format)}
            </span>
            {unit ? (
              <span className="text-base text-muted-foreground">{unit}</span>
            ) : null}
          </span>
          {entry.stats ? (
            <span className="text-sm tabular-nums text-muted-foreground">
              gap{" "}
              <span className={cn("font-normal", PEER_TEXT[color])}>
                {formatGap(entry)}
              </span>{" "}
              from {cohortLabel} median{" "}
              <span className="text-foreground">
                {formatStat(entry.stats.p50, entry.unit, entry.format)}
              </span>
            </span>
          ) : null}
        </div>
        {entry.stats && entry.stats.max > entry.stats.min ? (
          <PeerZoneBar entry={entry} />
        ) : null}
        <span className={cn("mt-auto text-xs font-medium", PEER_TEXT[color])}>
          {isBad ? `Bottom 25% in ${cohortLabel}` : `Top 25% in ${cohortLabel}`}
        </span>
      </div>
    </Card>
  );
}

function SideCards({ entries }: { entries: PeerStoryEntry[] }) {
  if (entries.length === 0) return null;
  return (
    <div className="grid content-start gap-3">
      {entries.map((entry) => (
        <SideCard key={entry.key} entry={entry} />
      ))}
    </div>
  );
}

function SideCard({ entry }: { entry: PeerStoryEntry }) {
  const unit = displayUnit(entry.unit, entry.format);
  return (
    <Card
      className={cn(
        "min-h-28 p-0",
        "border-current/20",
        PEER_TEXT[entry.status],
        entry.status === "top" && "bg-success/5",
        entry.status === "bottom" && "bg-destructive/5",
        entry.status === "top" && "shadow-[inset_4px_0_0_0_var(--success)]",
        entry.status === "bottom" && "shadow-[inset_4px_0_0_0_var(--destructive)]",
      )}
    >
      <div className="flex h-full">
        <div className="flex min-w-0 flex-1 flex-col justify-between gap-3 p-4">
          <div className="min-w-0">
            <div className="truncate text-sm font-semibold text-muted-foreground">
              {entry.label}
            </div>
            {entry.sublabel ? (
              <div className="mt-0.5 truncate text-xs text-muted-foreground">
                {entry.sublabel}
              </div>
            ) : null}
          </div>
          <div>
            <div className="text-2xl font-semibold tabular-nums">
              {formatDisplayValue(entry.value, entry.unit, entry.format)}
              {unit ? (
                <span className="ml-1 text-xs font-normal text-muted-foreground">
                  {unit}
                </span>
              ) : null}
            </div>
            <div className="mt-1 truncate text-[11px] text-muted-foreground">
              {outlierText(entry.status)}
            </div>
          </div>
        </div>
      </div>
    </Card>
  );
}

function ChipTooltip({
  entry,
  cohortLabel,
}: {
  entry: PeerStoryEntry;
  cohortLabel: PeerCohortLabel;
}) {
  return (
    <span className="flex flex-col gap-1 leading-snug">
      <span className="font-medium">{entry.label}</span>
      {entry.sublabel ? (
        <span className="text-background/70">{entry.sublabel}</span>
      ) : null}
      <span>
        {formatStat(entry.value, entry.unit, entry.format)} ·{" "}
        {outlierText(entry.status)}
      </span>
      {entry.stats ? (
        <span className="text-background/70">
          gap {formatGap(entry)} · {cohortLabel} median{" "}
          {formatStat(entry.stats.p50, entry.unit, entry.format)}
        </span>
      ) : null}
    </span>
  );
}

function OutlierChips({
  entries,
  cohortLabel,
}: {
  entries: PeerStoryEntry[];
  cohortLabel: PeerCohortLabel;
}) {
  if (entries.length === 0) return null;
  return (
    <div className="flex flex-wrap gap-1.5">
      {entries.map((entry) => (
        <Tooltip key={entry.key}>
          <TooltipTrigger
            render={
              <button
                type="button"
                className={cn(
                  "inline-flex cursor-help items-center gap-1 rounded-full border bg-transparent px-2.5 py-1 text-xs focus-visible:ring-1 focus-visible:ring-ring focus-visible:outline-none",
                  PEER_TEXT[entry.status],
                )}
              >
                <span className={cn("size-1.5 rounded-full", PEER_FILL[entry.status])} />
                {entry.label}
                <span className="font-mono tabular-nums">
                  {formatStat(entry.value, entry.unit, entry.format)}
                </span>
              </button>
            }
          />
          <TooltipContent side="top" className="max-w-64">
            <ChipTooltip entry={entry} cohortLabel={cohortLabel} />
          </TooltipContent>
        </Tooltip>
      ))}
    </div>
  );
}

function FlatGridCard({ entry }: { entry: PeerStoryEntry }) {
  const unit = displayUnit(entry.unit, entry.format);
  return (
    <Card className="p-4">
      <div className="min-w-0">
        <div className="truncate text-sm font-semibold text-muted-foreground">
          {entry.label}
        </div>
        {entry.sublabel ? (
          <div className="mt-0.5 truncate text-xs text-muted-foreground">
            {entry.sublabel}
          </div>
        ) : null}
      </div>
      <div className="mt-5 text-2xl font-semibold tabular-nums">
        {formatDisplayValue(entry.value, entry.unit, entry.format)}
        {unit ? (
          <span className="ml-1 text-xs font-normal text-muted-foreground">
            {unit}
          </span>
        ) : null}
      </div>
    </Card>
  );
}

function FlatGrid({
  entries,
  className,
}: {
  entries: PeerStoryEntry[];
  className?: string;
}) {
  if (entries.length === 0) return null;
  return (
    <div
      className={cn(
        "grid gap-3 [grid-template-columns:repeat(auto-fit,minmax(260px,1fr))]",
        className,
      )}
    >
      {entries.map((entry) => (
        <FlatGridCard key={entry.key} entry={entry} />
      ))}
    </div>
  );
}

function EmptyState({ label }: { label: string }) {
  return <Card className="p-6 text-sm text-muted-foreground">{label}</Card>;
}

function SupportingFold({
  entries,
  cohortLabel,
}: {
  entries: PeerStoryEntry[];
  cohortLabel: PeerCohortLabel;
}) {
  const [open, setOpen] = useState(false);
  if (entries.length === 0) return null;
  return (
    <div className="rounded-md border">
      <button
        type="button"
        onClick={() => setOpen((value) => !value)}
        className="flex w-full items-start justify-between gap-3 px-3 py-2 text-left transition-colors hover:bg-accent"
        aria-expanded={open}
      >
        <div className="min-w-0 flex-1">
          <div className="text-sm font-medium">
            {open ? "Hide" : "Show"} {entries.length} on-par metric
            {entries.length === 1 ? "" : "s"}
          </div>
          <div className="text-[11px] text-muted-foreground">
            Metrics within the {cohortLabel}&apos;s normal range - no peer outlier
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
              <SupportingRow key={entry.key} entry={entry} cohortLabel={cohortLabel} />
            ))}
          </div>
        </div>
      ) : null}
    </div>
  );
}

function SupportingRow({
  entry,
  cohortLabel,
}: {
  entry: PeerStoryEntry;
  cohortLabel: PeerCohortLabel;
}) {
  const unit = displayUnit(entry.unit, entry.format);
  return (
    <div className="contents">
      <div className="font-medium">{entry.label}</div>
      <div className="font-mono font-semibold tabular-nums">
        {formatDisplayValue(entry.value, entry.unit, entry.format)}
        {unit ? (
          <span className="ml-1 font-sans text-xs font-normal text-muted-foreground">
            {unit}
          </span>
        ) : null}
      </div>
      <div className="text-muted-foreground">
        {entry.stats ? (
          <span>
            on par · {cohortLabel} median:{" "}
            {formatStat(entry.stats.p50, entry.unit, entry.format)}
          </span>
        ) : (
          <span>no peer data</span>
        )}
      </div>
    </div>
  );
}

export function PeerStorySection({
  entries,
  cohortLabel = "department",
  emptyLabel = "No counter data yet.",
  className,
}: PeerStorySectionProps) {
  const { focusMode } = useSettings();
  const allEntries = storyEntries(entries);
  const { bottom, top } = sortedOutliers(allEntries);
  const foldedEntries = supportEntries(allEntries);
  const focusedOutliers =
    focusMode === "critical"
      ? bottom
      : focusMode === "rewards"
        ? top
        : focusMode === "all"
          ? bottom.length > 0
            ? [...bottom, ...top]
            : top
          : [];
  const hero = focusedOutliers[0] ?? null;
  const sideCards = hero ? focusedOutliers.slice(1, 4) : [];
  const chips = hero ? focusedOutliers.slice(4) : [];

  if (allEntries.length === 0) {
    return (
      <Card className={className}>
        <CardHeader>
          <CardTitle className="text-sm font-semibold">Counters</CardTitle>
          <CardDescription>{emptyLabel}</CardDescription>
        </CardHeader>
      </Card>
    );
  }

  if (focusMode === "neutral") {
    return <FlatGrid entries={allEntries} className={className} />;
  }

  if (focusMode === "all" && !hero) {
    return <FlatGrid entries={allEntries} className={className} />;
  }

  return (
    <div className={cn("flex flex-col gap-4", className)}>
      {hero ? (
        <div className="grid grid-cols-1 items-stretch gap-4 lg:grid-cols-2">
          <HeroCard entry={hero} cohortLabel={cohortLabel} />
          <SideCards entries={sideCards} />
        </div>
      ) : focusMode === "critical" ? (
        <EmptyState label="No critical issues this period" />
      ) : focusMode === "rewards" ? (
        <EmptyState label="No standout wins this period" />
      ) : null}
      <OutlierChips entries={chips} cohortLabel={cohortLabel} />
      <SupportingFold entries={foldedEntries} cohortLabel={cohortLabel} />
    </div>
  );
}

import { useState } from "react";
import { ChevronDown, ChevronRight } from "lucide-react";

import { MetricSublabel } from "@/components/widgets/v2/metric-sublabel";
import { Card } from "@/components/ui/card";
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
import { cn } from "@/lib/utils";
import type { BulletMetric } from "@/types/insight";

import type { PeerCohortLabel } from "@/lib/peers";

interface StoryEntry {
  row: BulletMetric;
  status: PeerStatusWithNeutral;
  stats: PeerStats | null;
  higherIsBetter: boolean;
  gap: number;
}

function buildEntries(
  rows: BulletMetric[],
  cohortStats: Map<string, PeerStats> | undefined,
): StoryEntry[] {
  return rows.map((row) => {
    const stats = cohortStats?.get(row.metric_key) ?? null;
    const def = BULLET_DEFS_BY_KEY[row.metric_key];
    const higherIsBetter = def?.higher_is_better ?? true;
    const numericValue = Number(row.value);
    const hasValue = Number.isFinite(numericValue);
    const status: PeerStatusWithNeutral =
      stats && hasValue
        ? peerStatusVsQuartiles(numericValue, stats, higherIsBetter)
        : "neutral";
    let gap = 0;
    if (stats && hasValue && Math.abs(stats.p50) > 1e-9) {
      const raw = (numericValue - stats.p50) / Math.abs(stats.p50);
      gap = higherIsBetter ? raw : -raw;
    }
    return { row, status, stats, higherIsBetter, gap };
  });
}

function formatGapPct(gap: number): string {
  const sign = gap >= 0 ? "+" : "−";
  return `${sign}${Math.round(Math.abs(gap) * 100)}%`;
}

export interface CountersBlockProps {
  rows: BulletMetric[];
  cohortStats?: Map<string, PeerStats>;
  cohortLabel?: PeerCohortLabel;
}

export function CountersBlock({
  rows,
  cohortStats,
  cohortLabel = "team",
}: CountersBlockProps) {
  const { focusMode } = useSettings();
  const entries = buildEntries(rows, cohortStats);

  const bottoms = entries
    .filter((e) => e.status === "bottom")
    .sort((a, b) => a.gap - b.gap);
  const tops = entries
    .filter((e) => e.status === "top")
    .sort((a, b) => b.gap - a.gap);
  const inPack = entries.filter(
    (e) =>
      (e.status === "in_pack" || e.status === "neutral") &&
      e.row.value !== "—" &&
      e.row.value !== "",
  );

  const heroBad = bottoms[0] ?? null;
  const heroGood = !heroBad ? (tops[0] ?? null) : null;
  const hero = heroBad ?? heroGood;
  const heroKind: "bad" | "good" | null = heroBad
    ? "bad"
    : heroGood
      ? "good"
      : null;

  const remainingBottoms = heroBad ? bottoms.slice(1) : bottoms;
  const remainingTops = heroGood ? tops.slice(1) : tops;
  const useChips = remainingTops.length > 2;
  const outlierTops = useChips ? [] : remainingTops;
  const chipTops = useChips ? remainingTops : [];
  const outliers = [...remainingBottoms, ...outlierTops];

  if (!hero && outliers.length === 0 && chipTops.length === 0 && inPack.length === 0) {
    return null;
  }

  return (
    <div className="flex flex-col gap-4">
      {hero || outliers.length > 0 ? (
        <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
          {hero ? (
            <HeroTile
              entry={hero}
              kind={heroKind ?? "bad"}
              focusMode={focusMode}
              cohortLabel={cohortLabel}
              span={outliers.length === 0}
            />
          ) : null}
          {outliers.length > 0 ? (
            <div className="flex flex-col gap-3">
              {outliers.map((e) => (
                <OutlierTile
                  key={e.row.metric_key}
                  entry={e}
                  focusMode={focusMode}
                  cohortLabel={cohortLabel}
                />
              ))}
            </div>
          ) : null}
        </div>
      ) : null}

      {chipTops.length > 0 ? (
        <div className="flex flex-col gap-2">
          <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
            Strong points
          </p>
          <div className="flex flex-wrap gap-1.5">
            {chipTops.map((e) => (
              <span
                key={e.row.metric_key}
                className={cn(
                  "inline-flex items-center gap-1 rounded-full border px-2.5 py-1 text-xs",
                  PEER_TEXT[applyFocus("top", focusMode)],
                )}
              >
                <span
                  className={cn("size-1.5 rounded-full", PEER_FILL[applyFocus("top", focusMode)])}
                />
                {e.row.label}
                <span className="font-mono tabular-nums">
                  {e.row.value}
                  {e.row.unit ? ` ${e.row.unit}` : ""}
                </span>
              </span>
            ))}
          </div>
        </div>
      ) : null}

      {inPack.length > 0 ? (
        hero || outliers.length > 0 || chipTops.length > 0 ? (
          <InPackFold
            entries={inPack}
            focusMode={focusMode}
            cohortLabel={cohortLabel}
          />
        ) : (
          <div className="grid gap-3 [grid-template-columns:repeat(auto-fit,minmax(260px,1fr))]">
            {inPack.map((e) => (
              <OutlierTile
                key={e.row.metric_key}
                entry={e}
                focusMode={focusMode}
                cohortLabel={cohortLabel}
                dense
              />
            ))}
          </div>
        )
      ) : null}
    </div>
  );
}

function HeroTile({
  entry,
  kind,
  focusMode,
  cohortLabel,
  span,
}: {
  entry: StoryEntry;
  kind: "bad" | "good";
  focusMode: ReturnType<typeof useSettings>["focusMode"];
  cohortLabel: PeerCohortLabel;
  span: boolean;
}) {
  const focused = applyFocus(kind === "bad" ? "bottom" : "top", focusMode);
  const inactive = focused === "neutral";
  return (
    <Card className={cn("flex flex-col gap-3 p-5 sm:p-6", span ? "md:col-span-2" : "")}>
      <div className="flex items-center gap-1.5">
        <span className={cn("size-1.5 rounded-full", PEER_FILL[focused])} />
        <span
          className={cn(
            "text-[10px] font-semibold uppercase tracking-widest",
            PEER_TEXT[focused],
          )}
        >
          {kind === "bad" ? "Top issue" : "Top win"}
        </span>
      </div>
      <h3 className="truncate text-xl font-semibold tracking-tight sm:text-2xl">
        {entry.row.label}
      </h3>
      <MetricSublabel
        description={BULLET_DEFS_BY_KEY[entry.row.metric_key]?.description}
        className="text-xs text-muted-foreground"
      />
      <div className="flex flex-wrap items-baseline gap-x-5 gap-y-1.5">
        <span className="flex items-baseline gap-1">
          <span
            className={cn(
              "text-4xl font-semibold tabular-nums tracking-tight sm:text-[2.75rem]",
              inactive ? "text-foreground" : PEER_TEXT[focused],
            )}
          >
            {entry.row.value}
          </span>
          {entry.row.unit ? (
            <span className="text-base text-muted-foreground">
              {entry.row.unit}
            </span>
          ) : null}
        </span>
        {entry.stats ? (
          <span className="text-sm tabular-nums text-muted-foreground">
            gap{" "}
            <span className={cn("font-medium", PEER_TEXT[focused])}>
              {formatGapPct(entry.gap)}
            </span>{" "}
            from {cohortLabel} median
          </span>
        ) : null}
      </div>
    </Card>
  );
}

function OutlierTile({
  entry,
  focusMode,
  cohortLabel,
  dense,
}: {
  entry: StoryEntry;
  focusMode: ReturnType<typeof useSettings>["focusMode"];
  cohortLabel: PeerCohortLabel;
  dense?: boolean;
}) {
  const focused = applyFocus(entry.status, focusMode);
  return (
    <Card className="flex items-baseline justify-between gap-3 px-4 py-3">
      <div className="flex min-w-0 flex-col gap-0.5">
        <span className="text-sm font-medium leading-tight line-clamp-2">
          {entry.row.label}
        </span>
        <MetricSublabel
          description={BULLET_DEFS_BY_KEY[entry.row.metric_key]?.description}
        />
        {dense ? null : (
          <span className={cn("text-[11px]", PEER_TEXT[focused])}>
            {entry.status === "top"
              ? `top 25% in ${cohortLabel}`
              : entry.status === "bottom"
                ? `bottom 25% in ${cohortLabel}`
                : `middle 50% in ${cohortLabel}`}
            {entry.stats ? (
              <span className="ml-1 text-muted-foreground">
                · gap {formatGapPct(entry.gap)}
              </span>
            ) : null}
          </span>
        )}
      </div>
      <span className="flex items-baseline gap-1 tabular-nums">
        <span className={cn("text-lg font-semibold", PEER_TEXT[focused])}>
          {entry.row.value}
        </span>
        {entry.row.unit ? (
          <span className="text-xs text-muted-foreground">
            {entry.row.unit}
          </span>
        ) : null}
      </span>
    </Card>
  );
}

function InPackFold({
  entries,
  focusMode,
  cohortLabel,
}: {
  entries: StoryEntry[];
  focusMode: ReturnType<typeof useSettings>["focusMode"];
  cohortLabel: PeerCohortLabel;
}) {
  const [open, setOpen] = useState(false);
  return (
    <div className="flex flex-col gap-2">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="flex items-center gap-1.5 text-xs font-medium text-muted-foreground transition-colors hover:text-foreground"
        aria-expanded={open}
      >
        {open ? (
          <ChevronDown className="size-3.5" />
        ) : (
          <ChevronRight className="size-3.5" />
        )}
        <span>
          {open ? "Hide" : "Show"} {entries.length} on-par metric
          {entries.length === 1 ? "" : "s"}
        </span>
      </button>
      {open ? (
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
          {entries.map((e) => (
            <OutlierTile
              key={e.row.metric_key}
              entry={e}
              focusMode={focusMode}
              cohortLabel={cohortLabel}
              dense
            />
          ))}
        </div>
      ) : null}
    </div>
  );
}

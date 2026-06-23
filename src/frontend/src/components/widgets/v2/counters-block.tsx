import { Fragment, useState } from "react";
import { ArrowUp, ChevronDown, ChevronRight } from "lucide-react";

import { useCatalog } from "@/api/use-catalog";
import { MetricSublabel } from "@/components/widgets/v2/metric-sublabel";
import { Card } from "@/components/ui/card";
import { useSettings } from "@/hooks/use-settings";
import { BULLET_DESCRIPTION_BY_KEY } from "@/lib/insight/v2/bullet-defs";
import { bulletCatalogKey, type CatalogByKey } from "@/lib/insight/v2/peer-status";
import {
  applyFocus,
  PEER_FILL,
  PEER_TEXT,
  peerStatusVsQuartiles,
  type PeerStats,
  type PeerStatusWithNeutral,
} from "@/lib/peers";
import { STATUS_SURFACE_CLASS } from "@/lib/status";
import { cn } from "@/lib/utils";
import type { BulletMetric } from "@/types/insight";

import type { PeerCohortLabel } from "@/lib/peers";

interface StoryEntry {
  row: BulletMetric;
  status: PeerStatusWithNeutral;
  stats: PeerStats | null;
  higherIsBetter: boolean;
  gap: number;
  value: number;
}

function buildEntries(
  rows: BulletMetric[],
  byMetricKey: CatalogByKey,
): StoryEntry[] {
  return rows.map((row) => {
    const stats = row.peer ?? null;
    const catalogRow = byMetricKey(bulletCatalogKey(row));
    // Match the wave-1 contract: schema_status='error' rows and rows the
    // catalog has no entry for collapse to 'neutral' (no peer coloring).
    const isSchemaError = row.schema_error === true;
    const higherIsBetter = catalogRow?.higher_is_better ?? true;
    const numericValue = Number(row.value);
    const hasValue = Number.isFinite(numericValue);
    const status: PeerStatusWithNeutral =
      !isSchemaError && catalogRow && stats && hasValue
        ? peerStatusVsQuartiles(numericValue, stats, higherIsBetter)
        : "neutral";
    let gap = 0;
    if (
      !isSchemaError &&
      catalogRow &&
      stats &&
      hasValue &&
      Math.abs(stats.p50) > 1e-9
    ) {
      const raw = (numericValue - stats.p50) / Math.abs(stats.p50);
      gap = higherIsBetter ? raw : -raw;
    }
    return { row, status, stats, higherIsBetter, gap, value: numericValue };
  });
}

function formatGapPct(gap: number): string {
  const sign = gap >= 0 ? "+" : "−";
  return `${sign}${Math.round(Math.abs(gap) * 100)}%`;
}

function fmtStat(v: number, unit: string): string {
  const rounded = unit === "d" ? Math.round(v * 10) / 10 : Math.round(v);
  if (!unit) return String(rounded);
  return unit === "%" ? `${rounded}%` : `${rounded} ${unit}`;
}

function PeerZoneBar({
  value,
  stats,
  status,
  higherIsBetter,
}: {
  value: number;
  stats: PeerStats;
  status: PeerStatusWithNeutral;
  higherIsBetter: boolean;
}) {
  const span = Math.max(1e-9, stats.max - stats.min);
  const pct = (v: number) =>
    ((Math.max(stats.min, Math.min(stats.max, v)) - stats.min) / span) * 100;
  const p25Left = pct(stats.p25);
  const p50Left = pct(stats.p50);
  const p75Left = pct(stats.p75);
  const valueLeft = pct(value);
  // Zones describe the peer distribution (bad/mid/good), oriented by
  // higher_is_better; the pin marks where this person lands.
  const bottomZone = higherIsBetter
    ? STATUS_SURFACE_CLASS.bad
    : STATUS_SURFACE_CLASS.good;
  const topZone = higherIsBetter
    ? STATUS_SURFACE_CLASS.good
    : STATUS_SURFACE_CLASS.bad;

  return (
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
          PEER_FILL[status],
        )}
        style={{ left: `${valueLeft}%` }}
        aria-label="your value"
      />
    </div>
  );
}

function HeroRangeBar({
  entry,
  status,
}: {
  entry: StoryEntry;
  status: PeerStatusWithNeutral;
}) {
  const stats = entry.stats!;
  const unit = entry.row.unit ?? "";
  const span = stats.max - stats.min;
  const pct = (v: number) =>
    ((Math.max(stats.min, Math.min(stats.max, v)) - stats.min) / span) * 100;
  const valuePct = pct(entry.value);
  const p50Pct = pct(stats.p50);
  // Drop the inline median tick when it crowds an endpoint; the value still
  // shows on its own line below so it's never lost.
  const showMedianTick = p50Pct > 18 && p50Pct < 82;
  const medianLabel = fmtStat(stats.p50, unit);

  return (
    <div className="mt-2">
      <PeerZoneBar
        value={entry.value}
        stats={stats}
        status={status}
        higherIsBetter={entry.higherIsBetter}
      />
      <div className="relative h-5">
        <ArrowUp
          className={cn(
            "absolute top-1 size-4 -translate-x-1/2",
            PEER_TEXT[status],
          )}
          style={{ left: `${valuePct}%` }}
          strokeWidth={3}
          aria-label={`your value ${fmtStat(entry.value, unit)}`}
        />
      </div>
      <div className="relative mt-1 h-4 text-[10px] tabular-nums">
        <span className="absolute left-0 top-0 text-muted-foreground">
          {fmtStat(stats.min, unit)}
        </span>
        {showMedianTick ? (
          <span
            className="absolute top-0 -translate-x-1/2 text-foreground/70"
            style={{ left: `${p50Pct}%` }}
          >
            median {medianLabel}
          </span>
        ) : null}
        <span className="absolute right-0 top-0 text-muted-foreground">
          {fmtStat(stats.max, unit)}
        </span>
      </div>
      {!showMedianTick ? (
        <div className="mt-0.5 text-[10px] tabular-nums text-foreground/70">
          median {medianLabel}
        </div>
      ) : null}
    </div>
  );
}

export interface CountersBlockProps {
  rows: BulletMetric[];
  cohortLabel?: PeerCohortLabel;
  layout?: "story" | "list";
}

export function CountersBlock({
  rows,
  cohortLabel = "department",
  layout = "story",
}: CountersBlockProps) {
  const { focusMode } = useSettings();
  const { byMetricKey } = useCatalog();
  const entries = buildEntries(rows, byMetricKey);

  // List layout: a flat value-vs-cohort row per metric (no hero/outlier
  // partitioning). Used for team-aggregate distributions, which have no
  // per-person histogram to chart.
  if (layout === "list") {
    const visible = entries.filter(
      (e) => e.row.value !== "—" && e.row.value !== "",
    );
    if (visible.length === 0) return null;
    return (
      <InPackRows
        entries={visible}
        focusMode={focusMode}
        cohortLabel={cohortLabel}
        colorValue
      />
    );
  }

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
  const showBar =
    entry.stats != null &&
    Number.isFinite(entry.value) &&
    entry.stats.max > entry.stats.min;
  return (
    <Card
      className={cn(
        "flex flex-col gap-0 overflow-hidden p-0",
        span ? "md:col-span-2" : "",
      )}
    >
      <div className={cn("h-[3px] w-full", PEER_FILL[focused])} aria-hidden />
      <div className="flex flex-col gap-3 p-5 sm:p-6">
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
          description={BULLET_DESCRIPTION_BY_KEY.get(entry.row.metric_key)}
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
        {showBar ? <HeroRangeBar entry={entry} status={focused} /> : null}
        {entry.stats ? (
          <span className={cn("text-xs font-medium", PEER_TEXT[focused])}>
            {kind === "bad"
              ? `Bottom 25% in ${cohortLabel}`
              : `Top 25% in ${cohortLabel}`}
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
          description={BULLET_DESCRIPTION_BY_KEY.get(entry.row.metric_key)}
        />
        {dense ? null : (
          <span className={cn("text-[11px]", PEER_TEXT[focused])}>
            {entry.status === "top"
              ? `Top 25% in ${cohortLabel}`
              : entry.status === "bottom"
                ? `Bottom 25% in ${cohortLabel}`
                : `Middle 50% in ${cohortLabel}`}
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
    <div className="rounded-md border">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="flex w-full items-start gap-2 px-3 py-2 text-left transition-colors hover:bg-accent"
        aria-expanded={open}
      >
        {open ? (
          <ChevronDown className="mt-0.5 size-4 shrink-0 text-muted-foreground" />
        ) : (
          <ChevronRight className="mt-0.5 size-4 shrink-0 text-muted-foreground" />
        )}
        <div className="min-w-0 flex-1">
          <div className="text-sm font-medium">
            {open ? "Hide" : "Show"} {entries.length} on-par metric
            {entries.length === 1 ? "" : "s"}
          </div>
          <div className="text-[11px] text-muted-foreground">
            Metrics within the {cohortLabel}&apos;s normal range — no peer
            outlier
          </div>
        </div>
      </button>
      {open ? (
        <div className="border-t p-3">
          <InPackRows
            entries={entries}
            focusMode={focusMode}
            cohortLabel={cohortLabel}
          />
        </div>
      ) : null}
    </div>
  );
}

function InPackRows({
  entries,
  focusMode,
  cohortLabel,
  colorValue = false,
}: {
  entries: StoryEntry[];
  focusMode: ReturnType<typeof useSettings>["focusMode"];
  cohortLabel: PeerCohortLabel;
  colorValue?: boolean;
}) {
  const items = entries.map((e) => {
    const status = applyFocus(e.status, focusMode);
    const unit = e.row.unit ?? "";
    const positionText =
      e.status === "top"
        ? "top 25%"
        : e.status === "bottom"
          ? "bottom 25%"
          : e.status === "in_pack"
            ? "on par"
            : "—";
    const medianText =
      e.stats != null
        ? `${cohortLabel} median: ${fmtStat(e.stats.p50, unit)}`
        : "no peer data";
    return { e, status, positionText, medianText };
  });

  const ValueCell = ({
    e,
    status,
  }: {
    e: StoryEntry;
    status: PeerStatusWithNeutral;
  }) => (
    <span className="flex items-baseline gap-1 tabular-nums">
      <span
        className={cn(
          "font-mono text-lg font-semibold",
          colorValue && PEER_TEXT[status],
        )}
      >
        {e.row.value}
      </span>
      {e.row.unit ? (
        <span className="text-sm text-muted-foreground">{e.row.unit}</span>
      ) : null}
    </span>
  );
  const PeerCell = ({
    status,
    positionText,
    medianText,
  }: {
    status: PeerStatusWithNeutral;
    positionText: string;
    medianText: string;
  }) => (
    <span className="whitespace-nowrap text-xs text-muted-foreground">
      <span className={PEER_TEXT[status]}>{positionText}</span>
      <span className="mx-1.5 opacity-50">·</span>
      {medianText}
    </span>
  );
  const Heading = ({ e }: { e: StoryEntry }) => (
    <div className="min-w-0">
      <span className="block truncate text-sm font-medium" title={e.row.label}>
        {e.row.label}
      </span>
      <MetricSublabel
        description={BULLET_DESCRIPTION_BY_KEY.get(e.row.metric_key)}
      />
    </div>
  );

  return (
    <>
      {/* Mobile: stacked block per metric. */}
      <div className="flex flex-col gap-3 sm:hidden">
        {items.map(({ e, status, positionText, medianText }) => (
          <div key={e.row.metric_key}>
            <div className="flex items-baseline justify-between gap-3">
              <Heading e={e} />
              <ValueCell e={e} status={status} />
            </div>
            <div className="mt-0.5">
              <PeerCell
                status={status}
                positionText={positionText}
                medianText={medianText}
              />
            </div>
          </div>
        ))}
      </div>
      {/* Desktop: aligned 3-column grid. */}
      <div className="hidden items-baseline gap-x-4 gap-y-2 [grid-template-columns:max-content_max-content_1fr] sm:grid">
        {items.map(({ e, status, positionText, medianText }) => (
          <Fragment key={e.row.metric_key}>
            <Heading e={e} />
            <ValueCell e={e} status={status} />
            <PeerCell
              status={status}
              positionText={positionText}
              medianText={medianText}
            />
          </Fragment>
        ))}
      </div>
    </>
  );
}

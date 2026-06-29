import { Fragment } from "react";

import { useCatalog } from "@/api/use-catalog";
import { MetricSublabel } from "@/components/widgets/v2/metric-sublabel";
import {
  PeerStorySection,
  type PeerStoryInput,
} from "@/components/widgets/v2/peer-story-section";
import { useSettings } from "@/hooks/use-settings";
import { BULLET_DESCRIPTION_BY_KEY } from "@/lib/insight/v2/bullet-defs";
import { bulletCatalogKey, type CatalogByKey } from "@/lib/insight/v2/peer-status";
import {
  applyFocus,
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

function buildPeerStoryInputs(
  rows: BulletMetric[],
  byMetricKey: CatalogByKey,
): PeerStoryInput[] {
  return rows.flatMap((row) => {
    if (row.value === "—" || row.value === "") return [];
    const value = Number(row.value);
    if (!Number.isFinite(value)) return [];
    const catalogRow = byMetricKey(bulletCatalogKey(row));
    const stats = row.schema_error === true || !catalogRow ? null : row.peer ?? null;
    return [
      {
        key: row.metric_key,
        label: row.label,
        sublabel: catalogRow?.sublabel,
        value,
        unit: row.unit,
        format: catalogRow?.format,
        higherIsBetter: catalogRow?.higher_is_better ?? true,
        stats,
      },
    ];
  });
}

function fmtStat(v: number, unit: string): string {
  const rounded = unit === "d" ? Math.round(v * 10) / 10 : Math.round(v);
  if (!unit) return String(rounded);
  return unit === "%" ? `${rounded}%` : `${rounded} ${unit}`;
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

  return (
    <PeerStorySection
      entries={buildPeerStoryInputs(rows, byMetricKey)}
      cohortLabel={cohortLabel}
    />
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

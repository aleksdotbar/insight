import { useMemo, useState } from "react";

import { useCatalog } from "@/api/use-catalog";
import { ComingSoon } from "@/components/widgets/coming-soon";
import { DashboardEmptyState } from "@/components/widgets/v2/dashboard-empty-state";
import { DashboardHeader } from "@/components/widgets/v2/dashboard-header";
import { IcNeedsAttention } from "@/components/widgets/v2/ic-needs-attention";
import {
  KpiTile,
  KpiTileLoading,
  KpiTilePlaceholder,
} from "@/components/widgets/v2/kpi-tile";
import { MetricGroupCard } from "@/components/widgets/metric-views/metric-group-card";
import { SectionCard } from "@/components/widgets/v2/section-card";
import { GroupDrilldownSheet } from "@/components/widgets/v2/group-drilldown-sheet";
import { Spinner } from "@/components/ui/spinner";
import { usePeriod } from "@/hooks/use-period";
import { useSettings } from "@/hooks/use-settings";
import {
  legacyAttentionItems,
  metricAttentionItems,
} from "@/lib/insight/attention";
import {
  kpiRowTiles,
  legacyKpiTiles,
  metricKpiTiles,
  type KpiTileData,
} from "@/lib/insight/kpi-row";
import {
  GROUPS,
  KPI_ROW,
  KPI_ROW_COLLECTION,
  metricGroups,
  type GroupId,
} from "@/lib/insight/groups";
import { orderRowsForSection } from "@/lib/insight/v2/metric-order";
import { hasBulletValue } from "@/lib/insight/v2/peer-status";
import {
  entityObserved,
  projectViews,
  type MetricCollectionConfig,
} from "@/lib/metrics/collection";
import { normalizePersonId } from "@/lib/metrics/entity";
import { cn } from "@/lib/utils";
import { useIcDashboardData, type IcDashboardData } from "@/queries/ic-dashboard";
import {
  useMetricCollection,
  useMetricCollectionSet,
} from "@/queries/metric-results";
import type { BulletMetric, IdentityPerson } from "@/types/insight";

const IC_KPI_PREFIX = "ic_kpis.";

// Stable references so the disabled drilldown query keeps a constant key.
const EMPTY_COLLECTION: MetricCollectionConfig = { metrics: [] };
const CLOSED_ENTITY = { type: "person" as const, ids: [] };
// Placeholder for closed drilldown sheets; their body never renders.
const CLOSED_DRILLDOWN_DATA = {
  byKey: new Map(),
  previousByKey: null,
  isPending: true,
  isFetching: false,
  isError: false,
  refetch: () => {},
} as const;

// The one per-key seam that survives coexistence: which legacy batch field
// feeds each legacy group's rows. Dies with `LegacyGroup`.
const LEGACY_GROUP_ROWS: Record<
  string,
  (data: IcDashboardData | undefined) => BulletMetric[]
> = {
  wiki: (data) => data?.wiki ?? [],
};

export interface EngineeringDashboardV2Props {
  personId: string;
  person?: IdentityPerson | null;
}

export function EngineeringDashboardV2({
  personId,
  person,
}: EngineeringDashboardV2Props) {
  const { period, dateRange, setPeriod } = usePeriod();
  const { focusMode } = useSettings();
  const catalog = useCatalog();
  const dashQ = useIcDashboardData(personId, period, dateRange, {
    keepPrevious: true,
  });
  const entityId = normalizePersonId(personId);
  const entity = { type: "person" as const, ids: [entityId] };

  const kpiData = useMetricCollection(KPI_ROW_COLLECTION, entity, dateRange, {
    previousPeriod: period,
  });
  // Cards only render period + peer; the heavy timeseries/breakdown views
  // exist for the drilldown. Fetch the light projection here so a card paints
  // as fast as a KPI tile, and let the open drilldown fetch the full
  // collection lazily below.
  const groupData = useMetricCollectionSet(
    metricGroups().map((def) => ({
      key: def.id,
      collection: projectViews(def.collection, ["period", "peer"]),
    })),
    entity,
    dateRange,
  );

  const [openGroup, setOpenGroup] = useState<GroupId | null>(null);
  const data = dashQ.data;

  // Full collection for the open metrics group only (drives the drilldown's
  // chart blocks + peer story). Disabled while nothing is open — empty ids
  // gate the query off — so heavy views are never fetched for drilldowns the
  // user doesn't open.
  const openMetricDef =
    openGroup != null
      ? (metricGroups().find((def) => def.id === openGroup) ?? null)
      : null;
  const drilldownData = useMetricCollection(
    openMetricDef?.collection ?? EMPTY_COLLECTION,
    openMetricDef ? entity : CLOSED_ENTITY,
    dateRange,
  );

  const legacyRowsByGroup: Record<string, BulletMetric[]> =
    Object.fromEntries(
      GROUPS.filter((def) => def.kind === "legacy").map((def) => [
        def.id,
        orderRowsForSection(def.id, LEGACY_GROUP_ROWS[def.id]?.(data) ?? []),
      ]),
    );

  const displayName = person?.display_name ?? personId;
  const role = person?.job_title;

  // KPI row: legacy tiles from the shared batch, metric tiles from the
  // collection; placeholders per source while a tile has no data yet.
  const legacyTiles = legacyKpiTiles(
    data?.kpis ?? [],
    catalog.byMetricKey,
    focusMode,
  );
  const metricTiles = metricKpiTiles(
    kpiData.byKey,
    kpiData.previousByKey,
    entityId,
    focusMode,
  );
  const tiles = kpiRowTiles(legacyTiles, metricTiles);
  const tilesByKey = new Map<string, KpiTileData>(
    tiles.map((tile) => [tile.key, tile]),
  );
  const legacyKpiLabels = useMemo(
    () =>
      new Map(
        (catalog.data?.metrics ?? [])
          .filter((m) => m.metric_key?.startsWith(IC_KPI_PREFIX))
          .map((m) => [
            (m.metric_key ?? "").slice(IC_KPI_PREFIX.length),
            m.label,
          ]),
      ),
    [catalog.data],
  );

  const attentionItems = [
    ...legacyAttentionItems(
      GROUPS.filter((def) => def.kind === "legacy").map((def) => ({
        id: def.id,
        rows: legacyRowsByGroup[def.id] ?? [],
      })),
      catalog.byMetricKey,
    ),
    ...metricGroups().flatMap((def) =>
      metricAttentionItems(
        def,
        groupData.get(def.id)?.byKey ?? new Map(),
        entityId,
      ),
    ),
  ];

  const hasLegacyKpiData = (data?.kpis ?? []).some((k) => k.raw_value !== null);
  // Period views zero-fill sums, so "has data" means observed (peer
  // target_value), not merely a non-null zero-filled total — otherwise the
  // empty state becomes unreachable for fully unmeasured people.
  const hasMetricKpiData = [...kpiData.byKey.values()].some((metric) =>
    entityObserved(metric, entityId),
  );
  const hasLegacyGroupData = Object.values(legacyRowsByGroup).some((rows) =>
    rows.some(hasBulletValue),
  );
  const hasMetricGroupData = [...groupData.values()].some((result) =>
    [...result.byKey.values()].some((metric) =>
      entityObserved(metric, entityId),
    ),
  );
  const metricsSettled =
    !kpiData.isPending &&
    [...groupData.values()].every((result) => !result.isPending);
  // Failed unified queries must surface as error cards with retry — never as
  // "you have no data".
  const anyMetricError =
    kpiData.isError ||
    [...groupData.values()].some((result) => result.isError);
  const isAllEmpty =
    Boolean(data) &&
    metricsSettled &&
    !anyMetricError &&
    !hasLegacyKpiData &&
    !hasMetricKpiData &&
    !hasLegacyGroupData &&
    !hasMetricGroupData;
  // The page dim signals "the data you're already looking at is being
  // replaced" (period/range change, where keepPreviousData shows the old
  // values while new ones load). A collection's FIRST load must not dim the
  // page — it has no prior data to replace and shows its own card spinner —
  // so gate on revalidation (`isFetching && !isPending`), not bare fetching.
  const isMetricsRevalidating =
    (kpiData.isFetching && !kpiData.isPending) ||
    [...groupData.values()].some(
      (result) => result.isFetching && !result.isPending,
    );
  const showFullSpinner = dashQ.isPending || (isAllEmpty && dashQ.isFetching);

  // Close any open drilldown when the viewed person changes. Render-phase
  // reset against the previous id rather than an effect (no cascading commit).
  const [prevPersonId, setPrevPersonId] = useState(personId);
  if (personId !== prevPersonId) {
    setPrevPersonId(personId);
    setOpenGroup(null);
  }

  return (
    <div className="flex flex-col">
      <DashboardHeader
        title={displayName}
        subtitle={role}
        person={personId}
        hasReports={(person?.subordinates?.length ?? 0) > 0}
      />
      <main className="flex flex-1 flex-col gap-8 p-4 md:p-6">
        {showFullSpinner ? (
          <div className="flex min-h-[70vh] items-center justify-center">
            <Spinner className="size-12 text-muted-foreground" />
          </div>
        ) : dashQ.isError && !data ? (
          <div className="flex min-h-[60vh] items-center justify-center">
            <ComingSoon state="error" onRetry={() => dashQ.refetch()} />
          </div>
        ) : isAllEmpty ? (
          <div
            className={cn(
              "transition-opacity",
              dashQ.isFetching && "opacity-60",
            )}
          >
            <DashboardEmptyState period={period} onSetPeriod={setPeriod} />
          </div>
        ) : (
          <div
            className={cn(
              "flex flex-col gap-8 transition-opacity",
              (dashQ.isFetching || isMetricsRevalidating) && "opacity-60",
            )}
          >
            <section className="flex flex-col gap-3">
              <p className="flex items-center gap-1.5 text-xs font-medium tracking-wider text-muted-foreground uppercase">
                At a glance
              </p>
              <div className="grid gap-3 grid-cols-[repeat(auto-fit,minmax(13rem,1fr))]">
                {KPI_ROW.map((source) => {
                  const key =
                    source.kind === "legacy" ? source.key : source.metricKey;
                  const tile = tilesByKey.get(key);
                  if (tile && (source.kind === "metric" || !data?.errors.kpis)) {
                    return (
                      <KpiTile
                        key={key}
                        tile={tile}
                        onOpenGroup={setOpenGroup}
                      />
                    );
                  }
                  if (source.kind === "metric" && kpiData.isError) {
                    return (
                      <ComingSoon
                        key={key}
                        variant="card"
                        state="error"
                        onRetry={kpiData.refetch}
                      />
                    );
                  }
                  // Metric tiles reset to pending on a period change (no
                  // placeholder retention); show a spinner, not "Coming soon".
                  if (source.kind === "metric" && kpiData.isPending) {
                    return <KpiTileLoading key={key} />;
                  }
                  return (
                    <KpiTilePlaceholder
                      key={key}
                      label={
                        source.kind === "legacy"
                          ? legacyKpiLabels.get(source.key)
                          : undefined
                      }
                    />
                  );
                })}
              </div>
            </section>

            <IcNeedsAttention
              items={attentionItems}
              onOpenGroup={setOpenGroup}
            />

            <section className="flex flex-col gap-3">
              <p className="text-xs font-medium tracking-wider text-muted-foreground uppercase">
                Sections
              </p>
              <div className="grid gap-3 grid-cols-[repeat(auto-fit,minmax(18rem,1fr))]">
                {GROUPS.map((def) => {
                  if (def.kind === "metrics") {
                    const result = groupData.get(def.id);
                    if (!result) return null;
                    return (
                      <MetricGroupCard
                        key={def.id}
                        def={def}
                        data={result}
                        entityId={entityId}
                        onOpen={() => setOpenGroup(def.id)}
                      />
                    );
                  }
                  if (data?.errors[def.id]) {
                    return (
                      <SectionCard
                        key={def.id}
                        title={def.title}
                        sectionId={def.id}
                        rows={[]}
                        onOpen={() => {}}
                        unavailable
                      />
                    );
                  }
                  return (
                    <SectionCard
                      key={def.id}
                      title={def.title}
                      sectionId={def.id}
                      rows={legacyRowsByGroup[def.id] ?? []}
                      onOpen={() => setOpenGroup(def.id)}
                    />
                  );
                })}
              </div>
            </section>
          </div>
        )}
      </main>

      {GROUPS.map((def) => (
        <GroupDrilldownSheet
          key={def.id}
          open={openGroup === def.id}
          onOpenChange={(o) => setOpenGroup(o ? def.id : null)}
          def={def}
          rows={legacyRowsByGroup[def.id] ?? []}
          metricTarget={
            def.kind === "metrics"
              ? {
                  kind: "person",
                  entityId,
                  // The drilldown for the open group reads the full-collection
                  // query; closed sheets never render their body.
                  data:
                    def.id === openGroup ? drilldownData : CLOSED_DRILLDOWN_DATA,
                }
              : undefined
          }
          personId={personId}
          range={dateRange}
          period={period}
          cohortLabel="department"
        />
      ))}
    </div>
  );
}

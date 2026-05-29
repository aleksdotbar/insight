import { useEffect, useMemo, useState } from "react";

import { ComingSoon } from "@/components/widgets/coming-soon";
import { PeriodSelectorBar } from "@/components/widgets/period-selector-bar";
import { DashboardEmptyState } from "@/components/widgets/v2/dashboard-empty-state";
import { IcNeedsAttention } from "@/components/widgets/v2/ic-needs-attention";
import { KpiTile } from "@/components/widgets/v2/kpi-tile";
import { SectionCard } from "@/components/widgets/v2/section-card";
import { SectionDrilldownSheet } from "@/components/widgets/v2/section-drilldown-sheet";
import { SectionStatus } from "@/components/widgets/v2/section-status";
import { SidebarTrigger } from "@/components/ui/sidebar";
import { Spinner } from "@/components/ui/spinner";
import { usePeriod } from "@/hooks/use-period";
import {
  icDrilldownBatchQueryOptions,
  useIcCohortStats,
  useIcKpiPeerMedians,
} from "@/queries/v2/ic-extras";
import { queryClient } from "@/query-client";
import type { PeerStats } from "@/lib/peers";
import {
  IC_HERO_SECTIONS,
  IC_SECTIONS,
  type IcSectionId,
} from "@/lib/insight/v2/sections";
import { IC_KPI_DEFS, IC_KPI_DEFS_BY_KEY } from "@/lib/insight/v2/kpi-defs";
import { orderRowsForSection } from "@/lib/insight/v2/metric-order";
import { hasBulletValue } from "@/lib/insight/v2/peer-status";
import { cn } from "@/lib/utils";
import { useIcDashboardData } from "@/queries/ic-dashboard";
import type { BulletMetric, IdentityPerson } from "@/types/insight";

export interface EngineeringDashboardV2Props {
  personId: string;
  person?: IdentityPerson | null;
}

export function EngineeringDashboardV2({
  personId,
  person,
}: EngineeringDashboardV2Props) {
  const { period, customRange, dateRange, setPeriod, setCustomRange } =
    usePeriod();
  const dashQ = useIcDashboardData(personId, period, dateRange, {
    keepPrevious: true,
  });
  const [openSection, setOpenSection] = useState<IcSectionId | null>(null);
  const data = dashQ.data;

  const rowsBySection: Record<IcSectionId, BulletMetric[]> = {
    task_delivery: orderRowsForSection("task_delivery", data?.taskDelivery ?? []),
    git_output: orderRowsForSection("git_output", data?.gitOutput ?? []),
    code_quality: orderRowsForSection("code_quality", data?.codeQuality ?? []),
    collaboration: orderRowsForSection("collaboration", data?.collaboration ?? []),
    ai_adoption: orderRowsForSection("ai_adoption", data?.aiAdoption ?? []),
  };

  const heroSections = IC_HERO_SECTIONS.map((s) => ({
    id: s.id,
    label: s.label,
    rows: rowsBySection[s.id],
  }));

  const displayName = person?.display_name ?? personId;
  const role = person?.job_title;

  const cohortStatsQ = useIcCohortStats(
    "ic",
    person?.supervisor_email ?? "",
    dateRange,
  );
  const kpiMediansQ = useIcKpiPeerMedians(
    person?.supervisor_email ?? "",
    dateRange,
  );
  const kpiMediansByKey = useMemo(() => {
    const m = new Map<string, { p50: number; n: number }>();
    for (const row of kpiMediansQ.data ?? []) {
      m.set(row.kpi_key, { p50: row.p50, n: row.n });
    }
    return m;
  }, [kpiMediansQ.data]);

  const hasKpiData = (data?.kpis ?? []).some((k) => k.raw_value !== null);
  const hasSectionData = Object.values(rowsBySection).some((rows) =>
    rows.some(hasBulletValue),
  );
  const isAllEmpty = Boolean(data) && !hasKpiData && !hasSectionData;
  const showFullSpinner =
    dashQ.isPending || (isAllEmpty && dashQ.isFetching);
  const cohortStatsByKey = useMemo<Map<string, PeerStats>>(() => {
    const m = new Map<string, PeerStats>();
    for (const row of cohortStatsQ.data ?? []) {
      m.set(row.metric_key, {
        p25: row.p25,
        p50: row.p50,
        p75: row.p75,
        min: row.min,
        max: row.max,
        n: row.n,
      });
    }
    return m;
  }, [cohortStatsQ.data]);

  useEffect(() => {
    setOpenSection(null);
  }, [personId]);

  const openSectionForMetric = (metricKey: string) => {
    const kpiSection = IC_KPI_DEFS_BY_KEY[metricKey]?.section;
    if (kpiSection) {
      setOpenSection(kpiSection);
      return;
    }
    const owner = IC_SECTIONS.find((s) =>
      rowsBySection[s.id].some((r) => r.metric_key === metricKey),
    );
    if (owner) setOpenSection(owner.id);
  };

  return (
    <div className="flex flex-col">
      <header className="sticky top-0 z-20 flex flex-wrap items-center justify-between gap-3 border-b bg-background/95 px-4 py-3 backdrop-blur-sm">
        <div className="flex items-center gap-2">
          <SidebarTrigger />
          <div className="flex flex-col">
            <h1 className="text-xl font-semibold tracking-tight">
              {displayName}
            </h1>
            {role ? (
              <p className="text-xs text-muted-foreground">{role}</p>
            ) : null}
          </div>
        </div>
        <PeriodSelectorBar
          period={period}
          customRange={customRange}
          onPeriodChange={setPeriod}
          onRangeChange={setCustomRange}
        />
      </header>
      <main className="flex flex-1 flex-col gap-8 p-4 md:p-6">
        {showFullSpinner ? (
          <div className="flex min-h-[70vh] items-center justify-center">
            <Spinner className="size-12 text-muted-foreground" />
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
              dashQ.isFetching && "opacity-60",
            )}
          >
            <section className="flex flex-col gap-3">
              <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
                At a glance
              </p>
              <div className="grid grid-cols-2 gap-3 md:grid-cols-4 lg:grid-cols-7">
                {dashQ.isError || data?.errors.kpis
                  ? Array.from({ length: IC_KPI_DEFS.length }, (_, i) => (
                      <ComingSoon
                        key={i}
                        state="error"
                        onRetry={() => dashQ.refetch()}
                      />
                    ))
                  : (data?.kpis ?? []).map((kpi) => (
                      <KpiTile
                        key={kpi.metric_key}
                        kpi={kpi}
                        median={kpiMediansByKey.get(kpi.metric_key) ?? null}
                        onClick={openSectionForMetric}
                      />
                    ))}
              </div>
            </section>

            {dashQ.isError ? (
              <ComingSoon state="error" onRetry={() => dashQ.refetch()} />
            ) : data ? (
              <>
                <IcNeedsAttention
                  sections={heroSections}
                  cohortStats={cohortStatsByKey}
                  onSectionClick={setOpenSection}
                />
                <SectionStatus
                  sections={heroSections}
                  peerLabel="peers"
                  cols="five"
                  cohortStats={cohortStatsByKey}
                  onSectionClick={setOpenSection}
                />
              </>
            ) : null}

            <section className="flex flex-col gap-3">
              <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
                Sections
              </p>
              <div className="grid grid-cols-1 gap-3 md:grid-cols-2 lg:grid-cols-3">
                {IC_SECTIONS.map((s) => {
                  if (dashQ.isError || data?.errors[s.id]) {
                    return (
                      <ComingSoon
                        key={s.id}
                        state="error"
                        label={`${s.label} — unable to load`}
                        onRetry={() => dashQ.refetch()}
                      />
                    );
                  }
                  return (
                    <SectionCard
                      key={s.id}
                      title={s.label}
                      sectionId={s.id}
                      rows={rowsBySection[s.id]}
                      cohortStats={cohortStatsByKey}
                      onOpen={() => setOpenSection(s.id)}
                      onHover={() => {
                        void queryClient.prefetchQuery(
                          icDrilldownBatchQueryOptions({
                            sectionId: s.id,
                            personId,
                            range: dateRange,
                            period,
                          }),
                        );
                      }}
                    />
                  );
                })}
              </div>
            </section>
          </div>
        )}
      </main>

      <SectionDrilldownSheet
        open={openSection !== null}
        onOpenChange={(open) => {
          if (!open) setOpenSection(null);
        }}
        title={
          openSection
            ? (IC_SECTIONS.find((s) => s.id === openSection)?.label ?? "")
            : ""
        }
        rows={openSection ? rowsBySection[openSection] : []}
        sectionId={openSection}
        personId={personId}
        range={dateRange}
        period={period}
        cohortStats={cohortStatsByKey}
        cohortLabel="org"
      />
    </div>
  );
}

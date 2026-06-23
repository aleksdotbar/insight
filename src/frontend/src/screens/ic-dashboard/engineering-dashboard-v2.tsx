import { useMemo, useState } from "react";

import { useCatalog } from "@/api/use-catalog";
import { ComingSoon } from "@/components/widgets/coming-soon";
import { DashboardEmptyState } from "@/components/widgets/v2/dashboard-empty-state";
import { DashboardHeader } from "@/components/widgets/v2/dashboard-header";
import { IcNeedsAttention } from "@/components/widgets/v2/ic-needs-attention";
import { KpiTile, KpiTilePlaceholder } from "@/components/widgets/v2/kpi-tile";
import { SectionCard } from "@/components/widgets/v2/section-card";
import { SectionDrilldownSheet } from "@/components/widgets/v2/section-drilldown-sheet";
import { Spinner } from "@/components/ui/spinner";
import { usePeriod } from "@/hooks/use-period";
import {
  IC_HERO_SECTIONS,
  IC_SECTIONS,
  type IcSectionId,
} from "@/lib/insight/v2/sections";
import { IC_KPI_SECTION_BY_KEY, orderIcKpis } from "@/lib/insight/v2/kpi-defs";
import { orderRowsForSection } from "@/lib/insight/v2/metric-order";
import { hasBulletValue } from "@/lib/insight/v2/peer-status";
import { cn } from "@/lib/utils";
import { useIcDashboardData } from "@/queries/ic-dashboard";
import type { BulletMetric, IdentityPerson } from "@/types/insight";

const IC_KPI_PREFIX = "ic_kpis.";

export interface EngineeringDashboardV2Props {
  personId: string;
  person?: IdentityPerson | null;
}

export function EngineeringDashboardV2({
  personId,
  person,
}: EngineeringDashboardV2Props) {
  const { period, dateRange, setPeriod } = usePeriod();
  const catalog = useCatalog();
  const dashQ = useIcDashboardData(personId, period, dateRange, {
    keepPrevious: true,
  });
  const [openSection, setOpenSection] = useState<IcSectionId | null>(null);
  const data = dashQ.data;

  // KPI placeholder list driven by the wire catalog. Ordering is
  // wire-response order; the backend's seed migration emits the rows
  // deterministically. When the catalog is unavailable the placeholder
  // list is empty and consumers render the empty/error state.
  const kpiPlaceholders = useMemo(
    () =>
      orderIcKpis(
        (catalog.data?.metrics ?? [])
          .filter((m) => m.metric_key?.startsWith(IC_KPI_PREFIX))
          .map((m) => ({
            metric_key: (m.metric_key ?? "").slice(IC_KPI_PREFIX.length),
            label: m.label,
          }))
      ),
    [catalog.data]
  );

  const rowsBySection: Record<IcSectionId, BulletMetric[]> = {
    task_delivery: orderRowsForSection(
      "task_delivery",
      data?.taskDelivery ?? []
    ),
    git_output: orderRowsForSection("git_output", data?.gitOutput ?? []),
    code_quality: orderRowsForSection("code_quality", data?.codeQuality ?? []),
    collaboration: orderRowsForSection(
      "collaboration",
      data?.collaboration ?? []
    ),
    ai_adoption: orderRowsForSection("ai_adoption", data?.aiAdoption ?? []),
  };

  const heroSections = IC_HERO_SECTIONS.map((s) => ({
    id: s.id,
    label: s.label,
    rows: rowsBySection[s.id],
  }));

  const displayName = person?.display_name ?? personId;
  const role = person?.job_title;

  const kpis = orderIcKpis(data?.kpis ?? []);
  const peerCount = Math.max(0, ...kpis.map((k) => k.peer_n ?? 0));
  const hasKpiData = kpis.some((k) => k.raw_value !== null);
  const kpiTileCount = data?.errors.kpis ? kpiPlaceholders.length : kpis.length;
  const hasSectionData = Object.values(rowsBySection).some((rows) =>
    rows.some(hasBulletValue)
  );
  const isAllEmpty = Boolean(data) && !hasKpiData && !hasSectionData;
  const showFullSpinner = dashQ.isPending || (isAllEmpty && dashQ.isFetching);
  // Close any open drilldown when the viewed person changes. Render-phase
  // reset against the previous id rather than an effect (no cascading commit).
  const [prevPersonId, setPrevPersonId] = useState(personId);
  if (personId !== prevPersonId) {
    setPrevPersonId(personId);
    setOpenSection(null);
  }

  const openSectionForMetric = (metricKey: string) => {
    const kpiSection = IC_KPI_SECTION_BY_KEY.get(metricKey);
    if (kpiSection) {
      setOpenSection(kpiSection);
      return;
    }
    const owner = IC_SECTIONS.find((s) =>
      rowsBySection[s.id].some((r) => r.metric_key === metricKey)
    );
    if (owner) setOpenSection(owner.id);
  };

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
              dashQ.isFetching && "opacity-60"
            )}
          >
            <DashboardEmptyState period={period} onSetPeriod={setPeriod} />
          </div>
        ) : (
          <div
            className={cn(
              "flex flex-col gap-8 transition-opacity",
              dashQ.isFetching && "opacity-60"
            )}
          >
            {kpiTileCount > 0 && (
              <section className="flex flex-col gap-3">
                <p className="flex items-center gap-1.5 text-xs font-medium tracking-wider text-muted-foreground uppercase">
                  At a glance
                  {peerCount > 0 ? (
                    <span className="font-normal normal-case">
                      · {peerCount} peers
                    </span>
                  ) : null}
                </p>
                <div className="grid gap-3 grid-cols-[repeat(auto-fit,minmax(13rem,1fr))]">
                  {data?.errors.kpis
                    ? kpiPlaceholders.map((d) => (
                        <KpiTilePlaceholder
                          key={d.metric_key}
                          label={d.label}
                        />
                      ))
                    : kpis.map((kpi) => (
                        <KpiTile
                          key={kpi.metric_key}
                          kpi={kpi}
                          onClick={openSectionForMetric}
                        />
                      ))}
                </div>
              </section>
            )}

            <IcNeedsAttention
              sections={heroSections}
              onSectionClick={setOpenSection}
            />

            <section className="flex flex-col gap-3">
              <p className="text-xs font-medium tracking-wider text-muted-foreground uppercase">
                Sections
              </p>
              <div className="grid gap-3 grid-cols-[repeat(auto-fit,minmax(18rem,1fr))]">
                {IC_SECTIONS.map((s) => {
                  if (data?.errors[s.id]) {
                    return (
                      <SectionCard
                        key={s.id}
                        title={s.label}
                        sectionId={s.id}
                        rows={[]}
                        onOpen={() => {}}
                        unavailable
                      />
                    );
                  }
                  return (
                    <SectionCard
                      key={s.id}
                      title={s.label}
                      sectionId={s.id}
                      rows={rowsBySection[s.id]}
                      onOpen={() => setOpenSection(s.id)}
                    />
                  );
                })}
              </div>
            </section>
          </div>
        )}
      </main>

      {IC_SECTIONS.map((s) => (
        <SectionDrilldownSheet
          key={s.id}
          open={openSection === s.id}
          onOpenChange={(o) => setOpenSection(o ? s.id : null)}
          title={s.label}
          rows={rowsBySection[s.id]}
          sectionId={s.id}
          personId={personId}
          range={dateRange}
          period={period}
          cohortLabel="department"
        />
      ))}
    </div>
  );
}

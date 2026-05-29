import { useState } from "react";
import { useTranslation } from "react-i18next";

import { filterBulletsByLayoutGroup } from "@/api/threshold-config";
import { IcViewToggle } from "@/components/ic-view-toggle";
import { SidebarTrigger } from "@/components/ui/sidebar";
import { CollapsibleSection } from "@/components/widgets/collapsible-section";
import { ComingSoon } from "@/components/widgets/coming-soon";
import { DeliveryTrends } from "@/components/widgets/delivery-trends";
import { DrillModal } from "@/components/widgets/drill-modal";
import { KpiStrip } from "@/components/widgets/kpi-strip";
import { LocStackedBar } from "@/components/widgets/loc-stacked-bar";
import { MetricCard } from "@/components/widgets/metric-card";
import { PeriodSelectorBar } from "@/components/widgets/period-selector-bar";
import { PersonHeader } from "@/components/widgets/person-header";
import { TimeOffBanner } from "@/components/widgets/time-off-banner";
import { ViewModeToggle } from "@/components/widgets/view-mode-toggle";
import { usePeriod, useViewMode } from "@/hooks/use-period";
import {
  useIcDashboardData,
  useIcDrill,
  type IcDashboardSection,
} from "@/queries/ic-dashboard";
import type { IdentityPerson } from "@/types/insight";

export interface EngineeringDashboardProps {
  personId: string;
  person?: IdentityPerson | null;
}

export function EngineeringDashboard({
  personId,
  person: personProp,
}: EngineeringDashboardProps) {
  const { t } = useTranslation();
  const { period, customRange, dateRange, setPeriod, setCustomRange } =
    usePeriod();
  const { viewMode, setViewMode } = useViewMode();
  const [drillId, setDrillId] = useState<string | null>(null);

  const dashQ = useIcDashboardData(personId, period, dateRange);
  const drillQ = useIcDrill(personId, drillId, dateRange);

  const sectionState = (section: IcDashboardSection) => ({
    loading: dashQ.isPending,
    errored: dashQ.isError || (dashQ.data?.errors[section] ?? false),
    revalidating: false,
  });

  // Dispatcher (`<IcDashboardScreen>`) owns the identity fetch and forwards
  // it as a prop — children trust it and don't re-issue the query.
  const person = personProp ?? null;

  const handleDrillClick = (id: string): void => {
    setDrillId(id);
  };

  return (
    <div className="flex flex-col gap-4 p-6">
      <div className="bg-background/95 border-border/60 sticky top-0 z-20 -mx-6 -mt-6 border-b px-6 pt-6 pb-3 backdrop-blur-sm">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="flex min-w-0 items-center gap-3">
            <SidebarTrigger className="md:hidden" />
            <PersonHeader person={person} fallbackEmail={personId} inline />
            <IcViewToggle
              person={personId}
              hasReports={(person?.subordinates?.length ?? 0) > 0}
            />
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <PeriodSelectorBar
              period={period}
              customRange={customRange}
              onPeriodChange={setPeriod}
              onRangeChange={setCustomRange}
            />
            <ViewModeToggle mode={viewMode} onChange={setViewMode} />
          </div>
        </div>
      </div>

      <div className="border-border bg-card overflow-hidden rounded-xl border">
        {dashQ.isPending ? (
          <div className="p-4">
            <ComingSoon variant="row" state="loading" />
          </div>
        ) : dashQ.isError || dashQ.data?.errors.kpis ? (
          <div className="p-4">
            <ComingSoon
              variant="row"
              state="error"
              onRetry={() => dashQ.refetch()}
            />
          </div>
        ) : (
          <KpiStrip kpis={dashQ.data?.kpis ?? []} plain />
        )}
        <TimeOffBanner notice={dashQ.data?.timeOff ?? null} />
      </div>

      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
        <MetricCard
          title={t("ic_dashboard.sections.task_delivery")}
          metrics={dashQ.data?.taskDelivery ?? []}
          columns={1}
          mode={viewMode}
          personName={person?.display_name}
          onDrillClick={handleDrillClick}
          onRetry={() => dashQ.refetch()}
          {...sectionState("task_delivery")}
        />
        <MetricCard
          title={t("ic_dashboard.sections.git_output")}
          metrics={dashQ.data?.gitOutput ?? []}
          columns={1}
          mode={viewMode}
          personName={person?.display_name}
          onDrillClick={handleDrillClick}
          onRetry={() => dashQ.refetch()}
          {...sectionState("git_output")}
        />
      </div>

      <MetricCard
        title={t("ic_dashboard.sections.code_quality")}
        metrics={dashQ.data?.codeQuality ?? []}
        columns={3}
        mode={viewMode}
        personName={person?.display_name}
        onDrillClick={handleDrillClick}
        onRetry={() => dashQ.refetch()}
        {...sectionState("code_quality")}
      />

      <CollapsibleSection
        title={t("ic_dashboard.sections.loc_breakdown_title")}
        subtitle={t("ic_dashboard.sections.loc_breakdown_subtitle")}
        storageKey="insight:ic-dashboard:loc-breakdown"
      >
        <div className="p-4">
          {dashQ.isPending ? (
            <ComingSoon variant="card" state="loading" />
          ) : dashQ.isError || dashQ.data?.errors.loc_trend ? (
            <ComingSoon
              variant="card"
              state="error"
              onRetry={() => dashQ.refetch()}
            />
          ) : (
            <LocStackedBar data={dashQ.data?.locTrend ?? []} />
          )}
        </div>
      </CollapsibleSection>

      <CollapsibleSection
        title={t("ic_dashboard.sections.delivery_trends_title")}
        subtitle={t("ic_dashboard.sections.delivery_trends_subtitle")}
        storageKey="insight:ic-dashboard:delivery-trends"
      >
        <div className="p-4">
          {dashQ.isPending ? (
            <ComingSoon variant="card" state="loading" />
          ) : dashQ.isError || dashQ.data?.errors.delivery_trend ? (
            <ComingSoon
              variant="card"
              state="error"
              onRetry={() => dashQ.refetch()}
            />
          ) : (
            <DeliveryTrends data={dashQ.data?.deliveryTrend ?? []} />
          )}
        </div>
      </CollapsibleSection>

      <CollapsibleSection
        title={t("ic_dashboard.sections.ai_dev_tools_title")}
        storageKey="insight:ic-dashboard:ai-tools"
      >
        <div className="p-4">
          <MetricCard
            title={t("ic_dashboard.sections.ai_adoption")}
            metrics={dashQ.data?.aiAdoption ?? []}
            columns={2}
            mode={viewMode}
            personName={person?.display_name}
            onDrillClick={handleDrillClick}
            onRetry={() => dashQ.refetch()}
            {...sectionState("ai_adoption")}
          />
        </div>
      </CollapsibleSection>

      <CollapsibleSection
        title={t("ic_dashboard.sections.collaboration")}
        storageKey="insight:ic-dashboard:collaboration"
      >
        <div className="p-4">
          {(() => {
            const collab = dashQ.data?.collaboration ?? [];
            return (
              <MetricCard
                title={t("ic_dashboard.sections.collaboration")}
                metrics={collab}
                groups={[
                  {
                    heading: "Chat",
                    metrics: filterBulletsByLayoutGroup(collab, "chat"),
                  },
                  {
                    heading: "Email",
                    metrics: filterBulletsByLayoutGroup(collab, "email"),
                  },
                  {
                    heading: "Meetings",
                    metrics: filterBulletsByLayoutGroup(collab, "meetings"),
                  },
                  {
                    heading: "Files",
                    metrics: filterBulletsByLayoutGroup(collab, "files"),
                  },
                ]}
                mode={viewMode}
                personName={person?.display_name}
                onDrillClick={handleDrillClick}
                onRetry={() => dashQ.refetch()}
                {...sectionState("collaboration")}
              />
            );
          })()}
        </div>
      </CollapsibleSection>

      <DrillModal
        drill={drillQ.data ?? null}
        open={Boolean(drillId)}
        loading={drillQ.isPending && Boolean(drillId)}
        errored={drillQ.isError}
        onClose={() => setDrillId(null)}
        onRetry={() => drillQ.refetch()}
      />
    </div>
  );
}

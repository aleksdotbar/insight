import { useState } from "react";
import { useTranslation } from "react-i18next";

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
  useIcBulletSection,
  useIcDeliveryTrend,
  useIcDrill,
  useIcKpis,
  useIcLocTrend,
  useIcTimeOff,
} from "@/queries/ic-dashboard";
import type { IdentityPerson } from "@/types/insight";

export interface EngineeringDashboardProps {
  personId: string;
  person?: IdentityPerson | null;
}

function sectionState<T>(q: {
  isPending: boolean;
  isError: boolean;
  data: T | undefined;
}) {
  return {
    loading: q.isPending,
    errored: q.isError,
    revalidating: false,
  };
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

  const kpisQ = useIcKpis(personId, period, dateRange);
  const taskQ = useIcBulletSection(
    "task_delivery",
    personId,
    period,
    dateRange,
  );
  const gitQ = useIcBulletSection("git_output", personId, period, dateRange);
  const codeQ = useIcBulletSection(
    "code_quality",
    personId,
    period,
    dateRange,
  );
  const aiQ = useIcBulletSection("ai_adoption", personId, period, dateRange);
  const collabQ = useIcBulletSection(
    "collaboration",
    personId,
    period,
    dateRange,
  );
  const locQ = useIcLocTrend(personId, period, dateRange);
  const deliveryQ = useIcDeliveryTrend(personId, period, dateRange);
  const timeOffQ = useIcTimeOff(personId, dateRange);
  const drillQ = useIcDrill(personId, drillId, dateRange);

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
          <div className="flex min-w-0 flex-1 items-center gap-2">
            <SidebarTrigger className="md:hidden" />
            <PersonHeader person={person} fallbackEmail={personId} inline />
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <IcViewToggle
              person={personId}
              hasReports={(person?.subordinates?.length ?? 0) > 0}
            />
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
        {kpisQ.isPending ? (
          <div className="p-4">
            <ComingSoon variant="row" state="loading" />
          </div>
        ) : kpisQ.isError ? (
          <div className="p-4">
            <ComingSoon
              variant="row"
              state="error"
              onRetry={() => kpisQ.refetch()}
            />
          </div>
        ) : (
          <KpiStrip kpis={kpisQ.data ?? []} plain />
        )}
        <TimeOffBanner notice={timeOffQ.data ?? null} />
      </div>

      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
        <MetricCard
          title={t("ic_dashboard.sections.task_delivery")}
          metrics={taskQ.data ?? []}
          columns={1}
          mode={viewMode}
          personName={person?.display_name}
          onDrillClick={handleDrillClick}
          onRetry={() => taskQ.refetch()}
          {...sectionState(taskQ)}
        />
        <MetricCard
          title={t("ic_dashboard.sections.git_output")}
          metrics={gitQ.data ?? []}
          columns={1}
          mode={viewMode}
          personName={person?.display_name}
          onDrillClick={handleDrillClick}
          onRetry={() => gitQ.refetch()}
          {...sectionState(gitQ)}
        />
      </div>

      <MetricCard
        title={t("ic_dashboard.sections.code_quality")}
        metrics={codeQ.data ?? []}
        columns={3}
        mode={viewMode}
        personName={person?.display_name}
        onDrillClick={handleDrillClick}
        onRetry={() => codeQ.refetch()}
        {...sectionState(codeQ)}
      />

      <CollapsibleSection
        title={t("ic_dashboard.sections.loc_breakdown_title")}
        subtitle={t("ic_dashboard.sections.loc_breakdown_subtitle")}
        storageKey="insight:ic-dashboard:loc-breakdown"
      >
        <div className="p-4">
          {locQ.isPending ? (
            <ComingSoon variant="card" state="loading" />
          ) : locQ.isError ? (
            <ComingSoon
              variant="card"
              state="error"
              onRetry={() => locQ.refetch()}
            />
          ) : (
            <LocStackedBar data={locQ.data ?? []} />
          )}
        </div>
      </CollapsibleSection>

      <CollapsibleSection
        title={t("ic_dashboard.sections.delivery_trends_title")}
        subtitle={t("ic_dashboard.sections.delivery_trends_subtitle")}
        storageKey="insight:ic-dashboard:delivery-trends"
      >
        <div className="p-4">
          {deliveryQ.isPending ? (
            <ComingSoon variant="card" state="loading" />
          ) : deliveryQ.isError ? (
            <ComingSoon
              variant="card"
              state="error"
              onRetry={() => deliveryQ.refetch()}
            />
          ) : (
            <DeliveryTrends data={deliveryQ.data ?? []} />
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
            metrics={aiQ.data ?? []}
            columns={2}
            mode={viewMode}
            personName={person?.display_name}
            onDrillClick={handleDrillClick}
            onRetry={() => aiQ.refetch()}
            {...sectionState(aiQ)}
          />
        </div>
      </CollapsibleSection>

      <CollapsibleSection
        title={t("ic_dashboard.sections.collaboration")}
        storageKey="insight:ic-dashboard:collaboration"
      >
        <div className="p-4">
          <MetricCard
            title={t("ic_dashboard.sections.collaboration")}
            metrics={collabQ.data ?? []}
            columns={2}
            mode={viewMode}
            personName={person?.display_name}
            onDrillClick={handleDrillClick}
            onRetry={() => collabQ.refetch()}
            {...sectionState(collabQ)}
          />
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

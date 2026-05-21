import { useTranslation } from "react-i18next";

import { IcViewToggle } from "@/components/ic-view-toggle";
import { SidebarTrigger } from "@/components/ui/sidebar";
import { CollapsibleSection } from "@/components/widgets/collapsible-section";
import { ComingSoon } from "@/components/widgets/coming-soon";
import { DealFlowChart } from "@/components/widgets/deal-flow-chart";
import { KpiStrip, type KpiStripKpi } from "@/components/widgets/kpi-strip";
import { MetricCard } from "@/components/widgets/metric-card";
import { PeriodSelectorBar } from "@/components/widgets/period-selector-bar";
import { PersonHeader } from "@/components/widgets/person-header";
import { SalesPacingBand } from "@/components/widgets/sales-pacing-band";
import { usePeriod } from "@/hooks/use-period";
import {
  formatCurrencyCompact,
  formatNumber,
  formatWinRate,
} from "@/lib/format";
import { useSalesDashboardQueries } from "@/queries/sales-dashboard";
import type { CrmKpis, IdentityPerson } from "@/types/insight";

export interface SalesDashboardProps {
  personId: string;
  person?: IdentityPerson | null;
}

function heroKpiRows(
  kpis: CrmKpis,
  t: (key: string, opts?: Record<string, unknown>) => string,
): KpiStripKpi[] {
  return [
    {
      metric_key: "deals_opened",
      label: t("sales_dashboard.hero.deals_opened"),
      value: formatNumber(kpis.dealsOpened, ""),
      sublabel: t("sales_dashboard.hero.deals_opened_sublabel"),
    },
    {
      metric_key: "deals_closed",
      label: t("sales_dashboard.hero.deals_closed"),
      value: formatNumber(kpis.dealsClosed, ""),
      sublabel: t("sales_dashboard.hero.deals_closed_sublabel"),
    },
    {
      metric_key: "deals_value_closed",
      label: t("sales_dashboard.hero.closed_value"),
      value: formatCurrencyCompact(kpis.dealsValueClosed),
      sublabel: t("sales_dashboard.hero.closed_value_sublabel"),
    },
    {
      metric_key: "win_rate",
      label: t("sales_dashboard.hero.win_rate"),
      value: formatWinRate(kpis.dealsWon, kpis.dealsClosed),
      sublabel: t("sales_dashboard.hero.win_rate_sublabel"),
    },
    {
      metric_key: "pipeline_value",
      label: t("sales_dashboard.hero.pipeline_now"),
      value: formatCurrencyCompact(kpis.pipelineValue),
      sublabel: t("sales_dashboard.hero.pipeline_sublabel", {
        count: formatNumber(kpis.pipelineCount, ""),
      }),
    },
  ];
}

export function SalesDashboard({
  personId,
  person: personProp,
}: SalesDashboardProps) {
  const { t } = useTranslation();
  const { period, customRange, dateRange, setPeriod, setCustomRange } =
    usePeriod();
  const { kpisQ, prevKpisQ, flowQ, qualityQ, activityQ } =
    useSalesDashboardQueries(personId, period, dateRange);

  // Dispatcher (`<IcDashboardScreen>`) owns the identity fetch and forwards
  // it as a prop — children trust it and don't re-issue the query.
  const person = personProp ?? null;
  const kpis = kpisQ.data ?? null;
  const prevKpis = prevKpisQ.data ?? null;
  const flow = flowQ.data ?? [];
  const quality = qualityQ.data ?? [];
  const activity = activityQ.data ?? [];

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
        ) : kpis ? (
          <KpiStrip kpis={heroKpiRows(kpis, t)} plain />
        ) : (
          <div className="p-4">
            <ComingSoon variant="row" state="empty" />
          </div>
        )}
      </div>

      {kpis ? (
        <SalesPacingBand kpis={kpis} prevKpis={prevKpis} range={dateRange} />
      ) : null}

      <MetricCard
        title={t("sales_dashboard.sections.velocity_quality")}
        metrics={quality}
        columns={2}
        loading={qualityQ.isPending}
        errored={qualityQ.isError}
        revalidating={qualityQ.isFetching && !qualityQ.isPending}
        onRetry={() => qualityQ.refetch()}
        personName={person?.display_name}
      />

      <MetricCard
        title={t("sales_dashboard.sections.outreach_activity")}
        metrics={activity}
        columns={2}
        loading={activityQ.isPending}
        errored={activityQ.isError}
        revalidating={activityQ.isFetching && !activityQ.isPending}
        onRetry={() => activityQ.refetch()}
        personName={person?.display_name}
      />

      <CollapsibleSection
        title={t("sales_dashboard.sections.deal_flow_title")}
        subtitle={t("sales_dashboard.sections.deal_flow_subtitle")}
        storageKey="insight:sales-dashboard:deal-flow"
      >
        <div className="p-4">
          {flowQ.isPending && flow.length === 0 ? (
            <ComingSoon variant="card" state="loading" />
          ) : flowQ.isError ? (
            <ComingSoon
              variant="card"
              state="error"
              onRetry={() => flowQ.refetch()}
            />
          ) : (
            <DealFlowChart data={flow} />
          )}
        </div>
      </CollapsibleSection>
    </div>
  );
}

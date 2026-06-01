import { useExecViewConfig } from "@/api/view-configs";
import { OrgHealthRadar } from "@/components/widgets/org-health-radar";
import { OrgKpiCards } from "@/components/widgets/org-kpi-cards";
import { PeriodSelectorBar } from "@/components/widgets/period-selector-bar";
import { SidebarTrigger } from "@/components/ui/sidebar";
import { TeamMetricsBar } from "@/components/widgets/team-metrics-bar";
import { TeamsTable } from "@/components/widgets/teams-table";
import { usePeriod } from "@/hooks/use-period";
import { useExecSummary } from "@/queries/executive-view";

export function ExecutiveViewScreen() {
  const { period, customRange, dateRange, setPeriod, setCustomRange } =
    usePeriod();
  const execViewConfig = useExecViewConfig();
  const execQ = useExecSummary(dateRange, execViewConfig.column_thresholds);
  const teams = execQ.data?.teams ?? [];
  const orgKpis = execQ.data?.orgKpis ?? null;

  return (
    <div className="flex flex-col gap-4 p-6">
      <div className="bg-background/95 border-border/60 sticky top-0 z-20 -mx-6 -mt-6 flex flex-wrap items-center justify-between gap-3 border-b px-6 pt-6 pb-3 backdrop-blur-sm">
        <div className="flex min-w-0 flex-1 items-center gap-2">
          <SidebarTrigger className="md:hidden" />
          <div className="min-w-0">
            <div className="text-foreground truncate text-lg leading-tight font-bold">
              Executive View
            </div>
            <div className="text-muted-foreground truncate text-sm">
              All teams · Organization overview
            </div>
          </div>
        </div>
        <PeriodSelectorBar
          period={period}
          customRange={customRange}
          onPeriodChange={setPeriod}
          onRangeChange={setCustomRange}
        />
      </div>

      <OrgKpiCards
        teams={teams}
        orgKpis={orgKpis}
        columnThresholds={execViewConfig.column_thresholds}
      />

      <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
        <div className="border-border bg-card rounded-lg border p-4 shadow-sm">
          {orgKpis ? <OrgHealthRadar orgKpis={orgKpis} /> : <div className="h-60" />}
        </div>
        <div className="border-border bg-card rounded-lg border p-4 shadow-sm">
          <TeamMetricsBar teams={teams} />
        </div>
      </div>

      <TeamsTable
        teams={teams}
        loading={execQ.isPending}
        columnThresholds={execViewConfig.column_thresholds}
      />
    </div>
  );
}

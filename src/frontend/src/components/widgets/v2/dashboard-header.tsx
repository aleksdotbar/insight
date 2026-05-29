import { IcViewToggle } from "@/components/ic-view-toggle";
import { PeriodSelectorBar } from "@/components/widgets/period-selector-bar";
import { SidebarTrigger } from "@/components/ui/sidebar";
import { usePeriod } from "@/hooks/use-period";

export interface DashboardHeaderProps {
  title: string;
  subtitle?: string | null;
  person: string;
  hasReports: boolean;
}

export function DashboardHeader({
  title,
  subtitle,
  person,
  hasReports,
}: DashboardHeaderProps) {
  const { period, customRange, setPeriod, setCustomRange } = usePeriod();

  return (
    <header className="sticky top-0 z-20 flex flex-wrap items-center justify-between gap-3 border-b bg-background/95 px-4 py-3 backdrop-blur-sm">
      <div className="flex items-center gap-2">
        <SidebarTrigger />
        <div className="flex flex-col">
          <h1 className="text-xl font-semibold tracking-tight">{title}</h1>
          {subtitle ? (
            <p className="text-xs text-muted-foreground">{subtitle}</p>
          ) : null}
        </div>
      </div>
      <div className="flex flex-wrap items-center gap-2">
        <IcViewToggle person={person} hasReports={hasReports} />
        <PeriodSelectorBar
          period={period}
          customRange={customRange}
          onPeriodChange={setPeriod}
          onRangeChange={setCustomRange}
        />
      </div>
    </header>
  );
}

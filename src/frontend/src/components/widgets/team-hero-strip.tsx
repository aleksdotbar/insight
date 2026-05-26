import { Badge } from "@/components/ui/badge";
import { Card } from "@/components/ui/card";
import { cn } from "@/lib/utils";
import type { TeamKpi } from "@/types/insight";

import { ComingSoon } from "./coming-soon";
import { MetricInfo } from "./metric-info";

export interface TeamHeroStripProps {
  teamKpis: TeamKpi[];
}

const CHIP_CLASS: Record<"good" | "warn" | "bad", string> = {
  good: "bg-success/15 text-success",
  warn: "bg-warning/15 text-warning",
  bad: "bg-destructive/15 text-destructive",
};

const CARD_BORDER: Record<number, string> = {
  0: "",
  1: "border-l border-border",
  2: "border-t sm:border-t-0 sm:border-l border-border",
  3: "border-t sm:border-t-0 border-l border-border",
  4: "border-t sm:border-t-0 sm:border-l border-border",
  5: "border-t sm:border-t-0 border-l border-border",
};

const DEFAULT_GRID_COLS = "grid-cols-2 sm:grid-cols-4";
const GRID_COLS: Record<number, string> = {
  1: "grid-cols-1 sm:grid-cols-1",
  2: "grid-cols-2 sm:grid-cols-2",
  3: "grid-cols-2 sm:grid-cols-3",
  4: "grid-cols-2 sm:grid-cols-4",
  5: "grid-cols-2 sm:grid-cols-5",
  6: "grid-cols-2 sm:grid-cols-6",
};

function KpiCard({ kpi, idx }: { kpi: TeamKpi; idx: number }) {
  return (
    <div
      className={cn(
        "bg-card flex flex-col gap-0.5 p-3",
        CARD_BORDER[idx] ?? "border-l border-border",
      )}
    >
      <div className="text-foreground text-xl font-extrabold leading-tight tracking-tight">
        {kpi.value}
        {kpi.unit ? (
          <span className="text-muted-foreground ml-0.5 text-xs font-semibold">
            {kpi.unit}
          </span>
        ) : null}
      </div>
      <div className="flex items-center gap-0.5">
        <span className="text-foreground text-sm font-semibold">
          {kpi.label}
        </span>
        {kpi.description ? <MetricInfo description={kpi.description} /> : null}
      </div>
      {kpi.sublabel ? (
        <div className="text-muted-foreground text-xs">{kpi.sublabel}</div>
      ) : null}
      <Badge
        variant="secondary"
        className={cn("mt-1 text-xs font-bold", CHIP_CLASS[kpi.status])}
      >
        {kpi.chipLabel
          ? kpi.chipLabel
          : kpi.status.charAt(0).toUpperCase() + kpi.status.slice(1)}
      </Badge>
    </div>
  );
}

export function TeamHeroStrip({ teamKpis }: TeamHeroStripProps) {
  const cols = teamKpis.length;
  const gridClass = GRID_COLS[cols] ?? DEFAULT_GRID_COLS;

  return (
    <Card className="overflow-hidden">
      {cols === 0 ? (
        <div className="p-3">
          <ComingSoon variant="card" />
        </div>
      ) : (
        <div className={cn("grid", gridClass)}>
          {teamKpis.map((kpi, i) => (
            <KpiCard key={kpi.metric_key} kpi={kpi} idx={i} />
          ))}
        </div>
      )}
    </Card>
  );
}

import { memo } from "react";

import { Badge } from "@/components/ui/badge";
import { Card, CardContent } from "@/components/ui/card";
import { cn } from "@/lib/utils";
import { MetricInfo } from "@/components/widgets/metric-info";
import { getPeriodSuffix } from "@/lib/insight/period-suffix";

import { ComingSoon } from "./coming-soon";

const DELTA_CLASS: Record<string, string> = {
  good: "bg-success/15 text-success",
  warn: "bg-warning/15 text-warning",
  neutral: "bg-muted text-muted-foreground",
  bad: "bg-destructive/15 text-destructive",
};

export interface KpiStripKpi {
  metric_key: string;
  label: string;
  value: string | null;
  unit?: string;
  sublabel?: string;
  description?: string;
  delta?: string;
  delta_type?: "good" | "warn" | "bad" | "neutral";
  period?: string;
}

export interface KpiStripProps {
  kpis: KpiStripKpi[];
  plain?: boolean;
}

function KpiCell({ kpi, index }: { kpi: KpiStripKpi; index: number }) {
  const suffix = getPeriodSuffix(kpi.unit, kpi.period);
  const deltaClass = kpi.delta_type ? DELTA_CLASS[kpi.delta_type] : null;

  const mobileBorder = [
    index % 2 !== 0 ? "border-border border-l sm:border-l-0" : "",
    index >= 2 ? "border-border/60 border-t sm:border-t-0" : "",
  ]
    .filter(Boolean)
    .join(" ");

  const isMissing = kpi.value === null;

  return (
    <div className={cn("relative flex-1 px-3 py-2", mobileBorder)}>
      {index > 0 ? (
        <div className="bg-border absolute top-[15%] left-0 hidden h-[70%] w-px sm:block" />
      ) : null}

      {isMissing ? (
        <ComingSoon variant="chip" />
      ) : (
        <div className="flex items-baseline gap-px">
          <span className="text-foreground text-xl leading-tight font-extrabold">
            {kpi.value}
          </span>
          {kpi.unit ? (
            <sup className="text-muted-foreground text-xs font-semibold">
              {kpi.unit}
            </sup>
          ) : null}
          {suffix ? (
            <span className="text-muted-foreground ml-0.5 text-xs">
              {suffix}
            </span>
          ) : null}
        </div>
      )}

      <div className="mt-0.5 flex items-center">
        <span className="text-foreground text-sm font-semibold">{kpi.label}</span>
        {kpi.description ? <MetricInfo description={kpi.description} /> : null}
      </div>

      {kpi.sublabel ? (
        <div className="text-muted-foreground text-xs">{kpi.sublabel}</div>
      ) : null}

      {!isMissing && kpi.delta && deltaClass ? (
        <Badge variant="secondary" className={cn("mt-1 text-xs font-bold", deltaClass)}>
          {kpi.delta}
        </Badge>
      ) : null}
    </div>
  );
}

function KpiStripImpl({ kpis, plain = false }: KpiStripProps) {
  const body =
    kpis.length === 0 ? (
      <div className="p-3">
        <ComingSoon variant="card" />
      </div>
    ) : (
      <div className="grid grid-cols-2 sm:flex">
        {kpis.map((kpi, i) => (
          <KpiCell key={kpi.metric_key} kpi={kpi} index={i} />
        ))}
      </div>
    );

  if (plain) return body;
  return (
    <Card className="shadow-sm">
      <CardContent className="p-0">{body}</CardContent>
    </Card>
  );
}

export const KpiStrip = memo(KpiStripImpl);

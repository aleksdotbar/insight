import { useTranslation } from "react-i18next";

import { Card } from "@/components/ui/card";
import {
  formatCurrencyCompact,
  formatDeltaPct,
  formatPeriodProgress,
} from "@/lib/format";
import { cn } from "@/lib/utils";
import type { CrmKpis } from "@/types/insight";

export interface SalesPacingBandProps {
  kpis: CrmKpis;
  prevKpis: CrmKpis | null;
  range: { from: string; to: string };
}

export function SalesPacingBand({
  kpis,
  prevKpis,
  range,
}: SalesPacingBandProps) {
  const { t } = useTranslation();
  const delta = prevKpis
    ? formatDeltaPct(kpis.dealsValueClosed, prevKpis.dealsValueClosed)
    : null;
  const deltaPositive = delta !== null && delta.startsWith("+");
  return (
    <Card size="sm" className="px-4 py-3">
      <div className="flex flex-wrap items-center gap-x-6 gap-y-2 text-sm">
        <div className="flex items-center gap-2">
          <span className="text-muted-foreground">
            {t("sales_dashboard.pacing.closed_this_period")}
          </span>
          <span className="text-foreground font-semibold">
            {formatCurrencyCompact(kpis.dealsValueClosed)}
          </span>
        </div>
        {prevKpis ? (
          <div className="flex items-center gap-2">
            <span className="text-muted-foreground">
              {t("sales_dashboard.pacing.vs_prior_year")}
            </span>
            <span className="text-foreground font-semibold">
              {formatCurrencyCompact(prevKpis.dealsValueClosed)}
            </span>
            {delta !== null ? (
              <span
                className={cn(
                  "rounded px-1.5 py-0.5 text-xs font-semibold",
                  deltaPositive
                    ? "bg-success/15 text-success"
                    : "bg-destructive/15 text-destructive",
                )}
              >
                {delta}
              </span>
            ) : null}
          </div>
        ) : null}
        <div className="ml-auto flex items-center gap-2">
          <span className="text-muted-foreground">
            {formatPeriodProgress(range.from, range.to)}
          </span>
        </div>
      </div>
    </Card>
  );
}

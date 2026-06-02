import { useCatalog } from "@/api/use-catalog";
import { Card } from "@/components/ui/card";
import { useSettings } from "@/hooks/use-settings";
import {
  peerStatusForRow,
  type CatalogByKey,
} from "@/lib/insight/v2/peer-status";
import {
  applyFocus,
  PEER_FILL,
  type PeerStats,
  type PeerStatusWithNeutral,
} from "@/lib/peers";
import { cn } from "@/lib/utils";
import type { BulletMetric } from "@/types/insight";

export interface SectionStatusItem<T extends string> {
  id: T;
  label: string;
  rows: BulletMetric[];
}

interface SectionAggregate<T extends string> {
  id: T;
  label: string;
  status: PeerStatusWithNeutral;
  belowCount: number;
  topCount: number;
  evaluatedCount: number;
}

function aggregate<T extends string>(
  section: SectionStatusItem<T>,
  cohortStats: Map<string, PeerStats> | undefined,
  byMetricKey: CatalogByKey,
): SectionAggregate<T> {
  let below = 0;
  let top = 0;
  let evaluated = 0;
  for (const r of section.rows) {
    const ps = peerStatusForRow(r, cohortStats, byMetricKey);
    if (ps === "neutral") continue;
    evaluated += 1;
    if (ps === "bottom") below += 1;
    else if (ps === "top") top += 1;
  }
  let status: PeerStatusWithNeutral = "neutral";
  if (evaluated > 0) {
    if (below / evaluated >= 0.5) status = "bottom";
    else if (below === 0 && top / evaluated >= 0.3) status = "top";
    else status = "in_pack";
  }
  return {
    id: section.id,
    label: section.label,
    status,
    belowCount: below,
    topCount: top,
    evaluatedCount: evaluated,
  };
}

export interface SectionStatusProps<T extends string> {
  sections: ReadonlyArray<SectionStatusItem<T>>;
  peerLabel: string;
  cols: "four" | "five";
  cohortStats?: Map<string, PeerStats>;
  onSectionClick: (id: T) => void;
}

export function SectionStatus<T extends string>({
  sections,
  peerLabel,
  cols,
  cohortStats,
  onSectionClick,
}: SectionStatusProps<T>) {
  const { focusMode } = useSettings();
  const { byMetricKey } = useCatalog();
  const aggregates = sections.map((s) =>
    aggregate(s, cohortStats, byMetricKey),
  );
  const gridCols =
    cols === "five"
      ? "grid grid-cols-1 gap-3 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-5"
      : "grid grid-cols-1 gap-3 sm:grid-cols-2 md:grid-cols-4";

  return (
    <section>
      <h2 className="mb-3 text-xs font-medium uppercase tracking-wider text-muted-foreground">
        Section status
      </h2>
      <div className={gridCols}>
        {aggregates.map((a) => {
          const status = applyFocus(a.status, focusMode);
          const summary =
            a.evaluatedCount === 0
              ? "no peer data"
              : a.belowCount === 0 && a.topCount === 0
                ? "all on par"
                : `${a.belowCount} below ${peerLabel} · ${a.topCount} in top`;
          return (
            <Card
              key={a.id}
              data-size="sm"
              render={
                <button
                  type="button"
                  onClick={() => onSectionClick(a.id)}
                  aria-label={`Open ${a.label} details`}
                />
              }
              className="min-h-16 items-start gap-1 px-4 py-3 text-left transition-colors hover:bg-accent"
            >
              <span className="flex w-full items-center gap-2">
                <span
                  className={cn(
                    "size-2 shrink-0 rounded-full",
                    PEER_FILL[status],
                  )}
                  aria-hidden
                />
                <span className="min-w-0 flex-1 truncate text-sm font-semibold leading-tight">
                  {a.label}
                </span>
              </span>
              <span className="text-xs text-muted-foreground">{summary}</span>
            </Card>
          );
        })}
      </div>
    </section>
  );
}

import { useState } from "react";
import { AlertTriangle } from "lucide-react";

import { useCatalog } from "@/api/use-catalog";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { useSettings } from "@/hooks/use-settings";
import { bulletCatalogKey } from "@/lib/insight/v2/peer-status";
import {
  applyFocus,
  PEER_TEXT,
  peerStatusVsQuartiles,
  type PeerStats,
} from "@/lib/peers";
import type { IcSectionId } from "@/lib/insight/v2/sections";
import { cn } from "@/lib/utils";
import type { BulletMetric } from "@/types/insight";

const COLLAPSED_ATTENTION = 3;
const COLLAPSE_THRESHOLD = 6;

interface AttentionItem {
  section: IcSectionId;
  row: BulletMetric;
  median: number | null;
  relGap: number;
}

export interface IcNeedsAttentionSection {
  id: IcSectionId;
  label: string;
  rows: BulletMetric[];
}

export interface IcNeedsAttentionProps {
  sections: IcNeedsAttentionSection[];
  cohortStats?: Map<string, PeerStats>;
  onSectionClick: (id: IcSectionId) => void;
}

export function IcNeedsAttention({
  sections,
  cohortStats,
  onSectionClick,
}: IcNeedsAttentionProps) {
  const { focusMode } = useSettings();
  const { byMetricKey } = useCatalog();
  const [showAll, setShowAll] = useState(false);

  const attentionAll: AttentionItem[] = [];
  for (const s of sections) {
    for (const r of s.rows) {
      // schema_status='error' rows never trigger the attention surface —
      // we can't compare a broken metric to peers. Missing-id rows likewise
      // collapse out (no catalog row → no higher_is_better signal).
      if (r.schema_error) continue;
      const value = Number(r.value);
      if (!Number.isFinite(value)) continue;
      const stats = cohortStats?.get(r.metric_key);
      if (!stats) continue;
      const m = byMetricKey(bulletCatalogKey(r));
      if (!m) continue;
      const higherIsBetter = m.higher_is_better;
      const ps = peerStatusVsQuartiles(value, stats, higherIsBetter);
      if (ps !== "bottom") continue;
      const median = stats.p50;
      const denom = Math.abs(median) > 1e-9 ? Math.abs(median) : 1;
      const relGap = higherIsBetter
        ? (median - value) / denom
        : (value - median) / denom;
      attentionAll.push({ section: s.id, row: r, median, relGap });
    }
  }
  attentionAll.sort((a, b) => b.relGap - a.relGap);

  if (attentionAll.length === 0) return null;

  const shouldCollapse = attentionAll.length >= COLLAPSE_THRESHOLD;
  const visible =
    !shouldCollapse || showAll
      ? attentionAll
      : attentionAll.slice(0, COLLAPSED_ATTENTION);
  const badStatus = applyFocus("bottom", focusMode);

  return (
    <section>
      <h2 className="mb-3 text-xs font-medium uppercase tracking-wider text-muted-foreground">
        Needs attention
      </h2>
      <Alert variant="destructive">
        <AlertTriangle />
        <AlertTitle>{attentionAll.length} metrics below peers</AlertTitle>
        <AlertDescription>
          <ul className="grid grid-cols-1 gap-x-8 gap-y-1 md:grid-cols-2">
            {visible.map(({ row, section, median }) => (
              <li key={`${section}-${row.metric_key}`}>
                <button
                  type="button"
                  onClick={() => onSectionClick(section)}
                  className="-mx-2 flex w-[calc(100%+1rem)] items-baseline gap-2 rounded px-2 py-1 text-left text-sm transition-colors hover:bg-accent"
                >
                  <span className="min-w-0 truncate text-foreground">
                    {row.label}
                  </span>
                  <span
                    className={cn(
                      "shrink-0 font-mono font-bold tabular-nums",
                      PEER_TEXT[badStatus],
                    )}
                  >
                    {row.value}
                    {row.unit ? ` ${row.unit}` : ""}
                  </span>
                  {median != null ? (
                    <span className="shrink-0 whitespace-nowrap tabular-nums text-muted-foreground">
                      vs median {Math.round(median * 10) / 10}
                      {row.unit ? ` ${row.unit}` : ""}
                    </span>
                  ) : null}
                </button>
              </li>
            ))}
            {shouldCollapse ? (
              <li className="md:col-span-2">
                <button
                  type="button"
                  onClick={() => setShowAll((v) => !v)}
                  className="rounded text-xs font-semibold text-muted-foreground transition-colors hover:text-foreground"
                >
                  {showAll
                    ? "Show fewer"
                    : `Show ${attentionAll.length - COLLAPSED_ATTENTION} more`}
                </button>
              </li>
            ) : null}
          </ul>
        </AlertDescription>
      </Alert>
    </section>
  );
}

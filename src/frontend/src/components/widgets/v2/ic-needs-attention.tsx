import { useState } from "react";
import { AlertTriangle } from "lucide-react";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { useSettings } from "@/hooks/use-settings";
import type { AttentionItem } from "@/lib/insight/attention";
import type { GroupId } from "@/lib/insight/groups";
import { PEER_TEXT, applyFocus } from "@/lib/peers";
import { cn } from "@/lib/utils";

const COLLAPSED_ATTENTION = 3;
const COLLAPSE_THRESHOLD = 6;

export interface IcNeedsAttentionProps {
  items: AttentionItem[];
  onOpenGroup: (id: GroupId) => void;
}

/**
 * Cross-group "needs attention" surface. Items arrive precomputed from the
 * per-source selectors in `lib/insight/attention.ts`; this component only
 * ranks (relGap descending), collapses, and renders.
 */
export function IcNeedsAttention({
  items,
  onOpenGroup,
}: IcNeedsAttentionProps) {
  const { focusMode } = useSettings();
  const [showAll, setShowAll] = useState(false);

  const attentionAll = [...items].sort((a, b) => b.relGap - a.relGap);

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
            {visible.map((item) => (
              <li key={`${item.group}-${item.key}`}>
                <button
                  type="button"
                  onClick={() => onOpenGroup(item.group)}
                  className="-mx-2 flex w-[calc(100%+1rem)] items-baseline gap-2 rounded px-2 py-1 text-left text-sm transition-colors hover:bg-accent"
                >
                  <span className="min-w-0 truncate text-foreground">
                    {item.label}
                  </span>
                  <span
                    className={cn(
                      "shrink-0 font-mono font-bold tabular-nums",
                      PEER_TEXT[badStatus],
                    )}
                  >
                    {item.valueText}
                  </span>
                  {item.medianText ? (
                    <span className="shrink-0 whitespace-nowrap tabular-nums text-muted-foreground">
                      {item.medianText}
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

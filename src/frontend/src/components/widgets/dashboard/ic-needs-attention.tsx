import { ChevronRight } from "lucide-react";
import { useState } from "react";

import { Card, CardContent } from "@/components/ui/card";
import { useSettings } from "@/hooks/use-settings";
import type { AttentionItem } from "@/lib/insight/attention";
import type { GroupId } from "@/lib/insight/groups";
import { PEER_TEXT, applyFocus } from "@/lib/peers";
import { cn } from "@/lib/utils";

const COLLAPSED_ATTENTION = 6;
const COLLAPSE_THRESHOLD = 7;

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
      <h2 className="mb-3 text-xs font-medium tracking-wider text-muted-foreground uppercase">
        Needs attention
      </h2>
      <Card data-size="sm">
        <CardContent className="text-sm">
          <ul className="grid grid-cols-1 gap-x-8 gap-y-1 md:grid-cols-2">
            {visible.map((item) => (
              <li key={`${item.group}-${item.key}`}>
                <button
                  type="button"
                  onClick={() => onOpenGroup(item.group)}
                  /* A grid, not a sentence: the values used to start wherever
                     each label happened to end, so four rows put four numbers
                     at four different x-positions. Numbers line up in their own
                     right-aligned column now, and the comparison in the next
                     one, so the eye reads down instead of hunting across.
                     The value/gap columns carry a MINIMUM width because each
                     row is its own grid: `auto` would size every row to its
                     own content and the numbers would land wherever their
                     labels ended, which is the thing being fixed. */
                  className="-mx-2 grid w-[calc(100%+1rem)] grid-cols-[minmax(0,1fr)_auto_auto_auto] items-baseline gap-x-2 rounded px-2 py-1 text-left text-sm transition-colors hover:bg-accent"
                >
                  <span className="min-w-0 truncate text-foreground">
                    {item.label}
                  </span>
                  <span
                    className={cn(
                      "min-w-[5.5rem] justify-self-end text-right tabular-nums",
                      PEER_TEXT[badStatus]
                    )}
                  >
                    {item.valueText}
                  </span>
                  <span className="min-w-[9rem] justify-self-end text-right text-xs whitespace-nowrap text-muted-foreground tabular-nums">
                    {item.medianText ? (
                      <>
                        {item.gapText ? <>{item.gapText} vs </> : null}
                        median {item.medianText}
                      </>
                    ) : null}
                  </span>
                  {/* Same standing affordance as every other openable surface:
                      a row that only reacts to hover is indistinguishable from
                      a line of text until the mouse happens to cross it. */}
                  <ChevronRight
                    className="size-3.5 shrink-0 self-center text-muted-foreground/50"
                    aria-hidden
                  />
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
        </CardContent>
      </Card>
    </section>
  );
}

import { ChevronRight } from "lucide-react";

import { Card, CardContent } from "@/components/ui/card";
import { useSettings } from "@/hooks/use-settings";
import {
  applyFocus,
  PEER_BORDER,
  PEER_TEXT,
  type PeerStatusWithNeutral,
} from "@/lib/peers";
import { cn } from "@/lib/utils";
import type { TeamMember } from "@/types/insight";

function rollupStatus(
  belowCount: number,
  topCount: number,
): PeerStatusWithNeutral {
  if (belowCount > 0) return "bottom";
  if (topCount > 0) return "top";
  return "in_pack";
}

export interface TriageRow {
  member: TeamMember;
  cells: {
    label: string;
    short: string;
    value: number | null;
    status: PeerStatusWithNeutral;
  }[];
  belowCount: number;
  topCount: number;
}

export interface TriageListProps {
  rows: TriageRow[];
  onMemberClick: (member: TeamMember) => void;
}

export function TriageList({ rows, onMemberClick }: TriageListProps) {
  const { focusMode } = useSettings();
  return (
    <div className="flex flex-col gap-2">
      {rows.map((r) => {
        const worst = r.cells.find((c) => c.status === "bottom");
        const status = applyFocus(
          rollupStatus(r.belowCount, r.topCount),
          focusMode,
        );
        return (
          <Card
            key={r.member.person_id}
            className={cn(
              "border-l-2 transition-colors hover:bg-accent",
              PEER_BORDER[status],
            )}
            onClick={() => onMemberClick(r.member)}
          >
            <CardContent className="flex items-center justify-between gap-2 px-3 py-3">
              <div className="flex min-w-0 flex-col gap-0.5">
                <div className="flex items-baseline gap-2">
                  <span className="truncate text-sm font-medium leading-tight">
                    {r.member.name}
                  </span>
                  {r.belowCount > 0 ? (
                    <span className={cn("text-xs", PEER_TEXT[applyFocus("bottom", focusMode)])}>
                      {r.belowCount} issue{r.belowCount === 1 ? "" : "s"}
                    </span>
                  ) : (
                    <span className="text-xs text-muted-foreground">on par</span>
                  )}
                </div>
                {worst ? (
                  <p className="truncate text-xs text-muted-foreground">
                    worst: {worst.label}
                  </p>
                ) : null}
              </div>
              <ChevronRight
                className="size-4 shrink-0 text-muted-foreground"
                aria-hidden
              />
            </CardContent>
          </Card>
        );
      })}
    </div>
  );
}

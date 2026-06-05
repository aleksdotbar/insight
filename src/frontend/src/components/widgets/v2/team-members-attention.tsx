import { AlertTriangle } from "lucide-react";

import { useCatalog } from "@/api/use-catalog";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { useSettings } from "@/hooks/use-settings";
import { bulletCatalogKey } from "@/lib/insight/v2/peer-status";
import {
  applyFocus,
  PEER_TEXT,
  peerStatsFor,
  peerStatusVsQuartiles,
  type PeerStats,
} from "@/lib/peers";
import { cn } from "@/lib/utils";
import type { BulletMetric, TeamMember } from "@/types/insight";

export interface TeamMembersAttentionProps {
  members: TeamMember[];
  bulletsByPerson?: Map<string, BulletMetric[]>;
  onMemberClick: (member: TeamMember) => void;
}

export function TeamMembersAttention({
  members,
  bulletsByPerson,
  onMemberClick,
}: TeamMembersAttentionProps) {
  const { focusMode } = useSettings();
  const { byMetricKey } = useCatalog();

  // Cohort = the displayed team (the manager's reports). Each metric's
  // quartiles are computed client-side from the members shown, so this
  // surface uses the same team cohort as the heatmap — no separate query.
  const cohortByMetric = new Map<string, PeerStats>();
  {
    const valuesByMetric = new Map<string, number[]>();
    for (const m of members) {
      for (const b of bulletsByPerson?.get(m.person_id.toLowerCase()) ?? []) {
        if (b.schema_error) continue;
        const v = Number(b.value);
        if (!Number.isFinite(v)) continue;
        const arr = valuesByMetric.get(b.metric_key);
        if (arr) arr.push(v);
        else valuesByMetric.set(b.metric_key, [v]);
      }
    }
    for (const [k, vals] of valuesByMetric) {
      const stats = peerStatsFor(vals);
      if (stats) cohortByMetric.set(k, stats);
    }
  }

  const attention = members
    .map((m) => {
      const bullets = bulletsByPerson?.get(m.person_id.toLowerCase()) ?? [];
      let belowCount = 0;
      for (const b of bullets) {
        // schema_error / missing-id rows can't contribute to the "below
        // peers" count — they collapse to neutral per DESIGN §3.3.
        if (b.schema_error) continue;
        const stats = cohortByMetric.get(b.metric_key);
        const value = Number(b.value);
        if (!stats || !Number.isFinite(value)) continue;
        const catalogRow = byMetricKey(bulletCatalogKey(b));
        if (!catalogRow) continue;
        const ps = peerStatusVsQuartiles(
          value,
          stats,
          catalogRow.higher_is_better,
        );
        if (ps === "bottom") belowCount += 1;
      }
      return { member: m, belowCount };
    })
    .filter((x) => x.belowCount > 0)
    .sort((a, b) => b.belowCount - a.belowCount)
    .slice(0, 6);

  if (attention.length === 0) return null;

  const subtitle =
    members.length > 0
      ? `${members.length} peers under the same supervisor`
      : "Peers under the same supervisor";
  const badStatus = applyFocus("bottom", focusMode);

  return (
    <section>
      <h2 className="mb-3 text-xs font-medium uppercase tracking-wider text-muted-foreground">
        Members needing attention
      </h2>
      <Alert variant="destructive">
        <AlertTriangle />
        <AlertTitle>{attention.length} members below peers</AlertTitle>
        <AlertDescription className="flex flex-col gap-2">
          <span className="text-[11px] text-muted-foreground">{subtitle}</span>
          <ul className="grid grid-cols-1 gap-x-8 gap-y-1 md:grid-cols-2">
            {attention.map(({ member, belowCount }) => (
              <li key={member.person_id}>
                <button
                  type="button"
                  onClick={() => onMemberClick(member)}
                  className="-mx-2 flex w-[calc(100%+1rem)] items-baseline gap-2 rounded px-2 py-1 text-left text-sm transition-colors hover:bg-accent"
                >
                  <span className="min-w-0 truncate text-foreground">
                    {member.name}
                  </span>
                  <span
                    className={cn(
                      "shrink-0 font-mono font-bold tabular-nums",
                      PEER_TEXT[badStatus],
                    )}
                  >
                    {belowCount}
                  </span>
                  <span className="shrink-0 whitespace-nowrap text-muted-foreground">
                    below peers
                  </span>
                </button>
              </li>
            ))}
          </ul>
        </AlertDescription>
      </Alert>
    </section>
  );
}

import { AlertTriangle } from "lucide-react";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { useSettings } from "@/hooks/use-settings";
import { BULLET_DEFS_BY_KEY } from "@/lib/insight/v2/bullet-defs";
import {
  applyFocus,
  PEER_TEXT,
  peerStatusVsQuartiles,
  type PeerStats,
} from "@/lib/peers";
import { cn } from "@/lib/utils";
import type { BulletMetric, TeamMember } from "@/types/insight";

export interface TeamMembersAttentionProps {
  members: TeamMember[];
  bulletsByPerson?: Map<string, BulletMetric[]>;
  cohortStats?: Map<string, PeerStats>;
  cohortSize?: number;
  onMemberClick: (member: TeamMember) => void;
}

export function TeamMembersAttention({
  members,
  bulletsByPerson,
  cohortStats,
  cohortSize,
  onMemberClick,
}: TeamMembersAttentionProps) {
  const { focusMode } = useSettings();

  const attention = members
    .map((m) => {
      const bullets = bulletsByPerson?.get(m.person_id.toLowerCase()) ?? [];
      let belowCount = 0;
      for (const b of bullets) {
        const stats = cohortStats?.get(b.metric_key);
        const value = Number(b.value);
        if (!stats || !Number.isFinite(value)) continue;
        const def = BULLET_DEFS_BY_KEY[b.metric_key];
        const higherIsBetter = def?.higher_is_better ?? true;
        const ps = peerStatusVsQuartiles(value, stats, higherIsBetter);
        if (ps === "bottom") belowCount += 1;
      }
      return { member: m, belowCount };
    })
    .filter((x) => x.belowCount > 0)
    .sort((a, b) => b.belowCount - a.belowCount)
    .slice(0, 6);

  if (attention.length === 0) return null;

  const subtitle =
    cohortSize && cohortSize > 0
      ? `vs ${cohortSize} peers under the same supervisor`
      : "vs peers under the same supervisor";
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

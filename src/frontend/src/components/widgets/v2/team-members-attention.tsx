import { Link } from "@tanstack/react-router";

import { useCatalog } from "@/api/use-catalog";
import { Card, CardContent } from "@/components/ui/card";
import { useSettings } from "@/hooks/use-settings";
import { memberMetricPeerStatus } from "@/lib/insight/v2/team-member-status";
import { normalizePersonId } from "@/lib/metrics/entity";
import { applyFocus, PEER_TEXT, type DeptCohorts } from "@/lib/peers";
import { cn } from "@/lib/utils";
import type { BulletMetric, TeamMember } from "@/types/insight";

export interface TeamMembersAttentionProps {
  members: TeamMember[];
  bulletsByPerson?: Map<string, BulletMetric[]>;
  /**
   * Per-department metric distributions split by source family; bullets are
   * counted against the `bullet` family. Each member is counted "below"
   * against THEIR OWN department; a member with an absent or degenerate
   * cohort (`n < MIN_DEPT_COHORT_N`) is not counted.
   */
  deptCohorts?: DeptCohorts;
  /**
   * Additional per-member below-peer counts from metrics-backed groups
   * (`metricBelowCounts` in `lib/insight/team-metrics.ts`), keyed by
   * lowercased person id. Merged into the legacy bullet counts.
   */
  metricBelowByMember?: Map<string, number>;
}

export function TeamMembersAttention({
  members,
  bulletsByPerson,
  deptCohorts,
  metricBelowByMember,
}: TeamMembersAttentionProps) {
  const { focusMode } = useSettings();
  const { byMetricKey } = useCatalog();

  const attention = members
    .map((m) => {
      const bullets = bulletsByPerson?.get(m.person_id.toLowerCase()) ?? [];
      let belowCount =
        metricBelowByMember?.get(normalizePersonId(m.person_id)) ?? 0;
      for (const b of bullets) {
        if (
          memberMetricPeerStatus(m, b, deptCohorts, byMetricKey) === "bottom"
        ) {
          belowCount += 1;
        }
      }
      return { member: m, belowCount };
    })
    .filter((x) => x.belowCount > 0)
    .sort((a, b) => b.belowCount - a.belowCount)
    .slice(0, 6);

  if (attention.length === 0) return null;

  const subtitle =
    members.length > 0
      ? `${members.length} members · vs department peers`
      : "vs department peers";
  const badStatus = applyFocus("bottom", focusMode);

  return (
    <section>
      <h2 className="mb-3 text-xs font-medium tracking-wider text-muted-foreground uppercase">
        Members needing attention
      </h2>
      <Card data-size="sm">
        <CardContent className="flex flex-col gap-2 text-sm">
          <span className="text-[11px] text-muted-foreground">{subtitle}</span>
          <ul className="grid grid-cols-1 gap-x-8 gap-y-1 md:grid-cols-2">
            {attention.map(({ member, belowCount }) => (
              <li key={member.person_id}>
                <Link
                  to="/ic/$person/personal"
                  params={{ person: member.person_id }}
                  className="-mx-2 flex w-[calc(100%+1rem)] items-baseline gap-2 rounded px-2 py-1 text-left text-sm no-underline! transition-colors hover:bg-accent"
                >
                  <span className="min-w-0 truncate text-foreground">
                    {member.name}
                  </span>
                  <span
                    className={cn(
                      "shrink-0 font-mono font-bold tabular-nums",
                      PEER_TEXT[badStatus]
                    )}
                  >
                    {belowCount}
                  </span>
                  <span className="shrink-0 whitespace-nowrap text-muted-foreground">
                    trailing
                  </span>
                </Link>
              </li>
            ))}
          </ul>
        </CardContent>
      </Card>
    </section>
  );
}

import { AlertTriangle } from "lucide-react";

import { useCatalog } from "@/api/use-catalog";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { useSettings } from "@/hooks/use-settings";
import { bulletCatalogKey } from "@/lib/insight/v2/peer-status";
import {
  applyFocus,
  PEER_TEXT,
  peerStatusVsQuartiles,
  statsToDisplayUnit,
  type DeptCohorts,
} from "@/lib/peers";
import { cn } from "@/lib/utils";
import type { BulletMetric, TeamMember } from "@/types/insight";

// A department cohort must hold at least this many people before a member
// is counted against it. Kept in sync with the heatmap's threshold.
const MIN_DEPT_COHORT_N = 5;

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
  onMemberClick: (member: TeamMember) => void;
}

export function TeamMembersAttention({
  members,
  bulletsByPerson,
  deptCohorts,
  onMemberClick,
}: TeamMembersAttentionProps) {
  const { focusMode } = useSettings();
  const { byMetricKey } = useCatalog();

  const attention = members
    .map((m) => {
      const bullets = bulletsByPerson?.get(m.person_id.toLowerCase()) ?? [];
      const byMetric = m.org_unit_id
        ? deptCohorts?.bullet.get(m.org_unit_id)
        : undefined;
      let belowCount = 0;
      for (const b of bullets) {
        // schema_error / missing-id rows can't contribute to the "below
        // peers" count — they collapse to neutral per DESIGN §3.3.
        if (b.schema_error) continue;
        const raw = byMetric?.get(b.metric_key);
        // Degenerate department cohort (thin or absent) → not counted.
        if (!raw || raw.n < MIN_DEPT_COHORT_N) continue;
        const value = Number(b.value);
        if (!Number.isFinite(value)) continue;
        const catalogRow = byMetricKey(bulletCatalogKey(b));
        if (!catalogRow) continue;
        const stats = statsToDisplayUnit(raw, catalogRow.unit, b.unit);
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
      ? `${members.length} members · vs department peers`
      : "vs department peers";
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

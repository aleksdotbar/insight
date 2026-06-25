import { bulletCatalogKey, type CatalogByKey } from "@/lib/insight/v2/peer-status";
import {
  peerStatusVsQuartiles,
  type DeptCohorts,
  type PeerStatusWithNeutral,
} from "@/lib/peers";
import type { Status } from "@/lib/status";
import type { BulletMetric, TeamMember } from "@/types/insight";

// A department cohort must hold at least this many people before a member is
// scored against it; thinner cohorts can't support stable quartiles. Shared by
// the heatmap, the attention list, and the section-card rollup.
export const MIN_DEPT_COHORT_N = 5;

/**
 * One member's standing on one bullet metric vs their OWN department's
 * distribution — the cohort the heatmap and attention list use. Returns `null`
 * when the pair can't be scored: schema error, non-numeric value, no catalog
 * row, or a degenerate/absent department cohort (`n < MIN_DEPT_COHORT_N`).
 */
export function memberMetricPeerStatus(
  member: TeamMember,
  bullet: BulletMetric,
  deptCohorts: DeptCohorts | undefined,
  byMetricKey: CatalogByKey,
): PeerStatusWithNeutral | null {
  if (bullet.schema_error) return null;
  const raw = member.org_unit_id
    ? deptCohorts?.bullet.get(member.org_unit_id)?.get(bullet.metric_key)
    : undefined;
  if (!raw || raw.n < MIN_DEPT_COHORT_N) return null;
  const value = Number(bullet.value);
  if (!Number.isFinite(value)) return null;
  const catalogRow = byMetricKey(bulletCatalogKey(bullet));
  if (!catalogRow) return null;
  // Dept cohort and bullet value share the catalog unit (the FE never
  // rescales a metric's unit), so compare the raw cohort stats directly.
  return peerStatusVsQuartiles(value, raw, catalogRow.higher_is_better);
}

/**
 * Team section-card status per metric, rolled up from per-member standings
 * rather than the (statistically miscalibrated) team-aggregate-vs-individual-
 * band comparison. Each metric takes the PLURALITY direction across the roster:
 * more members below their own department than above → `bad`; more above →
 * `good`; a tie or an on-par majority → `warn`; none scorable → `neutral`.
 */
export function teamSectionStatusByMetric(
  rows: BulletMetric[],
  members: TeamMember[],
  bulletsByPerson: Map<string, BulletMetric[]> | undefined,
  deptCohorts: DeptCohorts | undefined,
  byMetricKey: CatalogByKey,
): Map<string, Status> {
  const out = new Map<string, Status>();
  for (const row of rows) {
    let top = 0;
    let inPack = 0;
    let bottom = 0;
    let scored = 0;
    for (const member of members) {
      const bullet = bulletsByPerson
        ?.get(member.person_id.toLowerCase())
        ?.find((b) => b.metric_key === row.metric_key);
      if (!bullet) continue;
      const ps = memberMetricPeerStatus(member, bullet, deptCohorts, byMetricKey);
      if (ps == null) continue;
      scored += 1;
      if (ps === "top") top += 1;
      else if (ps === "bottom") bottom += 1;
      else if (ps === "in_pack") inPack += 1;
    }
    const status: Status =
      scored === 0
        ? "neutral"
        : bottom > top && bottom > inPack
          ? "bad"
          : top > bottom && top > inPack
            ? "good"
            : "warn";
    out.set(row.metric_key, status);
  }
  return out;
}

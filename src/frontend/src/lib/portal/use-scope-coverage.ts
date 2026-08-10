import { useMemo } from "react";

import {
  coverageDistribution,
  personCoverage,
  reachableMetricKeys,
  thinlyCovered,
  unreachableParts,
  type CoverageDistribution,
  type PersonCoverage,
  type UnreachablePart,
} from "@/lib/insight/coverage";
import { GROUPS } from "@/lib/insight/groups";
import { projectViews } from "@/lib/metrics/collection";
import { usePortalPeriod } from "@/hooks/use-portal-period";
import { useMetricDefinitionsResponse } from "@/queries/metric-definitions";
import { useMetricCollectionSet } from "@/queries/metric-results";

export interface ScopeCoverage {
  distribution: CoverageDistribution;
  /** Per person, so a level can be opened into the people at it. */
  people: readonly PersonCoverage[];
  /** Parts nothing reaches for this tenant — see `unreachableParts`. */
  unreachable: readonly UnreachablePart[];
  /** People seen in fewer than half their parts — the screen's finding. */
  thin: number;
  isPending: boolean;
}

const CLOSED = { type: "person" as const, ids: [] as string[] };

/**
 * How much of their work the product can see, for everyone the viewer may see.
 *
 * Computed in the browser, over the viewer's visible set. Both are compromises
 * and both are stated: `distribution.counted` says how many people the answer
 * covers, and nothing here is presented as being about the organisation unless
 * the viewer's reach is the organisation.
 *
 * That compromise is affordable here and is NOT affordable for a statistic. A
 * quartile over a subset is a different quantity from the same quartile over
 * the whole group, and biased. A count over a subset stays a true statement
 * about that subset as long as the subset's size travels with it — which is
 * why `counted` is not optional and not cosmetic.
 *
 * The roster is the id list, exactly. The visibility check on the metrics
 * endpoint is all-or-nothing: one id outside the caller's visible set refuses
 * the whole request rather than filtering it, and does not say which id was at
 * fault. So the list is built from the tree the viewer was served and is never
 * widened or guessed.
 */
export function useScopeCoverage(
  memberIds: readonly string[],
): ScopeCoverage {
  const { dateRange } = usePortalPeriod();
  // The scope selector owns who is in view, so this takes the member list
  // rather than deriving one. Deriving it would leave the tab answering about
  // the viewer's whole reach while every other tab answered about the selected
  // scope, and the two would silently disagree on the same screen.
  const rosterIds = useMemo(() => [...memberIds], [memberIds]);

  // `period` only, deliberately. A collection carrying timeseries, breakdown
  // or histogram views cannot be chunked (`entityChunkSize` returns null for
  // them), and an unchunked roster-sized request runs into the backend's
  // projected-row limit. Asking for the one view this needs keeps the existing
  // chunk-and-merge path available at roster scale.
  const data = useMetricCollectionSet(
    rosterIds.length
      ? GROUPS.map((def) => ({
          key: def.id,
          collection: projectViews(def.collection, ["period"]),
        }))
      : [],
    rosterIds.length ? { type: "person" as const, ids: rosterIds } : CLOSED,
    dateRange,
  );

  // The same query key the availability gate uses, so this rides its cache
  // rather than issuing a second listing request.
  const definitions = useMetricDefinitionsResponse();
  const reachable = useMemo(
    () => reachableMetricKeys(definitions.data?.metrics ?? []),
    [definitions.data],
  );

  return useMemo(() => {
    const byKey = new Map(
      GROUPS.flatMap((def) => [...(data.get(def.id)?.byKey ?? new Map())]),
    );
    const people = rosterIds.map((id) =>
      personCoverage(GROUPS, byKey, id, reachable),
    );
    return {
      distribution: coverageDistribution(people, GROUPS.length),
      people,
      thin: thinlyCovered(people, GROUPS.length),
      unreachable: unreachableParts(GROUPS, reachable),
      isPending:
        definitions.isPending ||
        GROUPS.some((def) => data.get(def.id)?.isPending ?? true),
    };
  }, [data, rosterIds, reachable, definitions.isPending]);
}

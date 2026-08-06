import type { MetricGroup } from "@/lib/insight/groups";
import {
  forEntity,
  type NormalizedMetricResult,
} from "@/lib/metrics/collection";

/**
 * Whether a section has anything to say about this person in this period.
 *
 * The same test the card applies to decide it is empty, lifted out so the
 * screen above can apply it FIRST. A card that says "No data" is a
 * full-height box carrying one sentence; three of them took a third of a
 * person page and pushed what the person actually does below the fold.
 */
export function groupHasData(
  def: MetricGroup,
  byKey: Map<string, NormalizedMetricResult>,
  entityId: string,
): boolean {
  return def.collection.metrics.some((m) => {
    const metric = byKey.get(m.key);
    return metric != null && forEntity(metric, entityId).value != null;
  });
}

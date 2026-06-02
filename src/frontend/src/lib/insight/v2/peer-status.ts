/**
 * Peer-status helpers, catalog-driven (Refs #80).
 *
 * Wave-3 swap: `higher_is_better` for peer comparisons comes from the
 * catalog row, not a compile-in `BULLET_DEFS_BY_KEY[..]` lookup. Helpers
 * take `byMetricKey` (the `useCatalog().byMetricKey` index function) so
 * downstream callers pay one shared lookup per render instead of
 * rebuilding an index. The wire key is built from `row.section +
 * row.metric_key` via `prefixForBulletSection` to match the wire
 * `<table>.<column>` shape.
 *
 * Render rules (match wave 1's DESIGN §3.3 contract):
 *  - `row.schema_error === true` (set by `transforms.ts` when the catalog
 *    row's `schema_status='error'`) → peer status collapses to
 *    `'neutral'`. Coloring is suppressed; the label stays visible.
 *  - Catalog row absent for `row` (missing-id) → `'neutral'`. Widgets
 *    that want to hide entirely should test for the missing row directly.
 *  - `schema_status='unchecked'` is treated as `'ok'` for peer rendering;
 *    no special branch needed — only `'error'` is special-cased.
 */

import { prefixForBulletSection } from "@/api/catalog-client";
import type { CatalogMetric } from "@/api/catalog-client";
import {
  peerStatusVsQuartiles,
  type PeerStats,
  type PeerStatusWithNeutral,
} from "@/lib/peers";
import type { Status } from "@/lib/status";
import type { BulletMetric } from "@/types/insight";

/** Index function shape returned by `useCatalog().byMetricKey`. */
export type CatalogByKey = (key: string) => CatalogMetric | undefined;

/**
 * Wire `metric_key` (e.g. `task_delivery_bullet_rows.tasks_completed`)
 * for a transformed bullet row whose own `metric_key` is the bare form.
 */
export function bulletCatalogKey(row: BulletMetric): string {
  return `${prefixForBulletSection(row.section)}.${row.metric_key}`;
}

export function hasBulletValue(row: BulletMetric): boolean {
  if (row.value === "" || row.value === "—") return false;
  return Number.isFinite(Number(row.value));
}

export function peerStatusForRow(
  row: BulletMetric,
  cohortStats: Map<string, PeerStats> | undefined,
  byMetricKey: CatalogByKey,
): PeerStatusWithNeutral {
  if (row.schema_error) return "neutral";
  if (!cohortStats) return "neutral";
  const value = Number(row.value);
  if (!Number.isFinite(value)) return "neutral";
  const stats = cohortStats.get(row.metric_key);
  if (!stats) return "neutral";
  const m = byMetricKey(bulletCatalogKey(row));
  if (!m) return "neutral";
  return peerStatusVsQuartiles(value, stats, m.higher_is_better);
}

export function peerStatusToStatus(p: PeerStatusWithNeutral): Status {
  if (p === "top") return "good";
  if (p === "bottom") return "bad";
  if (p === "in_pack") return "warn";
  return "neutral";
}

export function rowStatus(
  row: BulletMetric,
  cohortStats: Map<string, PeerStats> | undefined,
  byMetricKey: CatalogByKey,
): Status {
  return peerStatusToStatus(peerStatusForRow(row, cohortStats, byMetricKey));
}

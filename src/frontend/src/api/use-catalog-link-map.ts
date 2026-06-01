/**
 * `useCatalogLinkMap()` — Layer-2 cache for the `metric_query_catalog`
 * mapping (Refs #66, ADR-003).
 *
 * The map answers "given a `metrics.query_ref` UUID, which catalog ids does
 * it emit?" Used by dashboards that issue value requests against a query
 * id and need to render each emitted value into the catalog row it belongs
 * to. The mapping is time- and filter-invariant — the same query emits the
 * same set of catalog ids regardless of `(period, person, org)` — so
 * caching it once per session/TTL avoids recomputing it per value request
 * (the failure mode the issue explicitly called out).
 *
 * Implementation: pure selector over the same `useCatalog()` query data.
 * Memoized on the catalog response identity, so N value requests sharing
 * the same catalog snapshot share one map computation. No additional
 * fetch, no second cache key — the Layer-2 contract piggybacks on the
 * Layer-1 catalog TTL.
 *
 * Fallback: when the response predates ADR-003 and omits `links`, the map
 * is empty. Consumers MUST degrade gracefully — render the value into
 * whatever catalog row their existing FE knowledge tells them to,
 * typically by `metric_key` (i.e. via `useCatalog().byMetricKey`).
 */

import { useMemo } from 'react';

import { useCatalog } from '@/api/use-catalog';

export type CatalogLinkMap = ReadonlyMap<string, readonly string[]>;

export type UseCatalogLinkMapResult = {
  /** `query_id → catalog_metric_ids`. Empty when wire response predates ADR-003. */
  linksByQuery: CatalogLinkMap;
  /** `catalog_metric_id → query_ids`. Useful for "which queries back this catalog row" debugging. */
  queriesByCatalogId: CatalogLinkMap;
};

export function useCatalogLinkMap(): UseCatalogLinkMapResult {
  const { data } = useCatalog();
  return useMemo(() => {
    const linksByQuery = new Map<string, readonly string[]>();
    const queriesByCatalogId = new Map<string, string[]>();
    const links = data.links ?? [];
    for (const link of links) {
      linksByQuery.set(link.query_id, link.catalog_metric_ids);
      for (const catalogId of link.catalog_metric_ids) {
        const existing = queriesByCatalogId.get(catalogId);
        if (existing) {
          existing.push(link.query_id);
        } else {
          queriesByCatalogId.set(catalogId, [link.query_id]);
        }
      }
    }
    return {
      linksByQuery,
      queriesByCatalogId: queriesByCatalogId as ReadonlyMap<
        string,
        readonly string[]
      >,
    };
  }, [data]);
}

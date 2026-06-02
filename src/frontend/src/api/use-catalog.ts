/**
 * `useCatalog()` — TanStack-Query-backed hydrator for the Metric Catalog
 * (Refs #66 / #82).
 *
 * Calls `POST /catalog/get_metrics` once per tenant per cache-TTL window
 * (5 min per DESIGN §3.3 Catalog Consumer Contract). The TanStack Query
 * cache dedupes parallel renders automatically — every component that calls
 * `useCatalog()` with the same args shares one in-flight request.
 *
 * ## Two-layer caching
 *
 * - **Layer 1 — catalog cache**: the response itself, governed by this hook's
 *   `staleTime` (5 min) and `queryKey` (tenant-scoped).
 * - **Layer 2 — query→catalog link map**: derived via
 *   `useCatalogLinkMap()`, which is a pure selector over the same query
 *   data; recomputed only when the catalog identity changes. See
 *   `use-catalog-link-map.ts`.
 *
 * ## Tenant isolation
 *
 * The query key includes `tenantId`. A tenant switch produces a different
 * key — the old tenant's payload stays in the cache but is no longer
 * surfaced. A defensive `removeQueries({ queryKey: ['catalog'] })` fires
 * on tenant transition in `catalog-provider.tsx`, and an in-hook
 * `data.tenant_id === tenantId` guard covers the one-render window
 * before that effect commits — a cross-tenant mismatch is treated as
 * "no data" (`data === undefined`).
 *
 * ## No fallback (post-#82)
 *
 * On API failure (network, 4xx, 5xx) `data` is `undefined` and
 * `isError` is `true`. Consumers MUST handle this: render skeletons
 * while `isLoading`, render error/empty while `isError`, and treat
 * lookups against an undefined catalog as misses (no-op render). The
 * compile-in fallback that previously synthesized a catalog from FE
 * constants was removed once the wire was confirmed parity-clean (#81).
 */

import { useCallback, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';

import {
  type CatalogMetric,
  type CatalogRequest,
  type CatalogResponse,
  fetchCatalog,
} from '@/api/catalog-client';
import { useAuth } from '@/auth/use-auth';

/** 5-minute TTL per DESIGN §3.3 Catalog Consumer Contract. */
export const CATALOG_TTL_MS = 5 * 60_000;

export type CatalogQueryKey = readonly [
  'catalog',
  string | null,
  string | undefined,
  string | undefined,
];

export type UseCatalogResult = {
  /**
   * The wire response, or `undefined` when the catalog isn't yet
   * available (initial load, fetch error, or cross-tenant mismatch).
   * Consumers MUST tolerate `undefined`.
   */
  data: CatalogResponse | undefined;
  isLoading: boolean;
  isError: boolean;
  /** Lookup by stable `id` (UUIDv7) — the wire contract. */
  byId: (id: string) => CatalogMetric | undefined;
  /**
   * Lookup by `metric_key` — convenience for the catalog-hydration
   * transitional release. Returns `undefined` when the wire response
   * predates ADR-002 and omits `metric_key`. Refactored consumers MUST
   * prefer `byId`.
   */
  byMetricKey: (metricKey: string) => CatalogMetric | undefined;
};

/**
 * Hydrate the catalog for the current tenant. `args` participate in
 * threshold resolution per DESIGN §3.3 (role / team / team+role variants);
 * omit them for the dashboard-default chain that resolves at tenant /
 * product-default only.
 */
export function useCatalog(args: CatalogRequest = {}): UseCatalogResult {
  const { tenantId } = useAuth();
  const roleSlug = args.role_slug;
  const teamId = args.team_id;

  const queryKey: CatalogQueryKey = ['catalog', tenantId, roleSlug, teamId];

  const query = useQuery<CatalogResponse>({
    queryKey,
    queryFn: () => fetchCatalog(args),
    staleTime: CATALOG_TTL_MS,
    // 30-min gc — the cached payload survives a brief away-from-app
    // gap so re-entry doesn't pay the round-trip when the data is still
    // fresh under the 5-min staleTime.
    gcTime: 30 * 60_000,
    // No retry: a 4xx is deterministic; a 5xx during hydration surfaces
    // as `isError` so consumers render their error state instead of
    // burning retry budget on a backend that's down.
    retry: 0,
  });

  // Cross-tenant defense in depth: if the cached payload's
  // `tenant_id` doesn't match the currently signed-in tenant, treat the
  // response as absent. This covers a one-render window between a
  // tenant switch and `<CatalogProvider>`'s eviction effect committing.
  const tenantMismatch =
    query.data != null && tenantId != null && query.data.tenant_id !== tenantId;

  const data = useMemo<CatalogResponse | undefined>(
    () => (query.data && !tenantMismatch ? query.data : undefined),
    [query.data, tenantMismatch],
  );

  const indexes = useMemo(() => {
    const byId = new Map<string, CatalogMetric>();
    const byKey = new Map<string, CatalogMetric>();
    if (data) {
      for (const m of data.metrics) {
        byId.set(m.id, m);
        if (m.metric_key && !byKey.has(m.metric_key)) byKey.set(m.metric_key, m);
      }
    }
    return { byId, byKey };
  }, [data]);

  // Keep the lookup closures reference-stable across renders so consumers
  // can include them in useMemo / useEffect dep arrays without
  // re-running every render. The closures key off `indexes`, which is
  // itself memoized on the response identity.
  const byId = useCallback((id: string) => indexes.byId.get(id), [indexes]);
  const byMetricKey = useCallback(
    (key: string) => indexes.byKey.get(key),
    [indexes],
  );

  return {
    data,
    isLoading: query.isLoading,
    isError: query.isError,
    byId,
    byMetricKey,
  };
}

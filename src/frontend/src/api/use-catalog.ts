/**
 * `useCatalog()` — TanStack-Query-backed hydrator for the Metric Catalog
 * (Refs #66).
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
 * before that effect commits.
 *
 * ## Fallback during transition
 *
 * On API failure (network, 4xx, 5xx) the hook returns a fallback catalog
 * derived from compile-in `BULLET_DEFS` / `IC_KPI_DEFS`. This keeps the UI
 * functional for one release window per PRD §12; the follow-on PR deletes
 * the fallback after byte-for-byte parity is confirmed.
 *
 * The fallback marks every row `schema_status = 'unchecked'` so any FE
 * code that special-cases the error state still works, and uses
 * `resolved_from = 'product-default'` since fallback values are the
 * compile-in defaults.
 */

import { useCallback, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';

import {
  type CatalogMetric,
  type CatalogRequest,
  type CatalogResponse,
  fetchCatalog,
  prefixForBulletSection,
} from '@/api/catalog-client';
import { BULLET_DEFS, IC_KPI_DEFS, VIEW_CONFIG_DEFS } from '@/api/threshold-config';
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
  data: CatalogResponse;
  isLoading: boolean;
  isError: boolean;
  isFallback: boolean;
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
 * Build a deterministic fallback catalog from the FE's compile-in constants.
 * Used when the API call fails so the UI stays functional during the
 * transitional release. Removed in the follow-on PR once parity is verified.
 *
 * `id` for fallback rows is synthesized from `metric_key` with a stable
 * prefix so they never collide with real UUIDv7 ids from the backend. A
 * lookup keyed by a real backend id will miss any fallback row, which is
 * the intended behavior — fallbacks support `byMetricKey` lookups by FE
 * call sites that still know their `metric_key`.
 */
function buildFallbackCatalog(tenantId: string | null): CatalogResponse {
  const now = new Date().toISOString();
  const bulletMetrics: CatalogMetric[] = BULLET_DEFS.map((d) => {
    const wireKey = `${prefixForBulletSection(d.section)}.${d.metric_key}`;
    return {
      // Fallback ids are `fallback:<wire-key>` so they're recognizably
      // distinct from real UUIDv7s in the QueryClient cache but byId
      // lookups against real ids deterministically miss (consumers see
      // undefined, render ComingSoon).
      id: `fallback:${wireKey}`,
      metric_key: wireKey,
      label: d.label,
      sublabel: d.sublabel,
      unit: d.unit || undefined,
      higher_is_better: d.higher_is_better,
      is_member_scale: d.isMemberScale ?? false,
      source_tags: [],
      schema_status: 'unchecked' as const,
      thresholds: {
        good: d.good,
        warn: d.warn,
        resolved_from: 'product-default' as const,
        bounded_by_lock: false,
      },
    };
  });
  const kpiMetrics: CatalogMetric[] = IC_KPI_DEFS.map((d) => {
    const wireKey = `ic_kpis.${d.metric_key}`;
    return {
      id: `fallback:${wireKey}`,
      metric_key: wireKey,
      label: d.label,
      sublabel: d.sublabel,
      description: d.description,
      unit: d.unit || undefined,
      format: d.format,
      higher_is_better: d.higher_is_better,
      is_member_scale: false,
      source_tags: [],
      schema_status: 'unchecked' as const,
      thresholds: {
        good: 0,
        warn: 0,
        resolved_from: 'product-default' as const,
        bounded_by_lock: false,
      },
    };
  });
  // Per-person / per-team policy thresholds consumed by Exec View columns,
  // Team View columns, and AttentionNeeded alerts. Wire prefix
  // `view_configs.` keeps them disjoint from bullet / IC-KPI rows so a
  // duplicate bare key (`bugs_fixed`) doesn't shadow either bucket.
  const viewConfigMetrics: CatalogMetric[] = VIEW_CONFIG_DEFS.map((d) => {
    const wireKey = `view_configs.${d.metric_key}`;
    return {
      id: `fallback:${wireKey}`,
      metric_key: wireKey,
      label: d.metric_key,
      unit: d.unit || undefined,
      higher_is_better: d.higher_is_better,
      is_member_scale: false,
      source_tags: [],
      schema_status: 'unchecked' as const,
      thresholds: {
        good: d.good,
        warn: d.warn,
        ...(d.alert
          ? {
              alert_trigger: d.alert.trigger,
              alert_bad: d.alert.bad,
              alert_reason: d.alert.reason,
            }
          : {}),
        resolved_from: 'product-default' as const,
        bounded_by_lock: false,
      },
    };
  });
  // Bullets first, KPIs second, view-config thresholds last. A duplicate
  // `metric_key` (some metrics appear in both lists — e.g. `bugs_fixed` is
  // both a code-quality bullet and an IC KPI) keeps the bullet row in
  // `byMetricKey` because `byMetricKey` walks the array and the first
  // match wins.
  return {
    tenant_id: tenantId ?? 'fallback',
    generated_at: now,
    metrics: [...bulletMetrics, ...kpiMetrics, ...viewConfigMetrics],
    links: [],
  };
}

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
    // No retry for catalog: a 4xx is deterministic and a 5xx during
    // hydration should fall back to compile-in constants immediately
    // rather than wait for retry budget to exhaust.
    retry: 0,
  });

  // Cross-tenant defense in depth: if the cached payload's
  // `tenant_id` doesn't match the currently signed-in tenant, treat the
  // response as absent and serve the fallback. This covers a one-render
  // window between a tenant switch and `<CatalogProvider>`'s eviction
  // effect committing — without this, a previously-rendered consumer
  // could paint another tenant's data once before the eviction fired.
  const tenantMismatch =
    query.data != null && tenantId != null && query.data.tenant_id !== tenantId;

  /**
   * `isFallback` is true whenever the rendered `data` is the compile-in
   * fallback rather than a wire response. Two cases:
   * - API call errored (`query.isError`).
   * - Cross-tenant mismatch in the cached payload (defensive guard).
   *
   * It is intentionally NOT true during the initial-loading window:
   * during `isLoading` the consumer also sees `data === buildFallbackCatalog(...)`
   * because the wire response hasn't arrived yet, but the contract says
   * "you might get the real catalog soon — wait or render skeletons".
   * Consumers that need "render skeletons until first paint" should gate
   * on `isLoading` directly; consumers that need "is the data
   * authoritative?" should gate on `!isFallback && !isLoading`.
   */
  const isFallback = query.isError || tenantMismatch;
  const data = useMemo<CatalogResponse>(
    () =>
      query.data && !tenantMismatch
        ? query.data
        : buildFallbackCatalog(tenantId ?? null),
    [query.data, tenantId, tenantMismatch],
  );

  const indexes = useMemo(() => {
    const byId = new Map<string, CatalogMetric>();
    const byKey = new Map<string, CatalogMetric>();
    for (const m of data.metrics) {
      byId.set(m.id, m);
      if (m.metric_key && !byKey.has(m.metric_key)) {
        // First-write-wins matches the bullet-before-KPI fallback order
        // for duplicate keys; live wire responses don't duplicate.
        byKey.set(m.metric_key, m);
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
    isFallback,
    byId,
    byMetricKey,
  };
}

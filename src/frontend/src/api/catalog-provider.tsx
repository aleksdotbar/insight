/**
 * `<CatalogProvider>` — root-level hydrator for the Metric Catalog (Refs #66).
 *
 * Why a provider exists when `useCatalog()` already works standalone:
 *
 * 1. **Prefetch.** Calling `useCatalog()` here populates the TanStack Query
 *    cache early so downstream consumers don't see a loading flash on the
 *    first dashboard render.
 * 2. **Tenant-switch invalidation.** Layer-1 + Layer-2 caches are tenant-
 *    keyed via the query key, so a tenant change naturally produces a
 *    miss. But the prior tenant's payload lingers in the QueryClient gc
 *    window — a few minutes of stale, wrong-tenant data sitting in memory.
 *    This provider proactively evicts on tenant change so cross-tenant
 *    bleed cannot happen, even in pathological "user clears cache and
 *    immediately switches" timing windows.
 * 3. **Anonymous routes.** When there's no tenant, the hook is a no-op
 *    (`fetchCatalog` would 401); the provider also no-ops by simply not
 *    calling the hook.
 */

import { useEffect, useRef } from 'react';
import { useQueryClient } from '@tanstack/react-query';

import { useCatalog } from '@/api/use-catalog';
import { useAuth } from '@/auth/use-auth';

/**
 * Prefetches and keeps the catalog warm for the current tenant. Does not
 * render any UI; consumers continue to call `useCatalog()` directly from
 * their components (TanStack Query dedupes against the same key).
 */
function CatalogPrefetch(): null {
  // Drive the hook for its cache-population side effect. Result is
  // discarded — children call `useCatalog()` themselves.
  useCatalog();
  return null;
}

export function CatalogProvider({
  children,
}: {
  children: React.ReactNode;
}): React.ReactElement {
  const { tenantId } = useAuth();
  const queryClient = useQueryClient();
  const prevTenantRef = useRef<string | null>(tenantId);

  // Evict every catalog-keyed query on tenant TRANSITION. The previous
  // implementation evicted in the effect cleanup, which fires AFTER the
  // new render commits — so children rendered for one paint cycle with
  // the prior tenant's payload before the cleanup ran. Cross-tenant data
  // MUST NOT be observable to the new tenant's session (DESIGN §3.3
  // cache-layer contract + security-review #10).
  //
  // Effect body runs after commit too, but we also guard `useCatalog`
  // with a `data.tenant_id === tenantId` check so the worst-case stale
  // observation is converted into the fallback codepath rather than a
  // wrong-tenant render. The `prevTenantRef` keeps unmount and first-
  // mount from purging gratuitously: only an actual transition between
  // two distinct non-null tenants evicts.
  useEffect(() => {
    const prev = prevTenantRef.current;
    if (prev !== tenantId && prev !== null && tenantId !== null) {
      queryClient.removeQueries({ queryKey: ['catalog'] });
    }
    prevTenantRef.current = tenantId;
  }, [tenantId, queryClient]);

  return (
    <>
      {tenantId ? <CatalogPrefetch /> : null}
      {children}
    </>
  );
}

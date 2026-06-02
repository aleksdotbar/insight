/**
 * Mock factory for `POST /catalog/get_metrics` (Refs #66, #82).
 *
 * Serves a frozen snapshot of the production wire response captured on
 * staging (see `docs/metric-catalog-parity/2026-06-02/`). The compile-in
 * metric definitions that previously sourced this factory were removed
 * in #82 once the wire was confirmed parity-clean (#81); the snapshot
 * keeps dev mode rendering against a realistic payload without
 * re-introducing compile-in metric metadata.
 *
 * Per-request fields (`tenant_id`, `generated_at`) are re-stamped so the
 * dev session looks live; metric ids and labels are taken verbatim from
 * the snapshot.
 *
 * TODO: refresh from a production capture quarterly so the dev surface
 * doesn't drift from the live catalog. Re-running `curl … /catalog/get_metrics`
 * on staging and overwriting `catalog-snapshot.json` is enough.
 */

import type {
  CatalogResponse,
  MetricQueryLink,
  CatalogMetric,
} from '@/api/catalog-client';
import snapshot from './catalog-snapshot.json';

type Snapshot = {
  tenant_id: string;
  generated_at: string;
  metrics: CatalogMetric[];
  links?: MetricQueryLink[];
};

const CATALOG_SNAPSHOT = snapshot as Snapshot;

export function buildMockCatalogResponse(tenantId: string): CatalogResponse {
  return {
    tenant_id: tenantId,
    generated_at: new Date().toISOString(),
    metrics: CATALOG_SNAPSHOT.metrics,
    links: CATALOG_SNAPSHOT.links ?? [],
  };
}

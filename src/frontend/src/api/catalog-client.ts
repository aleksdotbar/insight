/**
 * Wire types + fetch wrapper for `POST /catalog/get_metrics` (Refs #66).
 *
 * The backend response shape (DESIGN §3.3, ADR-002, ADR-003) is:
 *
 *   {
 *     tenant_id, generated_at,
 *     metrics: [{ id, metric_key, label, sublabel?, description?, unit?,
 *                 format?, higher_is_better, is_member_scale,
 *                 source_tags: string[],
 *                 schema_status, schema_error_code?,
 *                 thresholds: { good, warn, alert_trigger?, alert_bad?,
 *                               resolved_from, bounded_by_lock } }],
 *     links: [{ query_id, catalog_metric_ids: Uuid[] }]
 *   }
 *
 * `id` (UUIDv7) is the stable lookup contract — consumers MUST key off `id`.
 * `metric_key` is the FE-bridge identifier (ADR-002) used during the
 * transitional release to align compile-in `BULLET_DEFS` constants to wire
 * rows; it is parsed as optional here so a deployed environment that
 * predates ADR-002 still hydrates cleanly.
 */

import { fetchWithAuth } from '@/api/fetch-with-auth';

const BASE =
  (import.meta.env.VITE_API_BASE as string | undefined) ?? '/api/analytics/v1';

export type SchemaStatus = 'ok' | 'error' | 'unchecked';

export type SchemaErrorCode =
  | 'table_not_found'
  | 'column_not_found'
  | 'clickhouse_unreachable'
  | 'unknown';

export type ResolvedFrom =
  | 'team+role'
  | 'team'
  | 'role'
  | 'tenant'
  | 'product-default';

export type ResolvedThresholds = {
  good: number;
  warn: number;
  alert_trigger?: number;
  alert_bad?: number;
  resolved_from: ResolvedFrom;
  bounded_by_lock: boolean;
};

export type CatalogMetric = {
  /** Stable wire lookup identifier (UUIDv7). Consumers MUST key off this. */
  id: string;
  /**
   * Backend `<table>.<column>` identifier (ADR-002). Optional on the type so
   * a deployed env that predates the amendment still parses; runtime code
   * MUST tolerate `undefined` here.
   */
  metric_key?: string;
  label: string;
  sublabel?: string;
  description?: string;
  unit?: string;
  format?: string;
  higher_is_better: boolean;
  is_member_scale: boolean;
  source_tags: string[];
  schema_status: SchemaStatus;
  schema_error_code?: SchemaErrorCode;
  thresholds: ResolvedThresholds;
};

/**
 * One entry in the top-level `links` array (ADR-003). Tells a consumer which
 * `metric_catalog.id` UUIDs a `metrics.query_ref` emits. The mapping is
 * time/filter-invariant, so consumers cache it for the same TTL as the
 * catalog itself rather than recomputing it per value request.
 *
 * `links` is parsed as optional on the response type so older backends
 * (pre-ADR-003) still hydrate cleanly; consumers degrade gracefully on
 * absence (empty Layer-2 map).
 */
export type MetricQueryLink = {
  query_id: string;
  catalog_metric_ids: string[];
};

export type CatalogResponse = {
  tenant_id: string;
  generated_at: string;
  metrics: CatalogMetric[];
  links?: MetricQueryLink[];
};

export type CatalogRequest = {
  role_slug?: string;
  team_id?: string;
};

/**
 * Storage-table prefix for each bullet section so consumers can derive the
 * wire-shape `metric_key` (e.g. `task_delivery_bullet_rows.tasks_completed`)
 * from the FE-level section id. Mirrors the prefixes the backend seed
 * migration writes; if the seed list ever changes, this map MUST stay in
 * sync — the byte-for-byte parity gate (PRD §12) is the regression
 * detector.
 *
 * Sections that aren't covered (defensive) default to
 * `task_delivery_bullet_rows`, matching the backend's most-common bucket.
 */
export function prefixForBulletSection(section: string): string {
  if (section === 'git_output') return 'git_bullet_rows';
  if (section === 'code_quality') return 'code_quality_bullet_rows';
  if (section === 'ai_adoption') return 'ai_bullet_rows';
  if (section === 'collaboration') return 'collab_bullet_rows';
  return 'task_delivery_bullet_rows';
}

export class CatalogApiError extends Error {
  status: number;
  body: unknown;

  constructor(status: number, body: unknown) {
    super(`Catalog API ${status}`);
    this.name = 'CatalogApiError';
    this.status = status;
    this.body = body;
  }
}

/**
 * Fetch the catalog for the current tenant + (role, team) context.
 *
 * Sends `POST /catalog/get_metrics` with `Content-Type: application/json` —
 * the backend rejects other content types with 415 per DESIGN §3.3's CSRF
 * model. Empty body `{}` resolves at the tenant / product-default chain
 * only, which is the default for top-level hydration.
 */
export async function fetchCatalog(
  req: CatalogRequest = {},
): Promise<CatalogResponse> {
  const res = await fetchWithAuth(`${BASE}/catalog/get_metrics`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(req),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => null);
    throw new CatalogApiError(res.status, body);
  }
  try {
    return (await res.json()) as CatalogResponse;
  } catch {
    throw new CatalogApiError(res.status, { error: 'invalid_json' });
  }
}

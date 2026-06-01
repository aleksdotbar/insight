/**
 * Mock factory for `POST /catalog/get_metrics` (Refs #66).
 *
 * Bridges the existing compile-in `BULLET_DEFS` / `IC_KPI_DEFS` constants
 * into the wire shape that `fetchCatalog` consumes. This is intentional
 * during the transitional release per PRD §12 — the mock is the byte-for-byte
 * source of truth that the live backend response is compared against to
 * detect drift.
 *
 * Defaults:
 * - `schema_status = 'ok'` so dev mode renders normally (no broken-metric
 *   indicator on every dashboard tile).
 * - `resolved_from = 'product-default'` since mocked thresholds are the
 *   compile-in defaults (no tenant overlay simulated here).
 * - `links` ships the same `(query_id, catalog_metric_ids)` mapping the
 *   backend backfills in `m20260529_000001_metric_query_catalog_link.rs`
 *   so dev mode exercises the Layer-2 selector path.
 */

import { METRIC_REGISTRY } from '@/api/metric-registry';
import type {
  CatalogMetric,
  CatalogResponse,
  MetricQueryLink,
} from '@/api/catalog-client';
import { BULLET_DEFS, IC_KPI_DEFS } from '@/api/threshold-config';

/**
 * Deterministic UUIDv7-shaped synthesizer for mock catalog ids. Real backend
 * ids are UUIDv7; mock ids slot into the same `0000…` prefix as
 * `METRIC_REGISTRY` so mock and real responses are recognizably distinct in
 * dev tooling but the FE lookup-by-id contract is exercised the same way.
 */
function mockCatalogId(metricKey: string): string {
  // 32-char hex deterministic-from-key (FNV-1a 64-bit) so re-rendering
  // dev pages keeps stable ids across reloads.
  let h = 0xcbf29ce484222325n;
  for (const ch of metricKey) {
    h ^= BigInt(ch.charCodeAt(0));
    h = (h * 0x100000001b3n) & 0xffffffffffffffffn;
  }
  const hex = h.toString(16).padStart(16, '0');
  // Format roughly as 8-4-4-4-12; reuse the hash bytes for all positions.
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-7${hex.slice(13, 16)}-a${hex.slice(1, 4)}-${hex.slice(0, 12)}`;
}

function bulletKeyToTable(metricKey: string): string {
  // Mirrors the QUERY_TO_CATALOG_PREFIX mapping in the backend backfill
  // migration. Used to bucket each mock catalog row into the right
  // storage-table prefix for the link map.
  if (metricKey.startsWith('slack_') || metricKey.startsWith('m365_')) {
    return 'collab_bullet_rows';
  }
  if (metricKey.includes('meeting') || metricKey.includes('teams_') || metricKey.includes('zoom_')) {
    return 'collab_bullet_rows';
  }
  if (metricKey.includes('build_success') || metricKey.includes('pr_cycle_time')) {
    return 'code_quality_bullet_rows';
  }
  if (metricKey.startsWith('cursor_') || metricKey.startsWith('cc_') || metricKey.startsWith('codex_') ||
      metricKey.includes('ai_') || metricKey.includes('chatgpt') || metricKey.includes('claude_web')) {
    return 'ai_bullet_rows';
  }
  if (metricKey === 'commits' || metricKey.startsWith('pr') || metricKey === 'clean_loc' ||
      metricKey === 'merge_rate' || metricKey === 'lines_per_commit' ||
      metricKey === 'commits_per_active_day') {
    return 'git_bullet_rows';
  }
  return 'task_delivery_bullet_rows';
}

function buildBulletMetric(d: (typeof BULLET_DEFS)[number]): CatalogMetric {
  return {
    id: mockCatalogId(`${bulletKeyToTable(d.metric_key)}.${d.metric_key}`),
    metric_key: `${bulletKeyToTable(d.metric_key)}.${d.metric_key}`,
    label: d.label,
    sublabel: d.sublabel,
    unit: d.unit || undefined,
    higher_is_better: d.higher_is_better,
    is_member_scale: d.isMemberScale ?? false,
    source_tags: [],
    schema_status: 'ok',
    thresholds: {
      good: d.good,
      warn: d.warn,
      resolved_from: 'product-default',
      bounded_by_lock: false,
    },
  };
}

function buildKpiMetric(d: (typeof IC_KPI_DEFS)[number]): CatalogMetric {
  return {
    id: mockCatalogId(`ic_kpis.${d.metric_key}`),
    metric_key: `ic_kpis.${d.metric_key}`,
    label: d.label,
    sublabel: d.sublabel,
    description: d.description,
    unit: d.unit || undefined,
    format: d.format,
    higher_is_better: d.higher_is_better,
    is_member_scale: false,
    source_tags: [],
    schema_status: 'ok',
    thresholds: {
      good: 0,
      warn: 0,
      resolved_from: 'product-default',
      bounded_by_lock: false,
    },
  };
}

/**
 * Build the link map mirroring the backend's
 * `m20260529_000001_metric_query_catalog_link` backfill so dev mode
 * exercises the Layer-2 selector against realistic data.
 */
function buildLinks(metrics: readonly CatalogMetric[]): MetricQueryLink[] {
  const idsByPrefix: Record<string, string[]> = {};
  for (const m of metrics) {
    const key = m.metric_key ?? '';
    const dotIx = key.indexOf('.');
    if (dotIx < 0) continue;
    const prefix = key.slice(0, dotIx);
    (idsByPrefix[prefix] ??= []).push(m.id);
  }
  // Same nine-entry map as the backend migration. Empty buckets are
  // silently skipped so the dev payload stays consistent when an
  // upstream constant evolves.
  const pairs: ReadonlyArray<readonly [string, string]> = [
    [METRIC_REGISTRY.TEAM_BULLET_DELIVERY, 'task_delivery_bullet_rows'],
    [METRIC_REGISTRY.TEAM_BULLET_QUALITY, 'code_quality_bullet_rows'],
    [METRIC_REGISTRY.TEAM_BULLET_COLLAB, 'collab_bullet_rows'],
    [METRIC_REGISTRY.TEAM_BULLET_AI, 'ai_bullet_rows'],
    [METRIC_REGISTRY.IC_KPIS, 'ic_kpis'],
    [METRIC_REGISTRY.IC_BULLET_DELIVERY, 'task_delivery_bullet_rows'],
    [METRIC_REGISTRY.IC_BULLET_COLLAB, 'collab_bullet_rows'],
    [METRIC_REGISTRY.IC_BULLET_AI, 'ai_bullet_rows'],
    [METRIC_REGISTRY.IC_BULLET_GIT, 'git_bullet_rows'],
  ];
  const out: MetricQueryLink[] = [];
  for (const [queryId, prefix] of pairs) {
    const ids = idsByPrefix[prefix];
    if (!ids || ids.length === 0) continue;
    // Ascending sort for byte-stable wire output (mirrors the backend
    // ORDER BY in `fetch_links`).
    out.push({
      query_id: queryId,
      catalog_metric_ids: [...ids].sort(),
    });
  }
  return out;
}

export function buildMockCatalogResponse(tenantId: string): CatalogResponse {
  // Bullets first so duplicate `metric_key`s prefer the bullet semantics
  // (matches the fallback in `use-catalog.ts::buildFallbackCatalog`).
  const metrics: CatalogMetric[] = [
    ...BULLET_DEFS.map(buildBulletMetric),
    ...IC_KPI_DEFS.map(buildKpiMetric),
  ];
  return {
    tenant_id: tenantId,
    generated_at: new Date('2026-06-01T00:00:00Z').toISOString(),
    metrics,
    links: buildLinks(metrics),
  };
}

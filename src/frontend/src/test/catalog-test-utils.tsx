/**
 * Shared component-test scaffolding for wave-3 catalog-driven widgets
 * (Refs #80).
 *
 * Each v2 widget reads from `useCatalog()`; tests stub `fetchCatalog`
 * directly (same pattern `view-configs.test.tsx` and
 * `use-catalog.test.tsx` adopt) so assertions exercise the
 * catalog→widget wiring without MSW. Helpers here keep the
 * boilerplate-per-test small while keeping each test file
 * self-documenting on the specific render rule it covers.
 *
 * `vi.mock('@/api/catalog-client', ...)` must be declared in each test
 * file before this module is imported — `vi.mock` is hoisted relative
 * to the file that owns it, not transitively to helpers.
 */

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render } from "@testing-library/react";
import type React from "react";

import { authStore } from "@/auth/auth-store";
import type {
  CatalogMetric,
  CatalogResponse,
} from "@/api/catalog-client";

export function buildCatalogResponse(
  rows: Array<Partial<CatalogMetric>>,
): CatalogResponse {
  return {
    tenant_id: authStore.getSnapshot().tenantId ?? "t-1",
    generated_at: "2026-06-01T00:00:00Z",
    metrics: rows.map((r, i) => ({
      id: r.id ?? `id-${i}`,
      metric_key: r.metric_key ?? `mock.metric_${i}`,
      label: r.label ?? `Metric ${i}`,
      higher_is_better: r.higher_is_better ?? true,
      is_member_scale: r.is_member_scale ?? false,
      source_tags: r.source_tags ?? [],
      schema_status: r.schema_status ?? "ok",
      schema_error_code: r.schema_error_code,
      thresholds: r.thresholds ?? {
        good: 1,
        warn: 0,
        resolved_from: "product-default",
        bounded_by_lock: false,
      },
      unit: r.unit,
      sublabel: r.sublabel,
      description: r.description,
      format: r.format,
    })),
    links: [],
  };
}

export function renderWithCatalogClient(ui: React.ReactElement): {
  client: QueryClient;
} {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  });
  render(<QueryClientProvider client={client}>{ui}</QueryClientProvider>);
  return { client };
}

/**
 * Hook tests for `useExecViewConfig`, `useTeamViewConfig`, and
 * `useTeamKpisByPeriod` (Refs #79).
 *
 * Stubs `fetchCatalog` directly (vi.mock) — the same pattern
 * `use-catalog.test.tsx` uses — so the assertions exercise the catalog →
 * view-config wiring without leaning on MSW. Each test mounts a fresh
 * QueryClient.
 *
 * Coverage:
 *  - parity with the pre-#79 shape of `EXEC_VIEW_CONFIG.column_thresholds`
 *    and `TEAM_VIEW_CONFIG.{alert_thresholds, column_thresholds}` when the
 *    catalog returns matching `view_configs.*` rows.
 *  - `schema_status='error'` rows are omitted — downstream renderers
 *    (`OrgKpiCards`, `MembersTable`, `AttentionNeeded`) then fall through
 *    to their neutral-coloring path.
 *  - alert_thresholds are skipped when the catalog doesn't carry
 *    `alert_trigger` / `alert_bad` (e.g. an exec-only metric).
 */

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type React from "react";

import { authStore } from "@/auth/auth-store";

vi.mock("./catalog-client", async () => {
  const actual = await vi.importActual<typeof import("./catalog-client")>(
    "./catalog-client",
  );
  return { ...actual, fetchCatalog: vi.fn() };
});

import * as catalogClient from "./catalog-client";
import { useCatalog } from "./use-catalog";
import {
  useExecViewConfig,
  useTeamKpisByPeriod,
  useTeamViewConfig,
} from "./view-configs";

const fetchCatalog = catalogClient.fetchCatalog as ReturnType<typeof vi.fn>;

type Row = Partial<catalogClient.CatalogMetric> & { metric_key: string };

function buildResponse(rows: Row[]): catalogClient.CatalogResponse {
  return {
    tenant_id: authStore.getSnapshot().tenantId ?? "t-1",
    generated_at: "2026-06-01T00:00:00Z",
    metrics: rows.map((r, i) => ({
      id: r.id ?? `id-${i}`,
      metric_key: r.metric_key,
      label: r.label ?? r.metric_key,
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

function withClient(): ({ children }: { children: React.ReactNode }) => React.ReactElement {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  });
  return ({ children }) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  );
}

/**
 * Render a view-config hook AND observe the underlying catalog query so the
 * test can wait for the mocked `fetchCatalog` response to land before
 * asserting. Without this, the first synchronous render sees the
 * compile-in fallback rows (whose values intentionally mirror the catalog
 * defaults) — `waitFor(() => expect(value).toBeDefined())` passes
 * immediately against the fallback and tests that rely on schema_status
 * variations silently read pre-fetch state.
 */
async function renderWithFetched<T>(
  factory: () => T,
): Promise<{ current: T }> {
  const wrapper = withClient();
  const { result } = renderHook(
    () => ({ view: factory(), catalog: useCatalog() }),
    { wrapper },
  );
  await waitFor(() => expect(result.current.catalog.isLoading).toBe(false));
  return { get current() { return result.current.view; } };
}

describe("useExecViewConfig", () => {
  beforeEach(() => {
    authStore.reset();
    authStore.setTenantId("t-1");
    fetchCatalog.mockReset();
  });
  afterEach(() => {
    authStore.reset();
  });

  it("builds column_thresholds in the same shape EXEC_VIEW_CONFIG used to expose", async () => {
    fetchCatalog.mockResolvedValue(
      buildResponse([
        {
          metric_key: "view_configs.build_success_pct",
          higher_is_better: true,
          thresholds: {
            good: 90,
            warn: 80,
            resolved_from: "product-default",
            bounded_by_lock: false,
          },
        },
        {
          metric_key: "view_configs.focus_time_pct",
          higher_is_better: true,
          thresholds: {
            good: 60,
            warn: 50,
            resolved_from: "product-default",
            bounded_by_lock: false,
          },
        },
        {
          metric_key: "view_configs.ai_adoption_pct",
          higher_is_better: true,
          thresholds: {
            good: 60,
            warn: 40,
            resolved_from: "product-default",
            bounded_by_lock: false,
          },
        },
      ]),
    );
    const result = await renderWithFetched(() => useExecViewConfig());
    expect(result.current.column_thresholds).toEqual([
      { metric_key: "build_success_pct", threshold: 90 },
      { metric_key: "focus_time_pct", threshold: 60 },
      { metric_key: "ai_adoption_pct", threshold: 60 },
    ]);
  });

  it("omits schema_status='error' columns so consumers render neutral", async () => {
    fetchCatalog.mockResolvedValue(
      buildResponse([
        {
          metric_key: "view_configs.build_success_pct",
          schema_status: "error",
          schema_error_code: "table_not_found",
          thresholds: {
            good: 90,
            warn: 80,
            resolved_from: "product-default",
            bounded_by_lock: false,
          },
        },
        {
          metric_key: "view_configs.focus_time_pct",
          higher_is_better: true,
          thresholds: {
            good: 60,
            warn: 50,
            resolved_from: "product-default",
            bounded_by_lock: false,
          },
        },
        {
          metric_key: "view_configs.ai_adoption_pct",
          higher_is_better: true,
          thresholds: {
            good: 60,
            warn: 40,
            resolved_from: "product-default",
            bounded_by_lock: false,
          },
        },
      ]),
    );
    const result = await renderWithFetched(() => useExecViewConfig());
    // The build_success column is omitted entirely — `OrgKpiCards` /
    // `TeamsTable` look up by `metric_key` and fall through to neutral
    // coloring when the rule is absent.
    expect(
      result.current.column_thresholds.find(
        (t) => t.metric_key === "build_success_pct",
      ),
    ).toBeUndefined();
    expect(
      result.current.column_thresholds.some(
        (t) => t.metric_key === "focus_time_pct",
      ),
    ).toBe(true);
  });

  it("falls back to compile-in defaults when the catalog API fails", async () => {
    fetchCatalog.mockRejectedValue(
      new catalogClient.CatalogApiError(500, { type: "internal" }),
    );
    // Compile-in defaults (from VIEW_CONFIG_DEFS) are emitted as
    // schema_status='unchecked' fallback rows — useExecViewConfig still
    // produces the 3-column threshold list. Wait for `isError` so the
    // fallback path (not just initial render) is what we're asserting.
    const wrapper = withClient();
    const { result } = renderHook(
      () => ({ view: useExecViewConfig(), catalog: useCatalog() }),
      { wrapper },
    );
    await waitFor(() => expect(result.current.catalog.isError).toBe(true));
    expect(
      result.current.view.column_thresholds.map((t) => t.metric_key),
    ).toEqual(["build_success_pct", "focus_time_pct", "ai_adoption_pct"]);
  });
});

describe("useTeamViewConfig", () => {
  beforeEach(() => {
    authStore.reset();
    authStore.setTenantId("t-1");
    fetchCatalog.mockReset();
  });
  afterEach(() => {
    authStore.reset();
  });

  it("threads alert_trigger / alert_bad / alert_reason through from the catalog", async () => {
    fetchCatalog.mockResolvedValue(
      buildResponse([
        {
          metric_key: "view_configs.build_success_pct",
          higher_is_better: true,
          thresholds: {
            good: 90,
            warn: 80,
            alert_trigger: 90,
            alert_bad: 80,
            alert_reason: "Build success rate below 90% target",
            resolved_from: "product-default",
            bounded_by_lock: false,
          },
        },
        {
          metric_key: "view_configs.focus_time_pct",
          higher_is_better: true,
          thresholds: {
            good: 60,
            warn: 50,
            alert_trigger: 60,
            alert_bad: 48,
            alert_reason: "Focus time below 60% target",
            resolved_from: "product-default",
            bounded_by_lock: false,
          },
        },
        {
          metric_key: "view_configs.ai_loc_share_pct",
          higher_is_better: true,
          thresholds: {
            good: 20,
            warn: 10,
            alert_trigger: 10,
            alert_bad: 8,
            alert_reason: "Low AI tool adoption",
            resolved_from: "product-default",
            bounded_by_lock: false,
          },
        },
      ]),
    );
    const result = await renderWithFetched(() => useTeamViewConfig());
    expect(result.current.alert_thresholds).toEqual([
      {
        metric_key: "build_success_pct",
        trigger: 90,
        bad: 80,
        reason: "Build success rate below 90% target",
      },
      {
        metric_key: "focus_time_pct",
        trigger: 60,
        bad: 48,
        reason: "Focus time below 60% target",
      },
      {
        metric_key: "ai_loc_share_pct",
        trigger: 10,
        bad: 8,
        reason: "Low AI tool adoption",
      },
    ]);
  });

  it("skips alert rows when the catalog omits alert_trigger / alert_bad", async () => {
    fetchCatalog.mockResolvedValue(
      buildResponse([
        {
          metric_key: "view_configs.build_success_pct",
          higher_is_better: true,
          thresholds: {
            good: 90,
            warn: 80,
            resolved_from: "product-default",
            bounded_by_lock: false,
          },
        },
      ]),
    );
    const result = await renderWithFetched(() => useTeamViewConfig());
    // No `alert_trigger` on the wire → no alert rule emitted, even though
    // the metric IS in the alert-key list.
    expect(result.current.alert_thresholds).toEqual([]);
  });

  it("builds column_thresholds from higher_is_better + good + warn", async () => {
    fetchCatalog.mockResolvedValue(
      buildResponse([
        {
          metric_key: "view_configs.bugs_fixed",
          higher_is_better: true,
          thresholds: {
            good: 15,
            warn: 8,
            resolved_from: "product-default",
            bounded_by_lock: false,
          },
        },
        {
          metric_key: "view_configs.dev_time_h",
          higher_is_better: false,
          thresholds: {
            good: 14,
            warn: 20,
            resolved_from: "product-default",
            bounded_by_lock: false,
          },
        },
      ]),
    );
    const result = await renderWithFetched(() => useTeamViewConfig());
    expect(
      result.current.column_thresholds.find((t) => t.metric_key === "bugs_fixed"),
    ).toEqual({
      metric_key: "bugs_fixed",
      good: 15,
      warn: 8,
      higher_is_better: true,
    });
    expect(
      result.current.column_thresholds.find((t) => t.metric_key === "dev_time_h"),
    ).toEqual({
      metric_key: "dev_time_h",
      good: 14,
      warn: 20,
      higher_is_better: false,
    });
  });

  it("omits schema_status='error' columns AND alert rules", async () => {
    fetchCatalog.mockResolvedValue(
      buildResponse([
        {
          metric_key: "view_configs.build_success_pct",
          schema_status: "error",
          higher_is_better: true,
          thresholds: {
            good: 90,
            warn: 80,
            alert_trigger: 90,
            alert_bad: 80,
            alert_reason: "Build success rate below 90% target",
            resolved_from: "product-default",
            bounded_by_lock: false,
          },
        },
      ]),
    );
    const result = await renderWithFetched(() => useTeamViewConfig());
    expect(
      result.current.column_thresholds.find(
        (t) => t.metric_key === "build_success_pct",
      ),
    ).toBeUndefined();
    expect(
      result.current.alert_thresholds.find(
        (t) => t.metric_key === "build_success_pct",
      ),
    ).toBeUndefined();
  });
});

describe("useTeamKpisByPeriod", () => {
  it("returns the structural KPI templates for each period", () => {
    const wrapper = withClient();
    const { result } = renderHook(() => useTeamKpisByPeriod("month"), {
      wrapper,
    });
    // Templates don't depend on the catalog — synchronous read is fine.
    const keys = result.current.map((t) => t.metric_key);
    // Templates are static metadata — value/status/sublabel are filled in
    // by useTeamKpis from real members. Keys MUST stay stable; downstream
    // TeamHeroStrip switches by key.
    expect(keys).toEqual([
      "at_risk_count",
      "team_dev_time",
      "focus_gte_60",
      "not_using_ai",
    ]);
  });
});

/**
 * Catalog-driven transform tests (Refs #78).
 *
 * Pins the wave-1 contract: `transformBulletMetrics` / `transformIcKpis`
 * consume the hydrated Metric Catalog instead of compile-in
 * `BULLET_DEFS` / `IC_KPI_DEFS`. See DESIGN §3.3 "Catalog Consumer
 * Contract":
 *  - `schema_status='error'` → suppress threshold-based coloring
 *    (`status='unavailable'`) and flag the row with `schema_error: true`.
 *  - `schema_status='unchecked'` → render identically to `ok`.
 *  - Missing-id (catalog row absent) → silently omit.
 *  - Honest-zero rows use the catalog's label, not compile-in.
 */

import { describe, expect, it } from "vitest";

import type { CatalogMetric, CatalogResponse } from "./catalog-client";
import type { RawBulletAggregateRow, RawIcAggregateRow } from "./raw-types";
import { transformBulletMetrics, transformIcKpis } from "./transforms";

const TENANT = "t-test";

function bulletCatalogRow(
  bareKey: string,
  overrides: Partial<CatalogMetric> = {},
): CatalogMetric {
  // Default to a task_delivery_bullet_rows wire-prefix; tests that need
  // other prefixes override `metric_key` explicitly.
  return {
    id: `id-${bareKey}`,
    metric_key: `task_delivery_bullet_rows.${bareKey}`,
    label: `Label ${bareKey}`,
    sublabel: `Sublabel ${bareKey}`,
    higher_is_better: true,
    is_member_scale: false,
    source_tags: [],
    schema_status: "ok",
    thresholds: {
      good: 5,
      warn: 3,
      resolved_from: "product-default",
      bounded_by_lock: false,
    },
    ...overrides,
  };
}

function catalogWith(metrics: CatalogMetric[]): CatalogResponse {
  return {
    tenant_id: TENANT,
    generated_at: "2026-06-01T00:00:00Z",
    metrics,
    links: [],
  };
}

function rawBullet(
  bareKey: string,
  overrides: Partial<RawBulletAggregateRow> = {},
): RawBulletAggregateRow {
  return {
    metric_key: bareKey,
    value: 7,
    median: 5,
    range_min: 0,
    range_max: 10,
    ...overrides,
  };
}

describe("transformBulletMetrics", () => {
  it("renders schema_status='ok' rows with threshold-based status (parity)", () => {
    const catalog = catalogWith([bulletCatalogRow("tasks_completed")]);
    const out = transformBulletMetrics(
      [rawBullet("tasks_completed", { value: 7 })],
      "task_delivery",
      "week",
      undefined,
      "ic",
      catalog,
    );
    expect(out).toHaveLength(1);
    expect(out[0]!.status).toBe("good");
    expect(out[0]!.schema_error).toBeUndefined();
    expect(out[0]!.label).toBe("Label tasks_completed");
  });

  it("renders schema_status='unchecked' identically to 'ok' (no schema_error flag)", () => {
    const catalog = catalogWith([
      bulletCatalogRow("tasks_completed", { schema_status: "unchecked" }),
    ]);
    const out = transformBulletMetrics(
      [rawBullet("tasks_completed", { value: 7 })],
      "task_delivery",
      "week",
      undefined,
      "ic",
      catalog,
    );
    expect(out).toHaveLength(1);
    // Threshold-based status still computed (same as 'ok' path).
    expect(out[0]!.status).toBe("good");
    expect(out[0]!.schema_error).toBeUndefined();
  });

  it("flags schema_status='error' rows with schema_error:true and suppresses threshold coloring", () => {
    const catalog = catalogWith([
      bulletCatalogRow("tasks_completed", { schema_status: "error" }),
    ]);
    const out = transformBulletMetrics(
      [rawBullet("tasks_completed", { value: 7 })],
      "task_delivery",
      "week",
      undefined,
      "ic",
      catalog,
    );
    expect(out).toHaveLength(1);
    expect(out[0]!.schema_error).toBe(true);
    // Threshold coloring suppressed → status falls back to 'unavailable'.
    expect(out[0]!.status).toBe("unavailable");
    // Label still visible — consumers render it next to the broken indicator.
    expect(out[0]!.label).toBe("Label tasks_completed");
  });

  it("silently omits raw rows whose metric_key isn't in the catalog (missing-id)", () => {
    // Catalog knows only tasks_completed; backend returns an extra
    // `ghost_metric` row (e.g. catalog row was deleted between hydration
    // and this fetch).
    const catalog = catalogWith([bulletCatalogRow("tasks_completed")]);
    const out = transformBulletMetrics(
      [
        rawBullet("tasks_completed", { value: 7 }),
        rawBullet("ghost_metric", { value: 99 }),
      ],
      "task_delivery",
      "week",
      undefined,
      "ic",
      catalog,
    );
    expect(out).toHaveLength(1);
    expect(out[0]!.metric_key).toBe("tasks_completed");
  });

  it("synthesizes honest-zero rows using the catalog's label, not compile-in", () => {
    // Override the label so we can confirm the transform doesn't fall back
    // to BULLET_DEFS for the label.
    const catalog = catalogWith([
      bulletCatalogRow("tasks_completed", { label: "Catalog Override Label" }),
    ]);
    // Backend returns no rows → transform must synthesize one for the
    // catalog-known metric_key.
    const out = transformBulletMetrics(
      [],
      "task_delivery",
      "week",
      undefined,
      "ic",
      catalog,
    );
    expect(out).toHaveLength(1);
    expect(out[0]!.metric_key).toBe("tasks_completed");
    expect(out[0]!.label).toBe("Catalog Override Label");
    // Honest-zero rows have no distribution → 'unavailable'.
    expect(out[0]!.status).toBe("unavailable");
  });

  it("filters catalog rows by section prefix (mismatched prefix is omitted)", () => {
    // git_output → wire prefix `git_bullet_rows`. A catalog row with a
    // task_delivery_bullet_rows prefix must NOT be picked up.
    const catalog = catalogWith([
      bulletCatalogRow("tasks_completed"), // task_delivery prefix
      bulletCatalogRow("commits", {
        metric_key: "git_bullet_rows.commits",
        label: "Commits Authored",
      }),
    ]);
    const out = transformBulletMetrics(
      [rawBullet("commits", { value: 30 })],
      "git_output",
      "week",
      undefined,
      "ic",
      catalog,
    );
    expect(out).toHaveLength(1);
    expect(out[0]!.metric_key).toBe("commits");
    expect(out[0]!.label).toBe("Commits Authored");
  });
});

function icKpiRow(
  bareKey: string,
  overrides: Partial<CatalogMetric> = {},
): CatalogMetric {
  return {
    id: `id-kpi-${bareKey}`,
    metric_key: `ic_kpis.${bareKey}`,
    label: `KPI ${bareKey}`,
    sublabel: "src",
    description: "desc",
    higher_is_better: true,
    is_member_scale: false,
    source_tags: [],
    schema_status: "ok",
    format: "integer",
    thresholds: {
      good: 0,
      warn: 0,
      resolved_from: "product-default",
      bounded_by_lock: false,
    },
    ...overrides,
  };
}

function rawIcAggregate(
  overrides: Partial<RawIcAggregateRow> = {},
): RawIcAggregateRow {
  return {
    person_id: "p",
    loc: null,
    ai_loc_share_pct: 0,
    prs_merged: null,
    pr_cycle_time_h: null,
    focus_time_pct: 0,
    tasks_closed: 0,
    bugs_fixed: 0,
    build_success_pct: null,
    ai_sessions: 0,
    ...overrides,
  };
}

describe("transformIcKpis", () => {
  it("emits one IcKpi per catalog ic_kpis row using catalog label / sublabel", () => {
    const catalog = catalogWith([
      icKpiRow("tasks_closed", { label: "Catalog Tasks Closed" }),
      icKpiRow("bugs_fixed", { label: "Catalog Bugs Fixed" }),
    ]);
    const out = transformIcKpis(
      rawIcAggregate({ tasks_closed: 8, bugs_fixed: 2 }),
      null,
      "week",
      catalog,
    );
    const byKey = new Map(out.map((r) => [r.metric_key, r]));
    expect(byKey.get("tasks_closed")?.label).toBe("Catalog Tasks Closed");
    expect(byKey.get("tasks_closed")?.raw_value).toBe(8);
    expect(byKey.get("bugs_fixed")?.label).toBe("Catalog Bugs Fixed");
    expect(byKey.get("bugs_fixed")?.raw_value).toBe(2);
  });

  it("omits catalog ic_kpis rows whose bare key has no FE raw_field mapping", () => {
    // `unknown_metric` is not in IC_KPI_DEFS → transform can't source its
    // raw value and silently omits.
    const catalog = catalogWith([
      icKpiRow("tasks_closed"),
      icKpiRow("unknown_metric"),
    ]);
    const out = transformIcKpis(
      rawIcAggregate({ tasks_closed: 8 }),
      null,
      "week",
      catalog,
    );
    expect(out.map((r) => r.metric_key)).toEqual(["tasks_closed"]);
  });

  it("returns [] when the current raw aggregate row is null", () => {
    const catalog = catalogWith([icKpiRow("tasks_closed")]);
    expect(transformIcKpis(null, null, "week", catalog)).toEqual([]);
  });
});

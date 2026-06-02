/**
 * Pure-function tests for `peerStatusForRow` (Refs #80).
 *
 * Covers the wave-3 catalog-driven peer status helper across both
 * `higher_is_better=true` and `higher_is_better=false` axes, plus the
 * three rendering rules the wave-1 DESIGN §3.3 contract introduced:
 *   - `schema_status='error'` (surfaced as `row.schema_error: true` by
 *     transforms.ts) collapses to 'neutral'.
 *   - Missing-id (catalog row absent for the row's metric_key)
 *     collapses to 'neutral'.
 *   - `unchecked` schema_status renders the same as 'ok' (only 'error'
 *     is special-cased).
 */

import { describe, expect, it } from "vitest";

import type { CatalogMetric } from "@/api/catalog-client";
import type { PeerStats } from "@/lib/peers";
import type { BulletMetric, PeriodValue } from "@/types/insight";

import {
  bulletCatalogKey,
  hasBulletValue,
  peerStatusForRow,
  rowStatus,
  type CatalogByKey,
} from "./peer-status";

function makeRow(overrides: Partial<BulletMetric> = {}): BulletMetric {
  return {
    period: "month" as PeriodValue,
    section: "task_delivery",
    metric_key: "tasks_completed",
    label: "Tasks Closed",
    value: "8",
    unit: "tasks",
    range_min: "0",
    range_max: "20",
    median: "5",
    median_label: "Median: 5 tasks",
    bar_left_pct: 0,
    bar_width_pct: 40,
    median_left_pct: 25,
    status: "good",
    drill_id: "",
    ...overrides,
  };
}

function makeCatalogMetric(
  overrides: Partial<CatalogMetric> = {},
): CatalogMetric {
  return {
    id: overrides.id ?? "id-1",
    metric_key:
      overrides.metric_key ?? "task_delivery_bullet_rows.tasks_completed",
    label: overrides.label ?? "Tasks Closed",
    higher_is_better: overrides.higher_is_better ?? true,
    is_member_scale: overrides.is_member_scale ?? false,
    source_tags: overrides.source_tags ?? [],
    schema_status: overrides.schema_status ?? "ok",
    schema_error_code: overrides.schema_error_code,
    thresholds: overrides.thresholds ?? {
      good: 5,
      warn: 3,
      resolved_from: "product-default",
      bounded_by_lock: false,
    },
    unit: overrides.unit,
    sublabel: overrides.sublabel,
    description: overrides.description,
    format: overrides.format,
  };
}

function makeByMetricKey(
  metrics: ReadonlyArray<CatalogMetric>,
): CatalogByKey {
  const map = new Map<string, CatalogMetric>();
  for (const m of metrics) if (m.metric_key) map.set(m.metric_key, m);
  return (key) => map.get(key);
}

const STATS: PeerStats = { p25: 3, p50: 5, p75: 10, min: 1, max: 15, n: 12 };

describe("bulletCatalogKey", () => {
  it("composes <wire-prefix>.<bare> from section + metric_key", () => {
    expect(
      bulletCatalogKey(makeRow({ section: "task_delivery", metric_key: "x" })),
    ).toBe("task_delivery_bullet_rows.x");
    expect(
      bulletCatalogKey(makeRow({ section: "git_output", metric_key: "y" })),
    ).toBe("git_bullet_rows.y");
    expect(
      bulletCatalogKey(makeRow({ section: "ai_adoption", metric_key: "z" })),
    ).toBe("ai_bullet_rows.z");
  });
});

describe("hasBulletValue", () => {
  it("returns false for em-dash and empty placeholder strings", () => {
    expect(hasBulletValue(makeRow({ value: "—" }))).toBe(false);
    expect(hasBulletValue(makeRow({ value: "" }))).toBe(false);
  });

  it("returns true for finite numeric strings, false for non-numeric", () => {
    expect(hasBulletValue(makeRow({ value: "0" }))).toBe(true);
    expect(hasBulletValue(makeRow({ value: "12" }))).toBe(true);
    expect(hasBulletValue(makeRow({ value: "n/a" }))).toBe(false);
  });
});

describe("peerStatusForRow — higher_is_better=true", () => {
  const byKey = makeByMetricKey([
    makeCatalogMetric({ higher_is_better: true }),
  ]);
  const stats = new Map([["tasks_completed", STATS]]);

  it("top when value >= p75", () => {
    expect(peerStatusForRow(makeRow({ value: "12" }), stats, byKey)).toBe("top");
  });
  it("bottom when value <= p25", () => {
    expect(peerStatusForRow(makeRow({ value: "2" }), stats, byKey)).toBe(
      "bottom",
    );
  });
  it("in_pack between p25 and p75", () => {
    expect(peerStatusForRow(makeRow({ value: "6" }), stats, byKey)).toBe(
      "in_pack",
    );
  });
});

describe("peerStatusForRow — higher_is_better=false", () => {
  const byKey = makeByMetricKey([
    makeCatalogMetric({ higher_is_better: false }),
  ]);
  const stats = new Map([["tasks_completed", STATS]]);

  it("flips: low value is 'top', high value is 'bottom'", () => {
    expect(peerStatusForRow(makeRow({ value: "2" }), stats, byKey)).toBe("top");
    expect(peerStatusForRow(makeRow({ value: "12" }), stats, byKey)).toBe(
      "bottom",
    );
    expect(peerStatusForRow(makeRow({ value: "6" }), stats, byKey)).toBe(
      "in_pack",
    );
  });
});

describe("peerStatusForRow — rendering rules", () => {
  const okMetric = makeCatalogMetric({ higher_is_better: true });
  const byKey = makeByMetricKey([okMetric]);
  const stats = new Map([["tasks_completed", STATS]]);

  it("schema_error rows collapse to neutral even when value is clearly 'top'", () => {
    expect(
      peerStatusForRow(
        makeRow({ value: "12", schema_error: true }),
        stats,
        byKey,
      ),
    ).toBe("neutral");
  });

  it("missing-id (catalog row absent) collapses to neutral", () => {
    const emptyByKey: CatalogByKey = () => undefined;
    expect(peerStatusForRow(makeRow({ value: "12" }), stats, emptyByKey)).toBe(
      "neutral",
    );
  });

  it("unchecked catalog rows render the same as ok (schema_error gates the rule, not schema_status directly)", () => {
    // Per DESIGN §3.3 only `schema_status='error'` is special-cased.
    // For `'unchecked'` rows transforms.ts leaves `row.schema_error`
    // unset, so peer coloring proceeds as normal — this test pins
    // that behavior. `peerStatusForRow` itself never reads
    // `m.schema_status`; the gate lives upstream in transforms.
    const uncheckedByKey = makeByMetricKey([
      makeCatalogMetric({ schema_status: "unchecked", higher_is_better: true }),
    ]);
    expect(
      peerStatusForRow(makeRow({ value: "12" }), stats, uncheckedByKey),
    ).toBe("top");
  });

  it("missing cohort stats degrade to neutral", () => {
    expect(
      peerStatusForRow(makeRow({ value: "12" }), undefined, byKey),
    ).toBe("neutral");
  });

  it("non-numeric value degrades to neutral", () => {
    expect(peerStatusForRow(makeRow({ value: "—" }), stats, byKey)).toBe(
      "neutral",
    );
  });
});

describe("rowStatus — maps peer status to display status", () => {
  const byKey = makeByMetricKey([
    makeCatalogMetric({ higher_is_better: true }),
  ]);
  const stats = new Map([["tasks_completed", STATS]]);

  it("top → good, bottom → bad, in_pack → warn, neutral → neutral", () => {
    expect(rowStatus(makeRow({ value: "12" }), stats, byKey)).toBe("good");
    expect(rowStatus(makeRow({ value: "2" }), stats, byKey)).toBe("bad");
    expect(rowStatus(makeRow({ value: "6" }), stats, byKey)).toBe("warn");
    expect(rowStatus(makeRow({ value: "—" }), stats, byKey)).toBe("neutral");
  });
});

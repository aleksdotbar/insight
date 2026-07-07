import { describe, expect, it } from "vitest";

import type { CatalogMetric } from "@/api/catalog-client";
import type { MetricResult } from "@/api/metric-results-client";
import {
  legacyAttentionItems,
  metricAttentionItems,
} from "@/lib/insight/attention";
import type { MetricGroup } from "@/lib/insight/groups";
import { normalizeMetricResults } from "@/lib/metrics/collection";
import type { BulletMetric } from "@/types/insight";

function bullet(overrides: Partial<BulletMetric> = {}): BulletMetric {
  return {
    metric_key: "meeting_hours",
    label: "Meeting hours",
    value: "22",
    unit: "h",
    peer: { p25: 4, p50: 8, p75: 12, min: 1, max: 30, n: 9 },
    ...overrides,
  } as BulletMetric;
}

const CATALOG_ROW = {
  higher_is_better: false,
} as unknown as CatalogMetric;

function aiMetric(value: number | null): MetricResult {
  return {
    metric_key: "ai.active_days",
    label: "Active AI days",
    unit: "days",
    format: "integer",
    direction: "higher_is_better",
    computation: "sum",
    views: [
      { view: "period", values: [{ entity_id: "me@x.com", value }] },
      {
        view: "peer",
        values: [
          {
            entity_id: "me@x.com",
            target_value: value,
            p25: 5,
            median: 11,
            p75: 15,
            min: 0,
            max: 30,
            n: 9,
          },
        ],
      },
    ],
  };
}

const AI_DEF: MetricGroup = {
  kind: "metrics",
  id: "ai_adoption",
  title: "AI adoption",
  collection: {
    metrics: [
      { key: "ai.active_days", views: [{ view: "period" }, { view: "peer" }] },
    ],
  },
  card: { preview: [] },
  drilldown: [],
};

describe("legacyAttentionItems", () => {
  it("surfaces bottom-quartile rows with a display-ready shape", () => {
    const items = legacyAttentionItems(
      [{ id: "collaboration", rows: [bullet()] }],
      () => CATALOG_ROW,
    );
    expect(items).toHaveLength(1);
    expect(items[0]).toMatchObject({
      group: "collaboration",
      label: "Meeting hours",
      valueText: "22 h",
      medianText: "Median 8 h",
    });
    expect(items[0]?.relGap).toBeGreaterThan(0);
  });

  it("skips schema errors, non-numeric values, and in-pack rows", () => {
    const items = legacyAttentionItems(
      [
        {
          id: "collaboration",
          rows: [
            bullet({ schema_error: true }),
            bullet({ value: "—" }),
            bullet({ value: "8" }),
          ],
        },
      ],
      () => CATALOG_ROW,
    );
    expect(items).toHaveLength(0);
  });
});

describe("metricAttentionItems", () => {
  it("surfaces bottom-quartile metrics with the same item shape", () => {
    const byKey = normalizeMetricResults([aiMetric(2)]);
    const items = metricAttentionItems(AI_DEF, byKey, "me@x.com");
    expect(items).toHaveLength(1);
    expect(items[0]).toMatchObject({
      group: "ai_adoption",
      label: "Active AI days",
      valueText: "2 days",
      medianText: "Median 11 days",
    });
  });

  it("never flags unmeasured people (null peer target_value)", () => {
    const unmeasured = aiMetric(0);
    const peerView = unmeasured.views[1];
    if (peerView?.view === "peer" && peerView.values[0]) {
      peerView.values[0].target_value = null;
    }
    expect(
      metricAttentionItems(
        AI_DEF,
        normalizeMetricResults([unmeasured]),
        "me@x.com",
      ),
    ).toHaveLength(0);
  });

  it("ignores in-pack values and missing data", () => {
    expect(
      metricAttentionItems(
        AI_DEF,
        normalizeMetricResults([aiMetric(10)]),
        "me@x.com",
      ),
    ).toHaveLength(0);
    expect(
      metricAttentionItems(
        AI_DEF,
        normalizeMetricResults([aiMetric(null)]),
        "me@x.com",
      ),
    ).toHaveLength(0);
  });

  it("produces the identical intermediate shape as the legacy selector", () => {
    const legacy = legacyAttentionItems(
      [{ id: "collaboration", rows: [bullet()] }],
      () => CATALOG_ROW,
    )[0]!;
    const metric = metricAttentionItems(
      AI_DEF,
      normalizeMetricResults([aiMetric(2)]),
      "me@x.com",
    )[0]!;
    expect(Object.keys(legacy).sort()).toEqual(Object.keys(metric).sort());
  });
});

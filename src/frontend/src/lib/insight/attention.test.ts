import { describe, expect, it } from "vitest";

import type { MetricResult } from "@/api/metric-results-client";
import { metricAttentionItems } from "@/lib/insight/attention";
import type { MetricGroup } from "@/lib/insight/groups";
import { normalizeMetricResults } from "@/lib/metrics/collection";

function aiMetric(value: number | null, key = "ai.active_days"): MetricResult {
  return {
    metric_key: key,
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

/**
 * `ai.sessions` is the block's to show; `ai.active_days` is on the headline
 * row (it is in `KPI_ROW`) and therefore this block's to leave alone. A
 * fixture needs both to express the rule.
 */
const AI_DEF: MetricGroup = {
  id: "ai_adoption",
  title: "AI adoption",
  collection: {
    metrics: [
      { key: "ai.sessions", views: [{ view: "period" }, { view: "peer" }] },
      { key: "ai.active_days", views: [{ view: "period" }, { view: "peer" }] },
    ],
  },
  card: { preview: [] },
  drilldown: [],
};

function bothMetrics(value: number | null) {
  return normalizeMetricResults([
    aiMetric(value, "ai.sessions"),
    aiMetric(value, "ai.active_days"),
  ]);
}

describe("metricAttentionItems", () => {
  it("surfaces bottom-quartile metrics with the same item shape", () => {
    const items = metricAttentionItems(AI_DEF, bothMetrics(2), "me@x.com");
    expect(items).toHaveLength(1);
    expect(items[0]).toMatchObject({
      key: "ai.sessions",
      group: "ai_adoption",
      valueText: "2 days",
      medianText: "11 days",
      gapText: "-82%",
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
    expect(metricAttentionItems(AI_DEF, bothMetrics(10), "me@x.com")).toHaveLength(0);
    expect(metricAttentionItems(AI_DEF, bothMetrics(null), "me@x.com")).toHaveLength(0);
  });
});

describe("what the headline row already shows", () => {
  it("is left to the row — the block never repeats a headline metric", () => {
    // This block is the only place on the person page that names problems, so
    // it shows everything standing out EXCEPT what the row above already
    // carries. Repeating one puts a single finding on the screen twice, and a
    // reader counts marks rather than facts.
    const items = metricAttentionItems(AI_DEF, bothMetrics(2), "me@x.com");
    expect(items.map((i) => i.key)).not.toContain("ai.active_days");
  });

  it("shows a metric the row does not carry, whatever the card used to list", () => {
    // `card.preview` no longer gates this: the page has no section cards, so
    // excluding those keys would hide them from the screen entirely.
    const onOldCard: MetricGroup = {
      ...AI_DEF,
      card: { preview: ["ai.sessions"] },
    };
    const items = metricAttentionItems(onOldCard, bothMetrics(2), "me@x.com");
    expect(items.map((i) => i.key)).toEqual(["ai.sessions"]);
  });
});

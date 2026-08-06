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
 * Two metrics, as a real group has: the card leads with ONE of them, and the
 * block's job is what is left. A single-metric fixture cannot express that —
 * its only metric is always the card's lead.
 */
const AI_DEF: MetricGroup = {
  id: "ai_adoption",
  title: "AI adoption",
  collection: {
    metrics: [
      { key: "ai.cost", views: [{ view: "period" }, { view: "peer" }] },
      { key: "ai.active_days", views: [{ view: "period" }, { view: "peer" }] },
    ],
  },
  card: { preview: [] },
  drilldown: [],
};

/** The card leads with `ai.cost` here; `ai.active_days` is the block's to show. */
function bothMetrics(value: number | null) {
  return normalizeMetricResults([aiMetric(0, "ai.cost"), aiMetric(value)]);
}

describe("metricAttentionItems", () => {
  it("surfaces bottom-quartile metrics with the same item shape", () => {
    const byKey = bothMetrics(2);
    const items = metricAttentionItems(AI_DEF, byKey, "me@x.com");
    expect(items).toHaveLength(1);
    expect(items[0]).toMatchObject({
      group: "ai_adoption",
      label: "Active AI days",
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
        "me@x.com"
      )
    ).toHaveLength(0);
  });

  it("ignores in-pack values and missing data", () => {
    expect(
      metricAttentionItems(
        AI_DEF,
        bothMetrics(10),
        "me@x.com"
      )
    ).toHaveLength(0);
    expect(
      metricAttentionItems(
        AI_DEF,
        bothMetrics(null),
        "me@x.com"
      )
    ).toHaveLength(0);
  });
});

describe("what the section card already shows", () => {
  it("is left to the card — the block never repeats a preview row", () => {
    // A card carries its preview rows in the same colours plus a "N behind
    // peers" badge over them. Repeating those here put one finding on the
    // screen twice, and a reader counts red marks, not facts.
    const onCard: MetricGroup = { ...AI_DEF, card: { preview: ["ai.active_days"] } };
    expect(metricAttentionItems(onCard, bothMetrics(2), "me@x.com")).toEqual([]);
  });

  it("still surfaces a bottom-quartile metric the card omits", () => {
    // The block's whole job: the finding you would otherwise miss, because it
    // sits outside the three rows the card had room for.
    const offCard: MetricGroup = { ...AI_DEF, card: { preview: ["ai.cost"] } };
    const items = metricAttentionItems(offCard, bothMetrics(2), "me@x.com");
    expect(items.map((i) => i.key)).toEqual(["ai.active_days"]);
  });
});

describe("the card's summary line", () => {
  it("is left to the card too — the lead is not repeated above it", () => {
    // The lead is picked from EVERY metric of the group, not from the three
    // the card lists, so excluding the preview alone let it through: "Lines
    // added · −98% vs median" was both a card headline and the first
    // attention row on the same screen.
    const worst = normalizeMetricResults([
      aiMetric(0, "ai.cost"),
      aiMetric(9),
    ]);
    const items = metricAttentionItems(
      { ...AI_DEF, card: { preview: [] } },
      worst,
      "me@x.com",
    );
    expect(items.map((i) => i.key)).not.toContain("ai.cost");
  });
});

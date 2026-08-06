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
/** What the headline row rendered — only these are the block's to skip. */
const HEADLINE = new Set(["ai.active_days"]);

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

/** A previous period the current one fell from, so a standing is also a change. */
function before(value: number) {
  return normalizeMetricResults([
    aiMetric(value, "ai.sessions"),
    aiMetric(value, "ai.active_days"),
  ]);
}

describe("metricAttentionItems", () => {
  it("surfaces bottom-quartile metrics with the same item shape", () => {
    const items = metricAttentionItems(AI_DEF, bothMetrics(2), before(9), "me@x.com", HEADLINE);
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
        before(9),
        "me@x.com",
        HEADLINE,
      ),
    ).toHaveLength(0);
  });

  it("ignores in-pack values and missing data", () => {
    expect(metricAttentionItems(AI_DEF, bothMetrics(10), before(9), "me@x.com", HEADLINE)).toHaveLength(0);
    expect(metricAttentionItems(AI_DEF, bothMetrics(null), before(9), "me@x.com", HEADLINE)).toHaveLength(0);
  });
});

describe("what the headline row already shows", () => {
  it("is left to the row — the block never repeats a headline metric", () => {
    // This block is the only place on the person page that names problems, so
    // it shows everything standing out EXCEPT what the row above already
    // carries. Repeating one puts a single finding on the screen twice, and a
    // reader counts marks rather than facts.
    const items = metricAttentionItems(AI_DEF, bothMetrics(2), before(9), "me@x.com", HEADLINE);
    expect(items.map((i) => i.key)).not.toContain("ai.active_days");
  });

  it("shows a metric the row does not carry, whatever the card used to list", () => {
    // `card.preview` no longer gates this: the page has no section cards, so
    // excluding those keys would hide them from the screen entirely.
    const onOldCard: MetricGroup = {
      ...AI_DEF,
      card: { preview: ["ai.sessions"] },
    };
    const items = metricAttentionItems(onOldCard, bothMetrics(2), before(9), "me@x.com", HEADLINE);
    expect(items.map((i) => i.key)).toEqual(["ai.sessions"]);
  });
});

describe("a standing is not an event", () => {
  it("stays silent when a metric is below its cohort but did not move", () => {
    // A lead measured against the developers reporting to them is below on
    // commits every month, by the shape of the job. Repeating that forever
    // teaches the reader to skip the block; a flat gap is not news.
    const items = metricAttentionItems(AI_DEF, bothMetrics(2), before(2), "me@x.com", HEADLINE);
    expect(items).toEqual([]);
  });

  it("stays silent when it moved the RIGHT way, even if still behind", () => {
    const items = metricAttentionItems(AI_DEF, bothMetrics(2), before(1), "me@x.com", HEADLINE);
    expect(items).toEqual([]);
  });

  it("makes no claim about direction without a previous period", () => {
    // One period of data cannot say which way anything is going.
    expect(metricAttentionItems(AI_DEF, bothMetrics(2), null, "me@x.com", HEADLINE)).toEqual([]);
  });
});

describe("the row's candidates are not the row", () => {
  it("shows a bottom-quartile candidate the row had no slot for", () => {
    // `KPI_ROW` lists more candidates than the row renders. Excluding the
    // whole list would hide a metric that reached neither surface — visible
    // nowhere, which is the one outcome worse than showing it twice.
    const items = metricAttentionItems(
      AI_DEF,
      bothMetrics(2),
      before(9),
      "me@x.com",
      new Set<string>(), // the row rendered nothing from this group
    );
    expect(items.map((i) => i.key).sort()).toEqual([
      "ai.active_days",
      "ai.sessions",
    ]);
  });
});

describe("value split", () => {
  it("keeps a percent a percent", () => {
    // The list renders the split fields, so a lost "%" turns 50% into 50 — a
    // different number, not a shorter one.
    const pct = {
      ...aiMetric(2, "ai.sessions"),
      format: "percent" as const,
      unit: null,
    };
    const items = metricAttentionItems(
      AI_DEF,
      normalizeMetricResults([pct]),
      before(9),
      "me@x.com",
      new Set<string>(),
    );
    expect(items[0]?.valueNumber).toBe("2");
    expect(items[0]?.valueUnit).toBe("%");
  });
});

import { describe, expect, it } from "vitest";

import {
  RATIO_METRIC_FIXTURE,
  SUM_METRIC_FIXTURE,
} from "@/mocks/metric-results-fixtures";
import {
  buildMetricCollectionRequest,
  chunkEntityIds,
  entityChunkSize,
  entityObserved,
  forEntity,
  mergeNormalizedResults,
  normalizeMetricResult,
  normalizeMetricResults,
  projectViews,
  resolveBucket,
  type MetricCollectionConfig,
} from "@/lib/metrics/collection";

const COLLECTION: MetricCollectionConfig = {
  metrics: [
    {
      key: "ai.accepted_lines",
      views: [
        { view: "period" },
        { view: "peer" },
        { view: "timeseries", bucket: "auto", dimensions: ["tool"] },
        { view: "breakdown", dimensions: ["tool"] },
      ],
    },
    {
      key: "ai.tool_acceptance_rate",
      views: [{ view: "period" }, { view: "peer" }],
    },
  ],
};

const RANGE = { from: "2026-06-01", to: "2026-06-30" };

describe("buildMetricCollectionRequest", () => {
  it("derives the wire request from the collection", () => {
    const request = buildMetricCollectionRequest(
      COLLECTION,
      { type: "person", ids: ["alice@example.com"] },
      RANGE,
    );
    expect(request.entity).toEqual({
      type: "person",
      ids: ["alice@example.com"],
    });
    expect(request.metrics).toHaveLength(2);
    expect(request.metrics[0]?.views).toEqual([
      { view: "period" },
      { view: "peer" },
      { view: "timeseries", bucket: "day", dimensions: ["tool"] },
      { view: "breakdown", dimensions: ["tool"] },
    ]);
  });
});

describe("projectViews", () => {
  it("derives a view subset without touching the source collection", () => {
    const projected = projectViews(COLLECTION, ["period", "peer"]);
    expect(
      projected.metrics.every((m) =>
        m.views.every((v) => v.view === "period" || v.view === "peer"),
      ),
    ).toBe(true);
    expect(COLLECTION.metrics[0]?.views).toHaveLength(4);
  });

  it("drops metrics whose views project to empty (backend rejects views: [])", () => {
    const chartOnly: MetricCollectionConfig = {
      metrics: [
        {
          key: "m.chart",
          views: [{ view: "timeseries", bucket: "auto" }],
        },
        { key: "m.full", views: [{ view: "period" }, { view: "peer" }] },
      ],
    };
    const projected = projectViews(chartOnly, ["period", "peer"]);
    expect(projected.metrics.map((m) => m.key)).toEqual(["m.full"]);
  });
});

describe("normalizeMetricResult forward-compat", () => {
  it("ignores unknown view kinds instead of crashing the collection", () => {
    const withUnknown = {
      ...SUM_METRIC_FIXTURE,
      views: [
        ...SUM_METRIC_FIXTURE.views,
        { view: "histogram", bins: [] } as never,
      ],
    };
    const normalized = normalizeMetricResult(withUnknown);
    expect(normalized.period).toBeDefined();
    expect(normalized.peer).toBeDefined();
  });
});

describe("entityObserved", () => {
  it("distinguishes observed from zero-filled entities", () => {
    const metric = normalizeMetricResults([SUM_METRIC_FIXTURE]).get(
      "ai.accepted_lines",
    )!;
    // alice: peer row with a target_value → observed.
    expect(entityObserved(metric, "alice@example.com")).toBe(true);
    // bob: zero-filled period value, no peer row → unobserved.
    expect(entityObserved(metric, "bob@example.com")).toBe(false);
    // nobody: absent everywhere → unobserved.
    expect(entityObserved(metric, "nobody@example.com")).toBe(false);
  });
});

describe("row-limit chunking", () => {
  const PERIOD_PEER: MetricCollectionConfig = {
    metrics: Array.from({ length: 10 }, (_, i) => ({
      key: `m.${i}`,
      views: [{ view: "period" as const }, { view: "peer" as const }],
    })),
  };

  it("computes the per-request entity capacity from the view count", () => {
    // 10 metrics × 2 views = 20 rows/entity; 4500 / 20 = 225.
    expect(entityChunkSize(PERIOD_PEER)).toBe(225);
  });

  it("is not chunkable with bucket-projected views", () => {
    expect(entityChunkSize(COLLECTION)).toBeNull();
  });

  it("splits a 251-person roster into limit-safe chunks", () => {
    const ids = Array.from({ length: 251 }, (_, i) => `p${i}@x.com`);
    const chunks = chunkEntityIds(ids, entityChunkSize(PERIOD_PEER)!);
    expect(chunks).toHaveLength(2);
    expect(chunks.flat()).toHaveLength(251);
    for (const chunk of chunks) {
      expect(chunk.length * 20).toBeLessThanOrEqual(4500);
    }
  });

  it("merges chunked results back into one collection", () => {
    const a = normalizeMetricResults([SUM_METRIC_FIXTURE]);
    const bFixture = structuredClone(SUM_METRIC_FIXTURE);
    const periodView = bFixture.views[0];
    if (periodView?.view === "period") {
      periodView.values = [{ entity_id: "carol@example.com", value: 7 }];
    }
    const merged = mergeNormalizedResults([a, normalizeMetricResults([bFixture])]);
    const metric = merged.get("ai.accepted_lines")!;
    expect(metric.period?.values.map((v) => v.entity_id)).toEqual([
      "alice@example.com",
      "bob@example.com",
      "carol@example.com",
    ]);
    // Source maps untouched.
    expect(a.get("ai.accepted_lines")?.period?.values).toHaveLength(2);
  });
});

describe("resolveBucket", () => {
  it("keeps explicit buckets", () => {
    expect(resolveBucket("month", RANGE)).toBe("month");
  });

  it("tiers auto: day ≤ 62d, week ≤ 182d, month beyond", () => {
    expect(resolveBucket("auto", { from: "2026-06-01", to: "2026-06-30" })).toBe(
      "day",
    );
    expect(resolveBucket("auto", { from: "2026-01-01", to: "2026-03-03" })).toBe(
      "day",
    );
    expect(resolveBucket("auto", { from: "2026-01-01", to: "2026-05-01" })).toBe(
      "week",
    );
    expect(resolveBucket("auto", { from: "2025-07-01", to: "2026-06-30" })).toBe(
      "month",
    );
  });
});

describe("normalizeMetricResult", () => {
  it("maps views onto per-view fields and keeps wire metadata", () => {
    const normalized = normalizeMetricResult(SUM_METRIC_FIXTURE);
    expect(normalized.computation).toBe("sum");
    expect(normalized.explanation).toBeDefined();
    expect(normalized.period).toBeDefined();
    expect(normalized.peer).toBeDefined();
    expect(normalized.timeseries).toBeDefined();
    expect(normalized.breakdown).toBeDefined();
    expect("views" in normalized).toBe(false);
  });

  it("carries ratio scale and null unit", () => {
    const normalized = normalizeMetricResult(RATIO_METRIC_FIXTURE);
    expect(normalized.scale).toBe(100);
    expect(normalized.unit).toBeNull();
  });
});

describe("forEntity", () => {
  const byKey = normalizeMetricResults([SUM_METRIC_FIXTURE]);
  const metric = byKey.get("ai.accepted_lines")!;

  it("slices every view for one entity", () => {
    const data = forEntity(metric, "alice@example.com");
    expect(data.value).toBe(1240);
    expect(data.peer?.n).toBe(12);
    expect(data.bucket).toBe("day");
    expect(data.series).toHaveLength(2);
    expect(data.breakdown).toHaveLength(2);
  });

  it("returns empty slices for unknown entities", () => {
    const data = forEntity(metric, "nobody@example.com");
    expect(data.value).toBeNull();
    expect(data.peer).toBeNull();
    expect(data.series).toHaveLength(0);
    expect(data.breakdown).toHaveLength(0);
  });

  it("zero-fills sums for requested-but-inactive entities (backend contract)", () => {
    expect(forEntity(metric, "bob@example.com").value).toBe(0);
  });

  it("keeps a null period value distinct from a missing entity", () => {
    const ratio = normalizeMetricResults([RATIO_METRIC_FIXTURE]).get(
      "ai.tool_acceptance_rate",
    )!;
    expect(forEntity(ratio, "bob@example.com").value).toBeNull();
    expect(forEntity(ratio, "nobody@example.com").value).toBeNull();
  });
});

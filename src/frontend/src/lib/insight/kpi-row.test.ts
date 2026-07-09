import { describe, expect, it } from "vitest";

import type { CatalogMetric } from "@/api/catalog-client";
import type { MetricResult } from "@/api/metric-results-client";
import {
  kpiRowTiles,
  legacyKpiTiles,
  metricKpiTiles,
} from "@/lib/insight/kpi-row";
import { KPI_ROW } from "@/lib/insight/groups";
import { normalizeMetricResults } from "@/lib/metrics/collection";
import type { IcKpi } from "@/types/insight";

function icKpi(overrides: Partial<IcKpi> = {}): IcKpi {
  return {
    period: "month",
    metric_key: "tasks_closed",
    label: "Tasks closed",
    value: "12",
    raw_value: 12,
    unit: "",
    sublabel: "",
    delta: "+9%",
    delta_type: "good",
    peer_median: 10,
    peer_n: 8,
    ...overrides,
  };
}

const CATALOG_ROW = {
  higher_is_better: true,
  schema_status: "ok",
  format: "integer",
  source_tags: ["jira"],
} as unknown as CatalogMetric;

function metricResult(
  key: string,
  value: number | null,
  overrides: Partial<MetricResult> = {},
): MetricResult {
  return {
    metric_key: key,
    label: key,
    unit: null,
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
            median: 10,
            p75: 15,
            min: 0,
            max: 30,
            n: 8,
          },
        ],
      },
    ],
    ...overrides,
  } as MetricResult;
}

describe("legacyKpiTiles", () => {
  it("keeps legacy formatting/scoring semantics", () => {
    const tiles = legacyKpiTiles([icKpi()], () => CATALOG_ROW, "all");
    expect(tiles).toHaveLength(1);
    const tile = tiles[0]!;
    expect(tile.value).toBe("12");
    expect(tile.valueStatus).toBe("good"); // 12 >= median 10
    expect(tile.delta).toEqual({ text: "+9%", status: "good", down: false });
    expect(tile.groupId).toBe("task_delivery");
    expect(tile.context).toBe("jira");
  });

  it("drops the trailing % from percent-point deltas", () => {
    const tiles = legacyKpiTiles(
      [
        icKpi({
          metric_key: "focus_time_pct",
          unit: "%",
          value: "62",
          raw_value: 62,
          delta: "+5%",
        }),
      ],
      () => ({ ...CATALOG_ROW, format: "percent" }) as CatalogMetric,
      "all",
    );
    expect(tiles[0]?.delta?.text).toBe("+5");
    expect(tiles[0]?.value).toBe("62%");
  });
});

describe("metricKpiTiles", () => {
  it("builds display-ready tiles with median status and delta", () => {
    const byKey = normalizeMetricResults([
      metricResult("ai.active_days", 14),
      metricResult("ai.accepted_lines", 900),
    ]);
    const previous = normalizeMetricResults([
      metricResult("ai.active_days", 12),
      metricResult("ai.accepted_lines", 1000),
    ]);
    const tiles = metricKpiTiles(byKey, previous, "me@x.com", "all");
    expect(tiles.map((t) => t.key)).toEqual([
      "ai.active_days",
      "ai.accepted_lines",
    ]);
    const active = tiles[0]!;
    expect(active.value).toBe("14");
    expect(active.valueStatus).toBe("good"); // >= median 10
    expect(active.delta?.text).toBe("+17%");
    expect(active.medianLabel).toBe("Median 10");
    expect(active.groupId).toBe("ai_adoption");
    const lines = tiles[1]!;
    expect(lines.delta).toEqual({ text: "-10%", status: "bad", down: true });
  });

  it("renders neutral when the backend suppressed the peer stats", () => {
    // Below the disclosure floor the server returns null percentiles with
    // the true n; the tile must not score or show a median off them.
    const result = metricResult("ai.active_days", 14);
    const peerView = result.views[1];
    if (peerView?.view === "peer" && peerView.values[0]) {
      Object.assign(peerView.values[0], {
        p25: null,
        median: null,
        p75: null,
        min: null,
        max: null,
        n: 3,
      });
    }
    const tiles = metricKpiTiles(
      normalizeMetricResults([result]),
      null,
      "me@x.com",
      "all",
    );
    expect(tiles[0]?.valueStatus).toBe("neutral");
    expect(tiles[0]?.medianLabel).toBeNull();
  });

  it("renders neutral for unmeasured people (null peer target_value)", () => {
    const result = metricResult("ai.active_days", 0);
    const peerView = result.views[1];
    if (peerView?.view === "peer" && peerView.values[0]) {
      peerView.values[0].target_value = null;
    }
    const tiles = metricKpiTiles(
      normalizeMetricResults([result]),
      null,
      "me@x.com",
      "all",
    );
    expect(tiles[0]?.valueStatus).toBe("neutral");
  });

  it("computes pp deltas for percent ratios (pinned until a ratio reaches the row)", () => {
    const current = metricResult("ai.active_days", 77, {
      metric_key: "ai.active_days",
      format: "percent",
      computation: "ratio",
      scale: 100,
    } as Partial<MetricResult>);
    const previous = metricResult("ai.active_days", 72, {
      format: "percent",
      computation: "ratio",
      scale: 100,
    } as Partial<MetricResult>);
    const tiles = metricKpiTiles(
      normalizeMetricResults([current]),
      normalizeMetricResults([previous]),
      "me@x.com",
      "all",
    );
    expect(tiles[0]?.delta?.text).toBe("+5.0 pp");
  });
});

describe("kpiRowTiles", () => {
  it("orders tiles by KPI_ROW display order across legacy and metric sources", () => {
    const legacy = legacyKpiTiles(
      [icKpi({ metric_key: "tasks_closed" })],
      () => CATALOG_ROW,
      "all",
    );
    const metric = metricKpiTiles(
      normalizeMetricResults([
        metricResult("git.prs_merged", 9),
        metricResult("ai.active_days", 14),
      ]),
      null,
      "me@x.com",
      "all",
    );
    const ordered = kpiRowTiles(legacy, metric).map((t) => t.key);
    const expected = KPI_ROW.map((s) =>
      s.kind === "legacy" ? s.key : s.metricKey,
    ).filter((k) =>
      ["tasks_closed", "git.prs_merged", "ai.active_days"].includes(k),
    );
    expect(ordered).toEqual(expected);
    expect(ordered).toEqual([
      "tasks_closed",
      "git.prs_merged",
      "ai.active_days",
    ]);
  });
});

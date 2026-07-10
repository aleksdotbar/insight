import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { MetricTrend } from "@/components/widgets/metric-views/metric-trend";
import {
  normalizeMetricResults,
  type NormalizedMetricResult,
} from "@/lib/metrics/collection";
import type { MetricResult } from "@/api/metric-results-client";

function trendMetric(
  key: string,
  points: Array<{ bucket_start: string; value: number | null }>,
  dims: Array<{ key: string; value: string; label?: string }> = [],
): NormalizedMetricResult {
  const result: MetricResult = {
    metric_key: key,
    label: key,
    unit: "lines",
    format: "integer",
    direction: "higher_is_better",
    computation: "sum",
    views: [
      {
        view: "timeseries",
        bucket: "day",
        series: [{ entity_id: "me@x.com", dimensions: dims, points }],
      },
    ],
  };
  return normalizeMetricResults([result]).get(key)!;
}

describe("MetricTrend", () => {
  it("renders a single metric's title and line chart", () => {
    render(
      <MetricTrend
        metrics={[
          trendMetric("accepted", [
            { bucket_start: "2026-06-01", value: 5 },
            { bucket_start: "2026-06-02", value: 8 },
          ]),
        ]}
        entityId="me@x.com"
        chart="line"
      />,
    );
    expect(screen.getByText("accepted over time")).toBeInTheDocument();
  });

  it("joins titles for a multi-metric block", () => {
    render(
      <MetricTrend
        metrics={[
          trendMetric("commits", [{ bucket_start: "2026-06-01", value: 3 }]),
          trendMetric("prs", [{ bucket_start: "2026-06-01", value: 1 }]),
        ]}
        entityId="me@x.com"
        chart="stacked-bar"
      />,
    );
    expect(screen.getByText("commits & prs")).toBeInTheDocument();
  });

  it("shows the empty card when there are no points", () => {
    render(
      <MetricTrend
        metrics={[trendMetric("accepted", [])]}
        entityId="me@x.com"
        chart="line"
      />,
    );
    expect(screen.getByText("No trend data yet.")).toBeInTheDocument();
  });

  it("puts each part's total and share in the legend for a composition", () => {
    const composition: MetricResult = {
      metric_key: "lines_added",
      label: "Lines added",
      unit: "lines",
      format: "integer",
      direction: "higher_is_better",
      computation: "sum",
      views: [
        {
          view: "timeseries",
          bucket: "day",
          series: [
            {
              entity_id: "me@x.com",
              dimensions: [{ key: "category", value: "code", label: "Code" }],
              points: [{ bucket_start: "2026-06-01", value: 80 }],
            },
            {
              entity_id: "me@x.com",
              dimensions: [{ key: "category", value: "docs", label: "Docs" }],
              points: [{ bucket_start: "2026-06-01", value: 20 }],
            },
          ],
        },
      ],
    };
    render(
      <MetricTrend
        metrics={[normalizeMetricResults([composition]).get("lines_added")!]}
        entityId="me@x.com"
        chart="stacked-bar"
      />,
    );
    expect(screen.getByText(/80\s*lines\s*·\s*80%/)).toBeInTheDocument();
    expect(screen.getByText(/20\s*lines\s*·\s*20%/)).toBeInTheDocument();
  });

  it("omits shares for a multi-metric trend (not a composition)", () => {
    render(
      <MetricTrend
        metrics={[
          trendMetric("commits", [{ bucket_start: "2026-06-01", value: 3 }]),
          trendMetric("prs", [{ bucket_start: "2026-06-01", value: 1 }]),
        ]}
        entityId="me@x.com"
        chart="line"
      />,
    );
    expect(screen.getByText("commits")).toBeInTheDocument();
    expect(screen.getByText("prs")).toBeInTheDocument();
    expect(screen.queryByText(/%/)).not.toBeInTheDocument();
  });
});

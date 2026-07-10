import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { MetricBreakdown } from "@/components/widgets/metric-views/metric-breakdown";
import {
  normalizeMetricResults,
  type NormalizedMetricResult,
} from "@/lib/metrics/collection";
import type { MetricResult } from "@/api/metric-results-client";

function breakdownMetric(
  values: Array<{ tool: string; value: number | null }>,
): NormalizedMetricResult {
  const result: MetricResult = {
    metric_key: "ai.accepted_lines",
    label: "Accepted lines",
    unit: "lines",
    format: "integer",
    direction: "higher_is_better",
    computation: "sum",
    views: [
      {
        view: "breakdown",
        dimensions: ["tool"],
        values: values.map((v) => ({
          entity_id: "me@x.com",
          dimensions: [{ key: "tool", value: v.tool, label: v.tool }],
          value: v.value,
        })),
      },
    ],
  };
  return normalizeMetricResults([result]).get("ai.accepted_lines")!;
}

describe("MetricBreakdown", () => {
  it("renders a proportional strip over positive rows", () => {
    render(
      <MetricBreakdown
        metric={breakdownMetric([
          { tool: "cursor", value: 60 },
          { tool: "claude_code", value: 40 },
        ])}
        entityId="me@x.com"
      />,
    );
    expect(screen.getByText("cursor")).toBeInTheDocument();
    expect(screen.getByText("claude_code")).toBeInTheDocument();
    // Legend carries the share; narrow slices never clip inside the ribbon.
    expect(screen.getByText("60%")).toBeInTheDocument();
    expect(screen.getByText("40%")).toBeInTheDocument();
  });

  it("shows the empty state when no row has a positive value", () => {
    render(
      <MetricBreakdown
        metric={breakdownMetric([
          { tool: "cursor", value: 0 },
          { tool: "claude_code", value: null },
        ])}
        entityId="me@x.com"
      />,
    );
    expect(screen.getByText("No composition data yet.")).toBeInTheDocument();
  });
});

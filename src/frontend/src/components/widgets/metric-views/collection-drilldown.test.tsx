import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { CollectionDrilldown } from "@/components/widgets/metric-views/collection-drilldown";
import type { MetricGroup } from "@/lib/insight/groups";
import { normalizeMetricResults } from "@/lib/metrics/collection";
import type { MetricCollectionResult } from "@/queries/metric-results";
import {
  RATIO_METRIC_FIXTURE,
  SUM_METRIC_FIXTURE,
} from "@/mocks/metric-results-fixtures";

vi.mock("@/hooks/use-settings", () => ({
  useSettings: () => ({ focusMode: "all", showExplanations: true }),
}));

const DEF: MetricGroup = {
  kind: "metrics",
  id: "ai_adoption",
  title: "AI adoption",
  collection: {
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
  },
  card: { preview: ["ai.accepted_lines"] },
  drilldown: [
    { chart: "bars", view: "breakdown", metrics: ["ai.accepted_lines"] },
    {
      chart: "stacked-bar",
      view: "timeseries",
      metrics: ["ai.accepted_lines"],
    },
  ],
};

function result(
  overrides: Partial<MetricCollectionResult> = {},
): MetricCollectionResult {
  return {
    byKey: normalizeMetricResults([SUM_METRIC_FIXTURE, RATIO_METRIC_FIXTURE]),
    previousByKey: null,
    isPending: false,
    isFetching: false,
    isError: false,
    refetch: vi.fn(),
    ...overrides,
  };
}

describe("CollectionDrilldown", () => {
  it("renders the def's blocks and the peer story from wire data", () => {
    render(
      <CollectionDrilldown
        def={DEF}
        data={result()}
        entityId="alice@example.com"
      />,
    );
    // Breakdown block: composition by tool with response-provided labels.
    expect(screen.getByText("Period total by tool")).toBeInTheDocument();
    expect(screen.getAllByText("Claude Code").length).toBeGreaterThan(0);
    // Timeseries block.
    expect(screen.getByText("Accepted lines over time")).toBeInTheDocument();
    // Peer story: both fixture metrics are in-pack (no outlier hero), so the
    // story falls back to the flat grid with one card per metric.
    expect(screen.getByText("Tool acceptance rate")).toBeInTheDocument();
    expect(screen.getByText("77%")).toBeInTheDocument();
  });

  it("shows the error state with retry when the collection failed", () => {
    const refetch = vi.fn();
    render(
      <CollectionDrilldown
        def={DEF}
        data={result({ isError: true, refetch })}
        entityId="alice@example.com"
      />,
    );
    expect(screen.getByText("Unable to load metrics")).toBeInTheDocument();
  });

  it("shows a spinner while pending", () => {
    const { container } = render(
      <CollectionDrilldown
        def={DEF}
        data={result({ isPending: true })}
        entityId="alice@example.com"
      />,
    );
    expect(container.querySelector("svg")).toBeInTheDocument();
  });
});

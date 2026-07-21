import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { MetricTimeseriesChart } from "@/components/widgets/metric-views/metric-timeseries-chart";
import { MetricTimeseriesTable } from "@/components/widgets/metric-views/metric-timeseries-table";
import { groupedTimeseriesModel } from "@/components/widgets/metric-views/metric-timeseries.test-fixtures";

describe("metric timeseries presentations", () => {
  it("renders grouped metrics in a multi-level table", () => {
    render(<MetricTimeseriesTable model={groupedTimeseriesModel()} />);
    expect(screen.getByText("Week")).toBeInTheDocument();
    expect(screen.getAllByText("org/repo-a").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Commits").length).toBeGreaterThan(0);
    expect(screen.getByText("Grand total")).toBeInTheDocument();
    expect(screen.getByText(/Commits: 6/)).toBeInTheDocument();
    expect(screen.getAllByText("—").length).toBeGreaterThan(0);
  });

  it("renders an ungrouped single-metric table", () => {
    const grouped = groupedTimeseriesModel();
    const model = {
      ...grouped,
      dimensions: [],
      metrics: [grouped.metrics[0]!],
      columns: [grouped.columns[0]!],
      grandTotals: [grouped.grandTotals[0]],
    };
    render(<MetricTimeseriesTable model={model} />);
    expect(screen.getByText("Commits")).toBeInTheDocument();
    expect(screen.queryByText("Grand total")).not.toBeInTheDocument();
  });

  it("renders grouped and ungrouped chart variants", () => {
    const grouped = groupedTimeseriesModel();
    const { rerender } = render(
      <MetricTimeseriesChart model={grouped} selectedMetricKey="git.commits" />
    );
    expect(screen.getByText("org/repo-a")).toBeInTheDocument();
    expect(screen.getAllByText(/3 commits/)).toHaveLength(2);
    rerender(
      <MetricTimeseriesChart
        model={{
          ...grouped,
          dimensions: [],
          metrics: [grouped.metrics[0]!],
          columns: [grouped.columns[0]!],
        }}
        selectedMetricKey="missing"
      />
    );
    expect(screen.queryByText("org/repo-a")).not.toBeInTheDocument();
  });
});

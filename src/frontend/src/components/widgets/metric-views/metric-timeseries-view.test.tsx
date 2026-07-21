import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { MetricTimeseriesView } from "@/components/widgets/metric-views/metric-timeseries-view";
import {
  ENTITY_ID,
  RANGE,
  timeseriesByKey,
} from "@/components/widgets/metric-views/metric-timeseries.test-fixtures";

const mocks = vi.hoisted(() => ({
  collection: vi.fn(),
  collectionSet: vi.fn(),
  csv: vi.fn(),
  xlsx: vi.fn(),
}));

vi.mock("@/queries/metric-results", () => ({
  useMetricCollection: mocks.collection,
  useMetricCollectionSet: mocks.collectionSet,
}));

vi.mock("@/components/widgets/metric-views/metric-timeseries-chart", () => ({
  MetricTimeseriesChart: () => <div>chart presentation</div>,
}));

vi.mock("@/components/widgets/metric-views/metric-timeseries-table", () => ({
  MetricTimeseriesTable: () => <div>table presentation</div>,
}));

vi.mock("@/components/widgets/metric-views/metric-timeseries-csv", () => ({
  downloadMetricTimeseriesCsv: mocks.csv,
}));

vi.mock("@/components/widgets/metric-views/metric-timeseries-xlsx", () => ({
  downloadMetricTimeseriesXlsx: mocks.xlsx,
}));

const ready = {
  byKey: timeseriesByKey(),
  previousByKey: null,
  isPending: false,
  isFetching: false,
  isError: false,
  refetch: vi.fn(),
};

describe("MetricTimeseriesView", () => {
  beforeEach(() => {
    localStorage.clear();
    mocks.collection.mockReturnValue(ready);
    mocks.collectionSet.mockReturnValue(new Map());
    mocks.csv.mockReset();
    mocks.xlsx.mockReset().mockResolvedValue(undefined);
  });

  it("switches presentations and persists independent card state", async () => {
    const user = userEvent.setup();
    render(
      <MetricTimeseriesView
        id="git-output"
        entityId={ENTITY_ID}
        range={RANGE}
        metricKeys={["git.commits", "git.lines_added"]}
        groupBy={{ default: "repository" }}
      />
    );
    expect(screen.getByText("chart presentation")).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "Collapse card" })
    ).toHaveAttribute("aria-pressed", "true");
    await user.click(screen.getByRole("button", { name: "Table view" }));
    expect(screen.getByText("table presentation")).toBeInTheDocument();
    expect(
      localStorage.getItem("insight.timeseries.git-output.presentation")
    ).toBe("table");
    await user.click(screen.getByRole("button", { name: "Collapse card" }));
    expect(localStorage.getItem("insight.timeseries.git-output.expanded")).toBe(
      "false"
    );
  });

  it("renders pending, error, and empty states", () => {
    mocks.collection.mockReturnValue({ ...ready, isPending: true });
    const { container, rerender } = render(
      <MetricTimeseriesView
        id="states"
        entityId={ENTITY_ID}
        range={RANGE}
        metricKeys={["git.commits"]}
      />
    );
    expect(container.querySelector("[aria-busy] svg")).toBeInTheDocument();
    mocks.collection.mockReturnValue({ ...ready, isError: true });
    rerender(
      <MetricTimeseriesView
        id="states"
        entityId={ENTITY_ID}
        range={RANGE}
        metricKeys={["git.commits"]}
      />
    );
    expect(screen.getByText("Unable to load timeseries")).toBeInTheDocument();
    mocks.collection.mockReturnValue({ ...ready, byKey: new Map() });
    rerender(
      <MetricTimeseriesView
        id="states"
        entityId={ENTITY_ID}
        range={RANGE}
        metricKeys={["git.commits"]}
      />
    );
    expect(screen.getByText("No data in this period")).toBeInTheDocument();
  });

  it("builds grouped requests with automatic daily bucketing", () => {
    render(
      <MetricTimeseriesView
        id="request"
        entityId={ENTITY_ID}
        range={{ from: "2026-04-20", to: "2026-04-20" }}
        metricKeys={["git.commits"]}
        groupBy={{ default: "repository", options: ["source"] }}
      />
    );
    expect(mocks.collection.mock.calls.at(-1)?.[0]).toMatchObject({
      metrics: [
        {
          key: "git.commits",
          views: [
            {
              view: "timeseries",
              bucket: "day",
              dimensions: ["repository"],
            },
            { view: "breakdown", dimensions: ["repository"] },
            { view: "period" },
          ],
        },
      ],
    });
    expect(mocks.collectionSet).toHaveBeenCalled();
  });
});

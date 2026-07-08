import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import {
  TeamCollectionDrilldown,
  type TeamMemberRef,
} from "@/components/widgets/metric-views/team-collection-drilldown";
import type { MetricGroup } from "@/lib/insight/groups";
import { normalizeMetricResults } from "@/lib/metrics/collection";
import type { MetricCollectionResult } from "@/queries/metric-results";
import type { MetricResult } from "@/api/metric-results-client";

const DEF: MetricGroup = {
  kind: "metrics",
  id: "ai_adoption",
  title: "AI adoption",
  collection: {
    metrics: [
      { key: "ai.active_days", views: [{ view: "period" }] },
    ],
  },
  card: { preview: [] },
  drilldown: [],
};

const MEMBERS: TeamMemberRef[] = [
  { entityId: "a@x.com", displayName: "Ann" },
  { entityId: "b@x.com", displayName: "Bo" },
];

function metric(values: Array<{ id: string; value: number | null }>): MetricResult {
  return {
    metric_key: "ai.active_days",
    label: "Active AI days",
    unit: "days",
    format: "integer",
    direction: "higher_is_better",
    computation: "sum",
    views: [
      {
        view: "period",
        values: values.map((v) => ({ entity_id: v.id, value: v.value })),
      },
    ],
  };
}

function result(
  metrics: MetricResult[],
  overrides: Partial<MetricCollectionResult> = {},
): MetricCollectionResult {
  return {
    byKey: normalizeMetricResults(metrics),
    previousByKey: null,
    isPending: false,
    isFetching: false,
    isError: false,
    refetch: vi.fn(),
    ...overrides,
  };
}

describe("TeamCollectionDrilldown", () => {
  it("shows a spinner while pending", () => {
    const { container } = render(
      <TeamCollectionDrilldown
        def={DEF}
        data={result([], { isPending: true })}
        members={MEMBERS}
      />,
    );
    expect(container.querySelector("svg")).toBeInTheDocument();
  });

  it("shows an error with retry", () => {
    const refetch = vi.fn();
    render(
      <TeamCollectionDrilldown
        def={DEF}
        data={result([], { isError: true, refetch })}
        members={MEMBERS}
      />,
    );
    expect(screen.getByText("Unable to load metrics")).toBeInTheDocument();
  });

  it("shows the no-metrics message when the collection is empty", () => {
    render(
      <TeamCollectionDrilldown def={DEF} data={result([])} members={MEMBERS} />,
    );
    expect(
      screen.getByText(/No data for this group/i),
    ).toBeInTheDocument();
  });

  it("shows the no-members message when the roster is empty", () => {
    render(
      <TeamCollectionDrilldown
        def={DEF}
        data={result([metric([{ id: "a@x.com", value: 5 }])])}
        members={[]}
      />,
    );
    expect(screen.getByText(/No team members/i)).toBeInTheDocument();
  });

  it("renders a per-member table with formatted values and em-dash for gaps", () => {
    render(
      <TeamCollectionDrilldown
        def={DEF}
        data={result([
          metric([
            { id: "a@x.com", value: 5 },
            { id: "b@x.com", value: null },
          ]),
        ])}
        members={MEMBERS}
      />,
    );
    expect(screen.getByText("Ann")).toBeInTheDocument();
    expect(screen.getByText("Bo")).toBeInTheDocument();
    expect(screen.getByText("5 days")).toBeInTheDocument();
    expect(screen.getByText("—")).toBeInTheDocument();
  });
});

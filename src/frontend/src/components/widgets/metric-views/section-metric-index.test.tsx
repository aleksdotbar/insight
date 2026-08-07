// @vitest-environment jsdom
/**
 * The list that keeps a section honest about what it measures.
 *
 * Before it, a section showed three metrics closely and the other sixteen
 * were unreachable — so these tests are about completeness and about the two
 * ways a list of numbers stops being findable: repeating what is already on
 * screen, and being in no order at all.
 */
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import type { MetricResult } from "@/api/metric-results-client";
import { SectionMetricIndex } from "@/components/widgets/metric-views/section-metric-index";
import { normalizeMetricResults } from "@/lib/metrics/collection";

const ME = "019e27bc-dec0-7626-81a9-c5524662a6a9";

function metric(
  key: string,
  label: string,
  value: number | null
): MetricResult {
  return {
    metric_key: key,
    label,
    unit: null,
    format: "integer",
    computation: "sum",
    direction: "higher_is_better",
    views: [{ view: "period", values: [{ entity_id: ME, value }] }],
  } as MetricResult;
}

const RESULTS = [
  metric("collab.zeta", "Zeta", 3),
  metric("collab.alpha", "Alpha", 1),
  metric("collab.mid", "Mid", null),
];
const COLLECTION = {
  metrics: RESULTS.map((r) => ({ key: r.metric_key, views: [] })),
};

function renderIndex(shown: string[] = []) {
  return render(
    <SectionMetricIndex
      collection={COLLECTION}
      byKey={normalizeMetricResults(RESULTS)}
      entityId={ME}
      shown={new Set(shown)}
    />
  );
}

describe("SectionMetricIndex", () => {
  it("lists what the section measures in alphabetical order", () => {
    // The only thing done with this list is looking something up, and
    // declaration order is invisible to a reader.
    renderIndex();
    const names = screen.getAllByRole("term").map((n) => n.textContent);
    expect(names).toEqual(["Alpha", "Mid", "Zeta"]);
  });

  it("keeps a metric that holds nothing, as a dash", () => {
    // Dropping it would answer "is this measured at all?" with silence, which
    // reads as "no such metric" rather than "nothing for you this period".
    renderIndex();
    expect(screen.getByText("—")).toBeInTheDocument();
  });

  it("leaves out what the section already drew", () => {
    renderIndex(["collab.alpha"]);
    const names = screen.getAllByRole("term").map((n) => n.textContent);
    expect(names).toEqual(["Mid", "Zeta"]);
  });

  it("renders nothing when the section drew everything", () => {
    const { container } = renderIndex([
      "collab.alpha",
      "collab.mid",
      "collab.zeta",
    ]);
    expect(container).toBeEmptyDOMElement();
  });
});

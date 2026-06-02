/**
 * Component test pinning the wave-2 contract: when `useCatalog` reports a
 * column metric with `schema_status='error'`, the Executive View renders
 * that KPI's value WITHOUT good/warn coloring (Refs #79, ADR-002,
 * DESIGN §3.3).
 *
 * We mount `OrgKpiCards` rather than the full `<ExecutiveViewScreen>` —
 * the screen drags in the router / auth / sidebar tree, but the
 * threshold → coloring rule is entirely a property of the cards
 * component. The hook-level omission is covered separately in
 * `view-configs.test.tsx`; this test pins the rendered DOM consequence.
 */

import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { OrgKpiCards } from "@/components/widgets/org-kpi-cards";
import type { ExecColumnThreshold, ExecTeamRow, OrgKpis } from "@/types/insight";

const ORG_KPIS: OrgKpis = {
  avgBuildSuccess: 75,
  avgAiAdoption: 50,
  avgFocus: 55,
};

const TEAMS: ExecTeamRow[] = [];

describe("Executive view — schema_status='error' neutral coloring", () => {
  it("paints the value with text-foreground when its threshold is omitted (error row)", () => {
    // Simulate `useExecViewConfig` after dropping a schema_status='error'
    // row: build_success_pct is absent but the other rules stay.
    const thresholds: ExecColumnThreshold[] = [
      { metric_key: "focus_time_pct", threshold: 60 },
      { metric_key: "ai_adoption_pct", threshold: 60 },
    ];

    const { container } = render(
      <OrgKpiCards
        teams={TEAMS}
        orgKpis={ORG_KPIS}
        columnThresholds={thresholds}
      />,
    );

    // Find the "Avg Build Success" card and its value cell. The label
    // sits in the card; the value is the only `.text-2xl` descendant.
    const buildLabel = Array.from(
      container.querySelectorAll("div"),
    ).find((el) => el.textContent?.startsWith("Avg Build Success"));
    expect(buildLabel).toBeTruthy();

    const card = buildLabel!.closest("div[class*='text-center']")
      ?? buildLabel!.parentElement;
    const value = card!.querySelector(".text-2xl");
    expect(value).toBeTruthy();
    expect(value!.textContent).toContain("75");
    // Neutral color — NOT text-success / text-warning. This is the
    // wave-1 contract for `schema_status='error'`: label visible, value
    // visible, threshold-based coloring suppressed.
    expect(value!.className).toContain("text-foreground");
    expect(value!.className).not.toContain("text-success");
    expect(value!.className).not.toContain("text-warning");
  });

  it("paints the value with text-warning when its threshold IS present and value is below target", () => {
    // Sanity check the negative: with the threshold present, value=75 is
    // below the 90-target so the card SHOULD turn warn-colored. Pins the
    // distinction between "neutral due to error" and "neutral due to OK".
    const thresholds: ExecColumnThreshold[] = [
      { metric_key: "build_success_pct", threshold: 90 },
    ];

    const { container } = render(
      <OrgKpiCards
        teams={TEAMS}
        orgKpis={ORG_KPIS}
        columnThresholds={thresholds}
      />,
    );

    const buildLabel = Array.from(
      container.querySelectorAll("div"),
    ).find((el) => el.textContent?.startsWith("Avg Build Success"));
    const card = buildLabel!.closest("div[class*='text-center']")
      ?? buildLabel!.parentElement;
    const value = card!.querySelector(".text-2xl");
    expect(value!.className).toContain("text-warning");
  });
});

import { describe, expect, it } from "vitest";

import { computeDelta } from "@/lib/metrics/delta";

describe("computeDelta", () => {
  it("computes relative change for sums", () => {
    expect(computeDelta(120, 100, "sum", "integer")).toEqual({
      kind: "percent_change",
      value: 20,
    });
    expect(computeDelta(80, 100, "sum", "integer")).toEqual({
      kind: "percent_change",
      value: -20,
    });
  });

  it("computes percentage-point change for percent-formatted ratios", () => {
    // 77% acceptance vs 72% last period is +5 pp, not +6.9%.
    const delta = computeDelta(77.04, 72.04, "ratio", "percent");
    expect(delta?.kind).toBe("pp_change");
    expect(delta?.value).toBeCloseTo(5);
  });

  it("treats non-percent ratios as relative change", () => {
    expect(computeDelta(3, 2, "ratio", "decimal")).toEqual({
      kind: "percent_change",
      value: 50,
    });
  });

  it("returns null for missing values and zero baselines", () => {
    expect(computeDelta(null, 100, "sum", "integer")).toBeNull();
    expect(computeDelta(100, null, "sum", "integer")).toBeNull();
    expect(computeDelta(100, 0, "sum", "integer")).toBeNull();
    expect(computeDelta(Number.NaN, 100, "sum", "integer")).toBeNull();
  });

  it("uses the absolute baseline for negative previous values", () => {
    expect(computeDelta(-50, -100, "sum", "integer")).toEqual({
      kind: "percent_change",
      value: 50,
    });
  });
});

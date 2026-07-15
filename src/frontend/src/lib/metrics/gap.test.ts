import { describe, expect, it } from "vitest";

import { formatGapMagnitude } from "@/lib/metrics/gap";

describe("formatGapMagnitude", () => {
  it("renders percent metrics as points, never a relative percent or ratio", () => {
    // Focus 55% vs median 90%: the gap is 35 points below, not "-39%".
    expect(
      formatGapMagnitude({
        value: 55,
        median: 90,
        gapPct: (55 - 90) / 90,
        gapDelta: 55 - 90,
        format: "percent",
        unit: null,
      })
    ).toBe("-35.0 pp");
  });

  it("uses a multiple far above the median", () => {
    expect(
      formatGapMagnitude({
        value: 12424,
        median: 3563,
        gapPct: (12424 - 3563) / 3563,
        gapDelta: 12424 - 3563,
        format: "integer",
        unit: "lines",
      })
    ).toBe("3.5×");
  });

  it("uses a signed percent for sub-2x gaps", () => {
    expect(
      formatGapMagnitude({
        value: 3,
        median: 5,
        gapPct: (3 - 5) / 5,
        gapDelta: 3 - 5,
        format: "integer",
        unit: "tasks",
      })
    ).toBe("-40%");
  });

  it("falls back to a signed absolute delta when the median is ~0", () => {
    expect(
      formatGapMagnitude({
        value: 2,
        median: 0,
        gapPct: null,
        gapDelta: 2,
        format: "integer",
        unit: "lines",
      })
    ).toBe("+2 lines");
  });
});

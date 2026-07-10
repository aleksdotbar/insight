import { describe, expect, it } from "vitest";

import { integerPercentShares } from "@/lib/metrics/shares";

const sum = (xs: number[]) => xs.reduce((a, b) => a + b, 0);

describe("integerPercentShares", () => {
  it("sums to exactly 100 for equal thirds (no 99%)", () => {
    const shares = integerPercentShares([1, 1, 1]);
    expect(sum(shares)).toBe(100);
    expect(shares).toEqual([34, 33, 33]);
  });

  it("sums to exactly 100 for a lopsided split (no 101%)", () => {
    const shares = integerPercentShares([995, 5]);
    expect(sum(shares)).toBe(100);
  });

  it("keeps exact splits unchanged", () => {
    expect(integerPercentShares([80, 20])).toEqual([80, 20]);
    expect(integerPercentShares([60, 40])).toEqual([60, 40]);
  });

  it("returns zeros for a non-positive total", () => {
    expect(integerPercentShares([0, 0])).toEqual([0, 0]);
    expect(integerPercentShares([])).toEqual([]);
  });

  it("gives a single part the whole 100%", () => {
    expect(integerPercentShares([42])).toEqual([100]);
  });
});

/**
 * Pure-function tests for `evaluateStatus` and `teamHealthStatus`
 * (Refs #79).
 *
 * `evaluateStatus` lost its `METRIC_SEMANTICS`-lookup behavior in #79 and
 * is now a pure `(value, thresholds, higher_is_better)` function — tests
 * exercise both polarity directions across the good/warn/bad bands.
 *
 * `teamHealthStatus` keeps its member-scale fraction rule: a count of
 * affected members against team size, with 0 always reading as 'good'.
 */

import { describe, expect, it } from "vitest";

import { evaluateStatus, teamHealthStatus } from "./metric-semantics";

describe("evaluateStatus", () => {
  describe("higher_is_better=true", () => {
    const thresholds = { good: 90, warn: 80 };

    it("returns 'good' when value >= good", () => {
      expect(evaluateStatus(95, thresholds, true)).toBe("good");
      // Boundary: equal to `good` still counts as good (inclusive band).
      expect(evaluateStatus(90, thresholds, true)).toBe("good");
    });

    it("returns 'warn' when good > value >= warn", () => {
      expect(evaluateStatus(85, thresholds, true)).toBe("warn");
      // Boundary: equal to `warn` is the lower edge of the warn band.
      expect(evaluateStatus(80, thresholds, true)).toBe("warn");
    });

    it("returns 'bad' when value < warn", () => {
      expect(evaluateStatus(79, thresholds, true)).toBe("bad");
      expect(evaluateStatus(0, thresholds, true)).toBe("bad");
    });
  });

  describe("higher_is_better=false", () => {
    const thresholds = { good: 14, warn: 20 };

    it("returns 'good' when value <= good", () => {
      expect(evaluateStatus(10, thresholds, false)).toBe("good");
      expect(evaluateStatus(14, thresholds, false)).toBe("good");
    });

    it("returns 'warn' when good < value <= warn", () => {
      expect(evaluateStatus(15, thresholds, false)).toBe("warn");
      expect(evaluateStatus(20, thresholds, false)).toBe("warn");
    });

    it("returns 'bad' when value > warn", () => {
      expect(evaluateStatus(21, thresholds, false)).toBe("bad");
      expect(evaluateStatus(100, thresholds, false)).toBe("bad");
    });
  });

  it("is pure — ignores extra catalog fields when given a wider thresholds object", () => {
    // Confirms the signature accepts any `{ good, warn }` plus extras —
    // the catalog row from `useCatalog` carries `resolved_from`,
    // `bounded_by_lock`, etc., and feeding it directly MUST work.
    const catalogShape = {
      good: 90,
      warn: 80,
      alert_trigger: 90,
      alert_bad: 80,
      resolved_from: "tenant" as const,
      bounded_by_lock: false,
    };
    expect(evaluateStatus(95, catalogShape, true)).toBe("good");
  });
});

describe("teamHealthStatus", () => {
  it("returns 'good' for count=0 regardless of team size", () => {
    expect(teamHealthStatus(0, 1)).toBe("good");
    expect(teamHealthStatus(0, 1000)).toBe("good");
  });

  it("returns 'good' for an empty team (teamSize=0) — no scale to compute against", () => {
    expect(teamHealthStatus(5, 0)).toBe("good");
  });

  it("returns 'bad' at and above the 25% fraction", () => {
    // 25% — exact match to badPct: 0.25.
    expect(teamHealthStatus(3, 12)).toBe("bad");
    // 50% — well above.
    expect(teamHealthStatus(5, 10)).toBe("bad");
  });

  it("returns 'warn' in the 10%–25% range", () => {
    // 10% — exact match to warnPct: 0.10.
    expect(teamHealthStatus(1, 10)).toBe("warn");
    // 20% — clearly warn.
    expect(teamHealthStatus(4, 20)).toBe("warn");
  });

  it("returns 'good' below the warn fraction", () => {
    // 5% — below warnPct.
    expect(teamHealthStatus(1, 20)).toBe("good");
  });
});

import { describe, expect, it } from "vitest";

import {
  GROUPS,
  groupById,
  groupIdForMetricKey,
  legacyGroups,
  metricGroups,
} from "@/lib/insight/groups";

describe("groups registry", () => {
  it("groupById returns the def and throws on an unknown id", () => {
    expect(groupById("ai_adoption").id).toBe("ai_adoption");
    // @ts-expect-error — exercising the runtime guard with an invalid id.
    expect(() => groupById("does_not_exist")).toThrow(/Unknown group/);
  });

  it("partitions GROUPS by kind", () => {
    expect(legacyGroups().every((g) => g.kind === "legacy")).toBe(true);
    expect(metricGroups().every((g) => g.kind === "metrics")).toBe(true);
    expect(legacyGroups().length + metricGroups().length).toBe(GROUPS.length);
  });

  it("groupIdForMetricKey resolves a metric to its owning group, null otherwise", () => {
    expect(groupIdForMetricKey("ai.active_days")).toBe("ai_adoption");
    expect(groupIdForMetricKey("tasks_closed")).toBeNull();
    expect(groupIdForMetricKey("nope.unknown")).toBeNull();
  });
});

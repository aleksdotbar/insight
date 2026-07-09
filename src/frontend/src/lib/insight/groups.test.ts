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
    expect(groupIdForMetricKey("git.prs_merged")).toBe("git_output");
    expect(groupIdForMetricKey("git.pr_cycle_time_h")).toBe("git_output");
    expect(groupIdForMetricKey("tasks_closed")).toBeNull();
    expect(groupIdForMetricKey("nope.unknown")).toBeNull();
  });

  it("exposes git_output as a metrics group with a histogram drilldown block", () => {
    const git = groupById("git_output");
    expect(git.kind).toBe("metrics");
    if (git.kind === "metrics") {
      expect(git.collection.metrics.length).toBeGreaterThan(0);
      expect(git.drilldown.some((b) => b.view === "histogram")).toBe(true);
    }
  });
});

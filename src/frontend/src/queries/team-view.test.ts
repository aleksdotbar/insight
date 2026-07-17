import { describe, expect, it } from "vitest";

import { isTeamBulletSectionId } from "@/queries/team-view";

describe("isTeamBulletSectionId", () => {
  it("accepts ids backed by a team bullet section", () => {
    expect(isTeamBulletSectionId("collaboration")).toBe(true);
    expect(isTeamBulletSectionId("task_delivery")).toBe(true);
  });

  it("rejects ids with no team bullet section", () => {
    expect(isTeamBulletSectionId("wiki")).toBe(false);
    expect(isTeamBulletSectionId("nonsense")).toBe(false);
  });
});

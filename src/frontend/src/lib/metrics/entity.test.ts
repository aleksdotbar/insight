import { describe, expect, it } from "vitest";

import { normalizePersonId } from "@/lib/metrics/entity";

describe("normalizePersonId", () => {
  it("trims surrounding whitespace and lowercases", () => {
    expect(normalizePersonId("  Alice.Smith@Example.COM ")).toBe(
      "alice.smith@example.com",
    );
  });

  it("leaves an already-normalized id unchanged", () => {
    expect(normalizePersonId("bob@x.io")).toBe("bob@x.io");
  });
});

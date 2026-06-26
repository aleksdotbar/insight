import { describe, expect, it } from "vitest";

import { isSalesDepartment } from "./is-sales-department";

describe("isSalesDepartment", () => {
  it("matches sales departments (prod org)", () => {
    expect(isSalesDepartment("Sales")).toBe(true);
    expect(isSalesDepartment("Inside Sales")).toBe(true);
    expect(isSalesDepartment("sales")).toBe(true);
  });

  it("matches Global Services variants (dev org)", () => {
    // Real values observed in identity / bronze on insight-dev-vhc.
    expect(isSalesDepartment("Global Services")).toBe(true);
    expect(isSalesDepartment("Global Services Team")).toBe(true);
    expect(isSalesDepartment("Global services")).toBe(true);
    expect(isSalesDepartment("Global  Services Team")).toBe(true); // double space
    expect(isSalesDepartment("Global-Services")).toBe(true);
  });

  it("does not match unrelated departments", () => {
    expect(isSalesDepartment("R&D")).toBe(false);
    expect(isSalesDepartment("Marketing")).toBe(false);
    expect(isSalesDepartment("Professional Services")).toBe(false);
    expect(isSalesDepartment("Wholesaler")).toBe(false); // no word-boundary "sales"
  });

  it("handles null / undefined / empty", () => {
    expect(isSalesDepartment(null)).toBe(false);
    expect(isSalesDepartment(undefined)).toBe(false);
    expect(isSalesDepartment("")).toBe(false);
  });
});

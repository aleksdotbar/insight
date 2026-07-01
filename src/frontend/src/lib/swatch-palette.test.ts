import { describe, expect, it } from "vitest";

import { swatchPalette } from "@/lib/swatch-palette";

describe("swatchPalette", () => {
  it("is deterministic regardless of duplicate seed inputs", () => {
    expect(swatchPalette(["cursor", "claude_code", "cursor"])).toEqual(
      swatchPalette(["claude_code", "cursor"]),
    );
  });

  it("returns one color per unique seed", () => {
    expect(Object.keys(swatchPalette(["cursor", "cursor", "codex"])).sort()).toEqual(
      ["codex", "cursor"],
    );
  });

  it("uses the swatch oklch token shape", () => {
    const palette = swatchPalette(["cursor", "claude_code"]);

    expect(palette.cursor).toMatch(/^oklch\(var\(--swatch-l\) 0\.14 \d+\)$/);
    expect(palette.claude_code).toMatch(/^oklch\(var\(--swatch-l\) 0\.14 \d+\)$/);
  });
});

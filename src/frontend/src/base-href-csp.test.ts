// The index.html <base>-injecting inline script must stay byte-identical to
// the sha256 the nginx CSP allows: a reformat breaks the hash, CSP blocks the
// script, and prefix serving (/exp/<name>/) silently dies.
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

const root = join(__dirname, "..");

describe("base-href bootstrap script", () => {
  const html = readFileSync(join(root, "index.html"), "utf8");
  const script = /<script>([\s\S]*?)<\/script>/.exec(html)?.[1];

  it("exists as the first head element", () => {
    expect(script).toBeTruthy();
    const head = html.indexOf("<head>");
    expect(html.indexOf("<script>")).toBeGreaterThan(head);
    expect(html.indexOf("<script>")).toBeLessThan(html.indexOf("<link"));
  });

  it("hash matches the nginx CSP allowance", () => {
    const digest = createHash("sha256")
      .update(script ?? "")
      .digest("base64");
    const nginx = readFileSync(
      join(root, "nginx/default.conf.template"),
      "utf8"
    );
    expect(nginx).toContain(`'sha256-${digest}'`);
  });

  it("prefix regex accepts experiment slugs and rejects lookalikes", () => {
    const re = /^\/exp\/[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?=\/|$)/;
    expect("/exp/demo/".match(re)?.[0]).toBe("/exp/demo");
    expect("/exp/widget-alpha/deep/route".match(re)?.[0]).toBe(
      "/exp/widget-alpha"
    );
    expect("/exp/a".match(re)?.[0]).toBe("/exp/a");
    for (const miss of ["/", "/expunge/x", "/exp/", "/exp/-bad", "/EXP/demo"]) {
      expect(miss.match(re), `should reject: ${miss}`).toBeNull();
    }
  });
});

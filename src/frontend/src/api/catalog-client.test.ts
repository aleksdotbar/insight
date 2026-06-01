/**
 * Wire-shape unit tests for the catalog client.
 *
 * The backend response is `metric_key`-required (ADR-002) and `links`-required
 * (ADR-003), but the FE parses both as optional so a deployed env that
 * predates the amendments still hydrates cleanly. These tests pin both
 * directions: present and absent.
 */

import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";

import { CatalogApiError, fetchCatalog } from "./catalog-client";

const ENDPOINT = "/api/analytics/v1/catalog/get_metrics";

function jsonResponse(body: unknown, init: ResponseInit = {}): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "Content-Type": "application/json" },
    ...init,
  });
}

describe("fetchCatalog", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn());
  });
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("issues POST application/json against /catalog/get_metrics with the body", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      jsonResponse({
        tenant_id: "t",
        generated_at: "2026-06-01T00:00:00Z",
        metrics: [],
        links: [],
      }),
    );
    await fetchCatalog({ role_slug: "eng", team_id: "alpha" });
    expect(globalThis.fetch).toHaveBeenCalledTimes(1);
    const call = (globalThis.fetch as ReturnType<typeof vi.fn>).mock.calls[0];
    const [url, init] = call as [string, RequestInit];
    expect(url).toBe(ENDPOINT);
    expect(init.method).toBe("POST");
    const headers = new Headers(init.headers);
    expect(headers.get("Content-Type")).toBe("application/json");
    expect(JSON.parse(init.body as string)).toEqual({
      role_slug: "eng",
      team_id: "alpha",
    });
  });

  it("parses a wire response that carries metric_key + links (post-ADR-002/003)", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      jsonResponse({
        tenant_id: "t",
        generated_at: "2026-06-01T00:00:00Z",
        metrics: [
          {
            id: "id-1",
            metric_key: "ic_kpis.tasks_closed",
            label: "Tasks Closed",
            higher_is_better: true,
            is_member_scale: false,
            source_tags: ["jira"],
            schema_status: "ok",
            thresholds: {
              good: 5,
              warn: 3,
              resolved_from: "product-default",
              bounded_by_lock: false,
            },
          },
        ],
        links: [{ query_id: "q-1", catalog_metric_ids: ["id-1"] }],
      }),
    );
    const res = await fetchCatalog();
    expect(res.metrics[0]?.metric_key).toBe("ic_kpis.tasks_closed");
    expect(res.links).toEqual([
      { query_id: "q-1", catalog_metric_ids: ["id-1"] },
    ]);
  });

  it("tolerates a pre-ADR-002 response that omits metric_key (defensive parse)", async () => {
    // A deployed env that hasn't shipped ADR-002 yet returns metric_key
    // omitted. The FE MUST hydrate cleanly — no parse error, no thrown
    // exception. Consumers fall back to `byId` (which is the contract).
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      jsonResponse({
        tenant_id: "t",
        generated_at: "2026-06-01T00:00:00Z",
        metrics: [
          {
            id: "id-1",
            label: "Tasks Closed",
            higher_is_better: true,
            is_member_scale: false,
            source_tags: [],
            schema_status: "ok",
            thresholds: {
              good: 5,
              warn: 3,
              resolved_from: "product-default",
              bounded_by_lock: false,
            },
          },
        ],
      }),
    );
    const res = await fetchCatalog();
    expect(res.metrics[0]?.metric_key).toBeUndefined();
    // links can be absent on pre-ADR-003 backends; the type is optional.
    expect(res.links).toBeUndefined();
  });

  it("throws CatalogApiError on non-2xx", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      jsonResponse(
        { type: "gts://gts.cf.core.errors.err.v1~cf.core.err.internal.v1~" },
        { status: 500 },
      ),
    );
    await expect(fetchCatalog()).rejects.toBeInstanceOf(CatalogApiError);
  });
});

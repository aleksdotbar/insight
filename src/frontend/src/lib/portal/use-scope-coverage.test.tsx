/**
 * Coverage for everyone the viewer may see.
 *
 * Two of these tests are about the request rather than the answer, because
 * both ways this hook can go wrong are in the request: asking for an id the
 * viewer may not see refuses the whole screen, and asking for a view that
 * cannot be chunked breaks it at roster scale. Neither failure is visible in
 * the returned shape, so neither would be caught by testing the output.
 */
import { renderHook } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { MetricCollectionConfig } from "@/lib/metrics/collection";

const state = vi.hoisted(() => ({
  tree: null as unknown,
  definitions: undefined as unknown,
  collectionSet: new Map<string, unknown>(),
  lastCall: null as {
    collections: readonly { key: string; collection: MetricCollectionConfig }[];
    entity: { type: string; ids: string[] };
  } | null,
}));

vi.mock("@/auth", () => ({ useViewer: () => ({ personId: "viewer-1" }) }));
vi.mock("@/hooks/use-portal-period", () => ({
  usePortalPeriod: () => ({
    dateRange: { from: "2026-03-01", to: "2026-03-31" },
  }),
}));
vi.mock("@/queries/ic-dashboard", () => ({
  useIcPerson: () => ({ data: state.tree }),
}));
vi.mock("@/queries/metric-definitions", () => ({
  useMetricDefinitionsResponse: () => ({
    data: state.definitions,
    isPending: false,
  }),
}));
vi.mock("@/queries/metric-results", () => ({
  useMetricCollectionSet: (
    collections: readonly {
      key: string;
      collection: MetricCollectionConfig;
    }[],
    entity: { type: string; ids: string[] },
  ) => {
    state.lastCall = { collections, entity };
    return state.collectionSet;
  },
}));

import { GROUPS } from "@/lib/insight/groups";
import { useScopeCoverage } from "./use-scope-coverage";

/** A person tree node as identity serves it. */
function node(person_id: string, subordinates: unknown[] = []): unknown {
  return { person_id, subordinates };
}

beforeEach(() => {
  state.tree = node("viewer-1", [node("a-1"), node("a-2", [node("a-3")])]);
  state.definitions = { metrics: [] };
  state.collectionSet = new Map();
  state.lastCall = null;
});

describe("useScopeCoverage", () => {
  it("asks for exactly the roster it was served, and nothing more", () => {
    // The visibility check on the metrics endpoint is all-or-nothing: a single
    // id the caller may not see refuses the entire request rather than
    // filtering it, and does not say which id was at fault. Widening or
    // guessing the list therefore empties the screen with no diagnosis.
    renderHook(() => useScopeCoverage());

    expect(state.lastCall?.entity.type).toBe("person");
    expect([...(state.lastCall?.entity.ids ?? [])].sort()).toEqual([
      "a-1",
      "a-2",
      "a-3",
      "viewer-1",
    ]);
  });

  it("requests only the period view, so the roster can still be chunked", () => {
    // `entityChunkSize` refuses to chunk a collection carrying timeseries,
    // breakdown or histogram views, and an unchunked roster-sized request runs
    // into the backend's projected-row limit. Asking for one view keeps the
    // existing chunk-and-merge path available.
    renderHook(() => useScopeCoverage());

    const views = (state.lastCall?.collections ?? []).flatMap((c) =>
      c.collection.metrics.flatMap((m) => m.views.map((v) => v.view)),
    );
    expect(views.length).toBeGreaterThan(0);
    expect([...new Set(views)]).toEqual(["period"]);
  });

  it("counts every person in the roster, including the viewer", () => {
    const { result } = renderHook(() => useScopeCoverage());
    expect(result.current.distribution.counted).toBe(4);
    expect(result.current.people).toHaveLength(4);
  });

  it("stays closed while the roster has not resolved", () => {
    // An empty id list is refused by the client rather than sent, so the hook
    // must not reach that path at all before identity answers.
    state.tree = null;
    renderHook(() => useScopeCoverage());
    expect(state.lastCall?.entity.ids).toEqual([]);
    expect(state.lastCall?.collections).toEqual([]);
  });

  it("reports every part as unreachable when nothing has ever observed", () => {
    // No definition has observed anything, so no part can be claimed to reach
    // us — and this is read from the listing, never from the roster's nulls.
    const { result } = renderHook(() => useScopeCoverage());
    expect(result.current.unreachable).toHaveLength(GROUPS.length);
    expect(result.current.distribution.byLevel.get(0)).toBe(4);
  });
});

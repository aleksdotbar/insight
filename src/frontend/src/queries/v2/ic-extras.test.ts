import { beforeEach, describe, expect, it, vi } from "vitest";

import { METRIC_REGISTRY } from "@/api/metric-registry";
import type { DateRange } from "@/api/period-to-date-range";

const analytics = vi.hoisted(() => ({
  queryMetric: vi.fn(),
}));

const reactQuery = vi.hoisted(() => ({
  useQuery: vi.fn(),
}));

vi.mock("@tanstack/react-query", async () => {
  const actual = await vi.importActual<typeof import("@tanstack/react-query")>(
    "@tanstack/react-query",
  );
  return {
    ...actual,
    useQuery: reactQuery.useQuery,
  };
});

vi.mock("@/api/analytics-client", async () => {
  const actual = await vi.importActual<typeof import("@/api/analytics-client")>(
    "@/api/analytics-client",
  );
  return {
    ...actual,
    queryMetric: analytics.queryMetric,
  };
});

import {
  icAiPeerCountersQueryOptions,
  icAiToolSummaryQueryOptions,
  icAiToolTrendQueryOptions,
  useIcAiPeerCounters,
  useIcAiToolSummary,
  useIcAiToolTrend,
} from "@/queries/v2/ic-extras";

const RANGE: DateRange = { from: "2026-04-01", to: "2026-04-30" };

describe("AI query options", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it.each([
    [
      "summary",
      icAiToolSummaryQueryOptions,
      "ic-ai-tool-summary",
    ],
    [
      "trend",
      icAiToolTrendQueryOptions,
      "ic-ai-tool-trend",
    ],
    [
      "peer counters",
      icAiPeerCountersQueryOptions,
      "ic-ai-peer-counters",
    ],
  ])("builds stable %s query keys", (_, builder, keyPart) => {
    expect(builder("Dev@Example.com", RANGE).queryKey).toEqual([
      "v2",
      keyPart,
      "Dev@Example.com",
      "2026-04-01",
      "2026-04-30",
    ]);
  });

  it.each([
    ["", { from: "2026-04-01", to: "2026-04-30" }],
    ["dev@example.com", { from: "", to: "2026-04-30" }],
    ["dev@example.com", { from: "2026-04-01", to: "" }],
  ])("disables summary queries for incomplete inputs", (personId, range) => {
    expect(icAiToolSummaryQueryOptions(personId, range).enabled).toBe(false);
  });

  it.each([
    [
      "summary",
      icAiToolSummaryQueryOptions,
      METRIC_REGISTRY.V2_IC_AI_TOOL_SUMMARY,
    ],
    ["trend", icAiToolTrendQueryOptions, METRIC_REGISTRY.V2_IC_AI_TOOL_TREND],
    [
      "peer counters",
      icAiPeerCountersQueryOptions,
      METRIC_REGISTRY.V2_IC_AI_PEER_COUNTERS,
    ],
  ])("lowercases and OData-escapes %s person filters", async (_, builder, queryId) => {
    analytics.queryMetric.mockResolvedValueOnce({ items: [] });

    await builder("O'Hara@Example.COM", RANGE).queryFn();

    expect(analytics.queryMetric).toHaveBeenCalledWith(queryId, RANGE, {
      $filter: "person_id eq 'o''hara@example.com'",
    });
  });

  it.each([
    [useIcAiToolSummary, "ic-ai-tool-summary"],
    [useIcAiToolTrend, "ic-ai-tool-trend"],
    [useIcAiPeerCounters, "ic-ai-peer-counters"],
  ])("passes %s options to useQuery", (hook, keyPart) => {
    reactQuery.useQuery.mockReturnValue({});

    hook("Dev@Example.com", RANGE);

    expect(reactQuery.useQuery).toHaveBeenCalledWith(
      expect.objectContaining({
        queryKey: [
          "v2",
          keyPart,
          "Dev@Example.com",
          "2026-04-01",
          "2026-04-30",
        ],
      }),
    );
  });
});

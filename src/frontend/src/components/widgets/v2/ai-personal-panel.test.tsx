import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { CatalogMetric } from "@/api/catalog-client";
import type { DateRange } from "@/api/period-to-date-range";
import { AiPersonalPanel } from "@/components/widgets/v2/ai-personal-panel";
import type {
  AiPeerCounterRow,
  AiToolSummaryRow,
  AiToolTrendRow,
} from "@/queries/v2/ic-extras";

const mocks = vi.hoisted(() => ({
  useCatalog: vi.fn(),
  useIcAiPeerCounters: vi.fn(),
  useIcAiToolSummary: vi.fn(),
  useIcAiToolTrend: vi.fn(),
}));

vi.mock("@/api/use-catalog", () => ({
  useCatalog: mocks.useCatalog,
}));

vi.mock("@/queries/v2/ic-extras", async () => {
  const actual = await vi.importActual<typeof import("@/queries/v2/ic-extras")>(
    "@/queries/v2/ic-extras",
  );
  return {
    ...actual,
    useIcAiPeerCounters: mocks.useIcAiPeerCounters,
    useIcAiToolSummary: mocks.useIcAiToolSummary,
    useIcAiToolTrend: mocks.useIcAiToolTrend,
  };
});

vi.mock("@/hooks/use-settings", () => ({
  useSettings: () => ({ focusMode: "all" }),
}));

const RANGE: DateRange = { from: "2026-04-01", to: "2026-04-07" };

function queryState<T>(
  data: T,
  overrides: Partial<{
    data: T;
    isPending: boolean;
    isError: boolean;
    refetch: () => void;
  }> = {},
) {
  return {
    data,
    isPending: false,
    isError: false,
    refetch: vi.fn(),
    ...overrides,
  };
}

function catalogMetric(
  metric_key: string,
  label: string,
  overrides: Partial<CatalogMetric> = {},
): CatalogMetric {
  return {
    id: metric_key,
    metric_key,
    label,
    higher_is_better: true,
    is_member_scale: false,
    source_tags: [],
    schema_status: "ok",
    thresholds: {
      good: 1,
      warn: 0,
      resolved_from: "product-default",
      bounded_by_lock: false,
    },
    ...overrides,
  };
}

function mockCatalog(metrics: CatalogMetric[]) {
  const byKey = new Map(metrics.map((metric) => [metric.metric_key, metric]));
  mocks.useCatalog.mockReturnValue({
    byMetricKey: (metricKey: string) => byKey.get(metricKey),
    isLoading: false,
    isError: false,
    refetch: vi.fn(),
  });
}

function peerCounter(
  metric_key: string,
  value: number,
  overrides: Partial<AiPeerCounterRow> = {},
): AiPeerCounterRow {
  return {
    person_id: "dev@example.com",
    org_unit_id: "team-1",
    metric_key,
    value,
    median: 50,
    p25: 25,
    p75: 75,
    n: 10,
    range_min: 0,
    range_max: 100,
    ...overrides,
  };
}

function setPanelData({
  summary = [],
  trend = [],
  counters = [],
}: {
  summary?: AiToolSummaryRow[];
  trend?: AiToolTrendRow[];
  counters?: AiPeerCounterRow[];
} = {}) {
  mocks.useIcAiToolSummary.mockReturnValue(queryState(summary));
  mocks.useIcAiToolTrend.mockReturnValue(queryState(trend));
  mocks.useIcAiPeerCounters.mockReturnValue(queryState(counters));
}

describe("<AiPersonalPanel>", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockCatalog([
      catalogMetric("ai_person_counter_daily.ai_accepted_lines", "AI-added lines", {
        sublabel: "Accepted added coding output",
        unit: "lines",
      }),
      catalogMetric("ai_person_counter_daily.ai_active_days", "AI active days", {
        unit: "days",
      }),
      catalogMetric("ai_person_counter_daily.ai_removed_lines", "AI-removed lines", {
        sublabel: "Accepted deleted coding output",
        unit: "lines",
      }),
    ]);
    setPanelData();
  });

  it("renders nothing without a person or range", () => {
    const { container, rerender } = render(
      <AiPersonalPanel personId={null} range={RANGE} />,
    );

    expect(container).toBeEmptyDOMElement();

    rerender(<AiPersonalPanel personId="dev@example.com" range={null} />);

    expect(container).toBeEmptyDOMElement();
  });

  it("renders share chart rows sorted by accepted lines", () => {
    setPanelData({
      summary: [
        {
          person_id: "dev@example.com",
          tool: "claude_code",
          tool_name: "Claude Code",
          accepted_lines_added: 100,
          accepted_lines_removed: 0,
          cost_cents: null,
          active_days: 2,
        },
        {
          person_id: "dev@example.com",
          tool: "cursor",
          tool_name: "Cursor",
          accepted_lines_added: 250,
          accepted_lines_removed: 0,
          cost_cents: null,
          active_days: 3,
        },
        {
          person_id: "dev@example.com",
          tool: "codex",
          tool_name: "Codex",
          accepted_lines_added: 0,
          accepted_lines_removed: 0,
          cost_cents: null,
          active_days: 1,
        },
      ],
    });

    render(<AiPersonalPanel personId="dev@example.com" range={RANGE} />);

    const cursorSegment = screen.getByTitle("Cursor: 250 lines");
    const claudeSegment = screen.getByTitle("Claude Code: 100 lines");
    expect(
      cursorSegment.compareDocumentPosition(claudeSegment) &
        Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
    expect(screen.queryByTitle("Codex: 0 lines")).not.toBeInTheDocument();
    expect(screen.getByText("Period total by tool")).toBeInTheDocument();
  });

  it("renders trend chart and catalog-backed peer counters while ignoring unknown metric keys", () => {
    setPanelData({
      summary: [
        {
          person_id: "dev@example.com",
          tool: "cursor",
          tool_name: "Cursor",
          accepted_lines_added: 250,
          accepted_lines_removed: 0,
          cost_cents: null,
          active_days: 3,
        },
      ],
      trend: [
        {
          person_id: "dev@example.com",
          metric_date: "2026-04-02",
          tool: "cursor",
          tool_name: "Cursor",
          accepted_lines_added: 125,
        },
      ],
      counters: [
        peerCounter("ai_person_counter_daily.ai_accepted_lines", 95),
        peerCounter("ai_person_counter_daily.ai_removed_lines", 90),
        peerCounter("unknown.metric", 95),
      ],
    });

    render(<AiPersonalPanel personId="dev@example.com" range={RANGE} />);

    expect(screen.getByText("AI-added lines over time")).toBeInTheDocument();
    expect(screen.getByText("Daily by tool")).toBeInTheDocument();
    expect(screen.getByText("Top win")).toBeInTheDocument();
    expect(screen.getAllByText("AI-added lines")).toHaveLength(2);
    expect(screen.getByText("AI-removed lines")).toBeInTheDocument();
    expect(screen.queryByText("unknown.metric")).not.toBeInTheDocument();
  });

  it("renders loading and error states for independent sections", () => {
    mocks.useIcAiToolSummary.mockReturnValue(
      queryState([], { isPending: true }),
    );
    mocks.useIcAiToolTrend.mockReturnValue(
      queryState([], { isError: true }),
    );
    mocks.useIcAiPeerCounters.mockReturnValue(
      queryState([], { isError: true }),
    );

    const { container } = render(
      <AiPersonalPanel personId="dev@example.com" range={RANGE} />,
    );

    expect(container.querySelectorAll('[data-slot="skeleton"]')).toHaveLength(1);
    expect(screen.getByText("AI-added lines over time — unable to load")).toBeInTheDocument();
    expect(screen.getByText("AI counters — unable to load")).toBeInTheDocument();
  });
});

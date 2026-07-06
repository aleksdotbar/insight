import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { CatalogMetric } from "@/api/catalog-client";
import type { DateRange } from "@/api/period-to-date-range";
import { CollabMessagingPanel } from "@/components/widgets/v2/collab-messaging-panel";
import type { AiPeerCounterRow } from "@/queries/v2/ic-extras";

const mocks = vi.hoisted(() => ({
  useCatalog: vi.fn(),
  useIcCollabPeerCounters: vi.fn(),
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
    useIcCollabPeerCounters: mocks.useIcCollabPeerCounters,
  };
});

vi.mock("@/hooks/use-settings", () => ({
  useSettings: () => ({ focusMode: "all" }),
}));

const RANGE: DateRange = { from: "2026-04-01", to: "2026-04-07" };

function queryState<T>(data: T, overrides: Partial<Record<string, unknown>> = {}) {
  return { data, isPending: false, isError: false, refetch: vi.fn(), ...overrides };
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
  const byKey = new Map(metrics.map((m) => [m.metric_key, m]));
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
    org_unit_id: "Engineering",
    metric_key,
    value,
    median: 30,
    p25: 20,
    p75: 40,
    n: 5,
    range_min: 10,
    range_max: 50,
    ...overrides,
  };
}

describe("<CollabMessagingPanel>", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockCatalog([
      catalogMetric(
        "collab_person_counter_daily.messages_sent",
        "Messages sent",
        { unit: "messages", sublabel: "Chat messages across sources" },
      ),
      catalogMetric(
        "collab_person_counter_daily.channel_posts",
        "Channel posts",
        { unit: "messages", sublabel: "Channel posts + replies" },
      ),
    ]);
    mocks.useIcCollabPeerCounters.mockReturnValue(queryState([]));
  });

  it("renders nothing without a person or range", () => {
    const { container, rerender } = render(
      <CollabMessagingPanel personId={null} range={RANGE} />,
    );
    expect(container).toBeEmptyDOMElement();

    rerender(<CollabMessagingPanel personId="dev@example.com" range={null} />);
    expect(container).toBeEmptyDOMElement();
  });

  it("renders both Messaging counters under the Messaging heading", () => {
    mocks.useIcCollabPeerCounters.mockReturnValue(
      queryState([
        peerCounter("collab_person_counter_daily.messages_sent", 30),
        peerCounter("collab_person_counter_daily.channel_posts", 6, {
          median: 6,
          p25: 4,
          p75: 8,
          range_min: 2,
          range_max: 10,
        }),
      ]),
    );

    render(<CollabMessagingPanel personId="dev@example.com" range={RANGE} />);

    expect(screen.getByText("Messaging")).toBeInTheDocument();
    expect(screen.getByText("Messages sent")).toBeInTheDocument();
    expect(screen.getByText("Channel posts")).toBeInTheDocument();
  });

  it("omits counters whose metric_key is not in the catalog", () => {
    mocks.useIcCollabPeerCounters.mockReturnValue(
      queryState([
        peerCounter("collab_person_counter_daily.messages_sent", 30),
        peerCounter("collab_person_counter_daily.unknown_key", 99),
      ]),
    );

    render(<CollabMessagingPanel personId="dev@example.com" range={RANGE} />);

    expect(screen.getByText("Messages sent")).toBeInTheDocument();
    expect(screen.queryByText("collab_person_counter_daily.unknown_key")).not.toBeInTheDocument();
  });
});

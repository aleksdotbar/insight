/**
 * Story + browser component test for <CollabMessagingPanel> (#1527).
 *
 * Mirrors the KpiTile PoC: the story is tagged `["test"]` so it runs as a real-
 * browser test via @storybook/addon-vitest, with BOTH wire calls mocked over
 * MSW — the catalog (`/catalog/get_metrics`) and the collab peer-counter query
 * (`/metrics/{…0053}/query`). Asserts the two Messaging counters render under
 * the "Messaging" heading, driven entirely by the catalog rows.
 *
 * See docs/testing/storybook-component-tests.md.
 */

import type { Meta, StoryObj } from "@storybook/react-vite";
import { http, HttpResponse } from "msw";
import { expect, waitFor } from "storybook/test";

import type { CatalogResponse } from "@/api/catalog-client";
import { METRIC_REGISTRY } from "@/api/metric-registry";
import { authStore } from "@/auth/auth-store";

import { CollabMessagingPanel } from "./collab-messaging-panel";

const TENANT = "t-1";

const catalog: CatalogResponse = {
  tenant_id: TENANT,
  generated_at: "2026-06-01T00:00:00Z",
  metrics: [
    {
      id: "id-msg",
      metric_key: "collab_person_counter_daily.messages_sent",
      label: "Messages sent",
      sublabel: "Chat messages across sources",
      unit: "messages",
      higher_is_better: true,
      is_member_scale: false,
      source_tags: ["m365", "slack", "zulip"],
      schema_status: "ok",
      thresholds: {
        good: 200,
        warn: 50,
        resolved_from: "product-default",
        bounded_by_lock: false,
      },
    },
    {
      id: "id-chan",
      metric_key: "collab_person_counter_daily.channel_posts",
      label: "Channel posts",
      sublabel: "Channel posts + replies",
      unit: "messages",
      higher_is_better: true,
      is_member_scale: false,
      source_tags: ["m365", "slack"],
      schema_status: "ok",
      thresholds: {
        good: 20,
        warn: 5,
        resolved_from: "product-default",
        bounded_by_lock: false,
      },
    },
  ],
  links: [],
};

const counterRows = [
  {
    person_id: "dev@example.com",
    org_unit_id: "Engineering",
    metric_key: "collab_person_counter_daily.messages_sent",
    value: 30,
    median: 30,
    p25: 20,
    p75: 40,
    n: 5,
    range_min: 10,
    range_max: 50,
  },
  {
    person_id: "dev@example.com",
    org_unit_id: "Engineering",
    metric_key: "collab_person_counter_daily.channel_posts",
    value: 6,
    median: 6,
    p25: 4,
    p75: 8,
    n: 5,
    range_min: 2,
    range_max: 10,
  },
];

const handlers = [
  http.post("*/catalog/get_metrics", () => HttpResponse.json(catalog)),
  http.post(`*/metrics/${METRIC_REGISTRY.V2_IC_COLLAB_PEER_COUNTERS}/query`, () =>
    HttpResponse.json({ items: counterRows }),
  ),
];

const meta: Meta<typeof CollabMessagingPanel> = {
  title: "Widgets/v2/CollabMessagingPanel",
  component: CollabMessagingPanel,
  args: {
    personId: "dev@example.com",
    range: { from: "2026-04-01", to: "2026-04-30" },
  },
  decorators: [
    (Story) => (
      <div className="w-96">
        <Story />
      </div>
    ),
  ],
  beforeEach: () => {
    authStore.setTenantId(TENANT);
  },
};
export default meta;

type Story = StoryObj<typeof CollabMessagingPanel>;

/** Demo story for the Storybook UI (not a test — no `test` tag). */
export const Default: Story = {
  parameters: { msw: { handlers } },
};

/** Component test: both Messaging counters render under the "Messaging" card. */
export const TestMessagingCounters: Story = {
  tags: ["test"],
  parameters: { msw: { handlers } },
  play: async ({ canvas }) => {
    await expect(canvas.getByText("Messaging")).toBeInTheDocument();
    // Both counters render once the (mocked) counter query + catalog resolve.
    await waitFor(() =>
      expect(canvas.getByText("Messages sent")).toBeInTheDocument(),
    );
    await expect(canvas.getByText("Channel posts")).toBeInTheDocument();
  },
};

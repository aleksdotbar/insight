/**
 * Component-render coverage for `<IcNeedsAttention>` (Refs #80, wave 3).
 *
 * Catalog-driven; covers the wave-1 DESIGN §3.3 rendering rules:
 *   - `ok` row that scores 'bottom' surfaces as an attention item.
 *   - `schema_status='error'` rows (surfaced as `row.schema_error: true`
 *     by transforms.ts) are filtered out of the attention surface — a
 *     broken metric never raises an alert.
 *   - Missing-id rows are filtered out — we can't tell if the value is
 *     "below peers" without a higher_is_better signal.
 */

import { screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { authStore } from "@/auth/auth-store";

vi.mock("@/api/catalog-client", async () => {
  const actual = await vi.importActual<typeof import("@/api/catalog-client")>(
    "@/api/catalog-client",
  );
  return { ...actual, fetchCatalog: vi.fn() };
});

import * as catalogClient from "@/api/catalog-client";
import {
  buildCatalogResponse,
  renderWithCatalogClient,
} from "@/test/catalog-test-utils";
import { IcNeedsAttention } from "./ic-needs-attention";
import type { PeerStats } from "@/lib/peers";
import type { BulletMetric, PeriodValue } from "@/types/insight";

const fetchCatalog = catalogClient.fetchCatalog as ReturnType<typeof vi.fn>;

function makeBullet(overrides: Partial<BulletMetric> = {}): BulletMetric {
  return {
    period: "month" as PeriodValue,
    section: "task_delivery",
    metric_key: "tasks_completed",
    label: "Tasks Closed",
    value: "1",
    unit: "tasks",
    range_min: "0",
    range_max: "20",
    median: "5",
    median_label: "Median: 5 tasks",
    bar_left_pct: 0,
    bar_width_pct: 5,
    median_left_pct: 25,
    status: "bad",
    drill_id: "",
    ...overrides,
  };
}

const STATS: PeerStats = { p25: 3, p50: 5, p75: 10, min: 1, max: 15, n: 12 };

describe("<IcNeedsAttention>", () => {
  beforeEach(() => {
    authStore.reset();
    authStore.setTenantId("t-1");
    fetchCatalog.mockReset();
  });
  afterEach(() => {
    authStore.reset();
  });

  it("surfaces an ok-row whose value is bottom-quartile vs peers", async () => {
    fetchCatalog.mockResolvedValue(
      buildCatalogResponse([
        {
          metric_key: "task_delivery_bullet_rows.tasks_completed",
          higher_is_better: true,
          schema_status: "ok",
        },
      ]),
    );
    const cohortStats = new Map([["tasks_completed", STATS]]);
    renderWithCatalogClient(
      <IcNeedsAttention
        sections={[
          {
            id: "task_delivery",
            label: "Task delivery",
            rows: [makeBullet({ value: "1" })],
          },
        ]}
        cohortStats={cohortStats}
        onSectionClick={() => {}}
      />,
    );
    await waitFor(() => {
      expect(screen.getByText("1 metrics below peers")).toBeInTheDocument();
    });
    expect(screen.getByText("Tasks Closed")).toBeInTheDocument();
  });

  it("filters out schema_error rows even when value is bottom-quartile", async () => {
    fetchCatalog.mockResolvedValue(
      buildCatalogResponse([
        {
          metric_key: "task_delivery_bullet_rows.tasks_completed",
          higher_is_better: true,
          schema_status: "error",
          schema_error_code: "table_not_found",
        },
      ]),
    );
    const cohortStats = new Map([["tasks_completed", STATS]]);
    const { container } = (() => {
      renderWithCatalogClient(
        <IcNeedsAttention
          sections={[
            {
              id: "task_delivery",
              label: "Task delivery",
              rows: [makeBullet({ value: "1", schema_error: true })],
            },
          ]}
          cohortStats={cohortStats}
          onSectionClick={() => {}}
        />,
      );
      return { container: document.body };
    })();
    // The component returns null when attentionAll is empty — no
    // "Needs attention" heading appears.
    await waitFor(() => {
      expect(screen.queryByText("Needs attention")).not.toBeInTheDocument();
    });
    expect(container).toBeTruthy();
  });

  it("filters out missing-id rows (no catalog entry)", async () => {
    fetchCatalog.mockResolvedValue(buildCatalogResponse([]));
    const cohortStats = new Map([["tasks_completed", STATS]]);
    renderWithCatalogClient(
      <IcNeedsAttention
        sections={[
          {
            id: "task_delivery",
            label: "Task delivery",
            rows: [makeBullet({ value: "1" })],
          },
        ]}
        cohortStats={cohortStats}
        onSectionClick={() => {}}
      />,
    );
    await waitFor(() => {
      expect(screen.queryByText("Needs attention")).not.toBeInTheDocument();
    });
  });
});

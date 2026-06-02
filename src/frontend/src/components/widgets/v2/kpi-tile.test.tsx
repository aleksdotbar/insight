/**
 * Component-render coverage for `<KpiTile>` (Refs #80, wave 3).
 *
 * Verifies the catalog-driven KPI tile against the wave-1 DESIGN §3.3
 * rendering rules:
 *   - `ok` rows render with peer coloring driven by the catalog row's
 *     `higher_is_better` and the median bar's `vs median` label.
 *   - `schema_status='error'` rows render the label/value but suppress
 *     the median bar so the broken metric reads as "no peer coloring".
 *   - Missing-id rows (no catalog entry for `ic_kpis.<key>`) render the
 *     tile without the median bar — graceful degrade per AC #4.
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
import { KpiTile } from "./kpi-tile";
import type { IcKpi, PeriodValue } from "@/types/insight";

const fetchCatalog = catalogClient.fetchCatalog as ReturnType<typeof vi.fn>;

function makeKpi(overrides: Partial<IcKpi> = {}): IcKpi {
  return {
    period: "month" as PeriodValue,
    metric_key: "bugs_fixed",
    label: "Bugs Fixed",
    value: "12",
    raw_value: 12,
    unit: "",
    sublabel: "Jira",
    description: "Bug-type Jira issues closed in the selected period.",
    delta: "",
    delta_type: "neutral",
    ...overrides,
  };
}

describe("<KpiTile>", () => {
  beforeEach(() => {
    authStore.reset();
    authStore.setTenantId("t-1");
    fetchCatalog.mockReset();
  });
  afterEach(() => {
    authStore.reset();
  });

  it("renders value and median label for an ok catalog row", async () => {
    fetchCatalog.mockResolvedValue(
      buildCatalogResponse([
        {
          metric_key: "ic_kpis.bugs_fixed",
          higher_is_better: true,
          schema_status: "ok",
        },
      ]),
    );
    renderWithCatalogClient(
      <KpiTile kpi={makeKpi()} median={{ p50: 6, n: 4 }} />,
    );
    // `vs median 6 · 4 peers` proves the catalog row drove the median
    // bar render (showMedian gates on `catalogRow !== undefined`). Wait
    // for it directly — the label renders synchronously from the prop,
    // but the median bar only appears once the catalog query resolves.
    await waitFor(() => {
      expect(screen.getByText(/vs median 6/)).toBeInTheDocument();
    });
    expect(screen.getByText("Bugs Fixed")).toBeInTheDocument();
    expect(screen.getByText("12")).toBeInTheDocument();
  });

  it("suppresses the median bar for schema_status='error'", async () => {
    fetchCatalog.mockResolvedValue(
      buildCatalogResponse([
        {
          metric_key: "ic_kpis.bugs_fixed",
          higher_is_better: true,
          schema_status: "error",
          schema_error_code: "column_not_found",
        },
      ]),
    );
    renderWithCatalogClient(
      <KpiTile kpi={makeKpi()} median={{ p50: 6, n: 4 }} />,
    );
    // Tile label still renders (broken-metric indicator semantic),
    // but the median bar is suppressed so the row reads as "no peer
    // comparison available."
    await waitFor(() => {
      expect(screen.queryByText(/vs median/)).not.toBeInTheDocument();
    });
    expect(screen.getByText("Bugs Fixed")).toBeInTheDocument();
  });

  it("renders the tile without median bar when the catalog has no row (missing-id)", async () => {
    fetchCatalog.mockResolvedValue(buildCatalogResponse([]));
    renderWithCatalogClient(
      <KpiTile kpi={makeKpi()} median={{ p50: 6, n: 4 }} />,
    );
    // Missing-id row: `catalogRow === undefined`, so the median bar is
    // suppressed. The tile's label / value come from the KPI prop
    // itself (already catalog-sourced by transforms), so they still
    // appear.
    await waitFor(() => {
      expect(screen.queryByText(/vs median/)).not.toBeInTheDocument();
    });
    expect(screen.getByText("Bugs Fixed")).toBeInTheDocument();
    expect(screen.getByText("12")).toBeInTheDocument();
  });
});

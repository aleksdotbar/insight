/**
 * Component-render coverage for `<SectionCard>` peer-driven coloring.
 *
 * The team view folds a blended department expectation onto each
 * team-bullet row's `peer` (p25/p50/p75). `rowStatus` colors the card off
 * that `peer` + the catalog's `higher_is_better` — no FE math. This pins:
 *   - a team-bullet row whose `peer` puts the value in the top quartile
 *     drives a 'good' badge stripe + "1 of 1 in top".
 *   - the headline still reflects the team aggregate (the row's label/value),
 *     independent of which cohort drew the color.
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
import { SectionCard } from "./section-card";
import type { BulletMetric, PeriodValue } from "@/types/insight";

const fetchCatalog = catalogClient.fetchCatalog as ReturnType<typeof vi.fn>;

function makeBullet(overrides: Partial<BulletMetric> = {}): BulletMetric {
  return {
    period: "month" as PeriodValue,
    section: "task_delivery",
    metric_key: "tasks_completed",
    label: "Tasks Closed",
    value: "12",
    unit: "tasks",
    range_min: "0",
    range_max: "20",
    median: "5",
    median_label: "",
    bar_left_pct: 0,
    bar_width_pct: 60,
    median_left_pct: 25,
    status: "good",
    drill_id: "",
    ...overrides,
  };
}

describe("<SectionCard> peer-driven coloring", () => {
  beforeEach(() => {
    authStore.reset();
    authStore.setTenantId("t-1");
    fetchCatalog.mockReset();
  });
  afterEach(() => {
    authStore.reset();
  });

  it("colors the badge off the row's blended-department peer, headline stays the aggregate", async () => {
    fetchCatalog.mockResolvedValue(
      buildCatalogResponse([
        {
          metric_key: "task_delivery_bullet_rows.tasks_completed",
          higher_is_better: true,
          schema_status: "ok",
        },
      ]),
    );
    // value 12 ≥ peer.p75 (8), higher_is_better ⇒ 'top' ⇒ good ⇒ "1 of 1 in top".
    const row = makeBullet({
      value: "12",
      peer: { p25: 4, p50: 6, p75: 8, min: 2, max: 15, n: 12 },
    });
    renderWithCatalogClient(
      <SectionCard
        title="Task delivery"
        sectionId="task_delivery"
        rows={[row]}
        onOpen={() => {}}
        subtitle="vs department expectation"
      />,
    );
    await waitFor(() => {
      expect(screen.getByText("1 of 1 in top")).toBeInTheDocument();
    });
    // Headline reflects the team aggregate row, not the cohort.
    expect(screen.getByText("Tasks Closed: 12 tasks")).toBeInTheDocument();
    expect(
      screen.getByText("vs department expectation"),
    ).toBeInTheDocument();
  });

  it("a row with no peer scores neutral — 'No peer data' badge", async () => {
    fetchCatalog.mockResolvedValue(
      buildCatalogResponse([
        {
          metric_key: "task_delivery_bullet_rows.tasks_completed",
          higher_is_better: true,
          schema_status: "ok",
        },
      ]),
    );
    const row = makeBullet({ value: "12", peer: undefined });
    renderWithCatalogClient(
      <SectionCard
        title="Task delivery"
        sectionId="task_delivery"
        rows={[row]}
        onOpen={() => {}}
      />,
    );
    await waitFor(() => {
      expect(screen.getByText("No peer data")).toBeInTheDocument();
    });
  });
});

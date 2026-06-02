/**
 * Component-render coverage for `<TeamMembersAttention>` (Refs #80,
 * wave 3).
 *
 * Catalog-driven; covers the wave-1 DESIGN §3.3 rendering rules:
 *   - `ok` bullet that lands in the bottom quartile vs peers counts
 *     toward a member's "N below peers" surface.
 *   - `schema_status='error'` bullets (surfaced as `row.schema_error: true`
 *     by transforms.ts) are filtered out of the count — a broken-source
 *     metric never raises a member alert.
 *   - Missing-id bullets (no catalog row) are filtered out — without a
 *     `higher_is_better` signal we can't classify them as "below".
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
import { TeamMembersAttention } from "./team-members-attention";
import type { PeerStats } from "@/lib/peers";
import type {
  BulletMetric,
  PeriodValue,
  TeamMember,
} from "@/types/insight";

const fetchCatalog = catalogClient.fetchCatalog as ReturnType<typeof vi.fn>;

function makeMember(overrides: Partial<TeamMember> = {}): TeamMember {
  return {
    person_id: "alice@example.com",
    period: "month" as PeriodValue,
    name: "Alice",
    seniority: "Senior",
    supervisor_email: null,
    tasks_closed: 0,
    bugs_fixed: 0,
    dev_time_h: null,
    prs_merged: null,
    build_success_pct: null,
    focus_time_pct: null,
    ai_tools: [],
    ai_loc_share_pct: null,
    ...overrides,
  };
}

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
    median_label: "",
    bar_left_pct: 0,
    bar_width_pct: 5,
    median_left_pct: 25,
    status: "bad",
    drill_id: "",
    ...overrides,
  };
}

const STATS: PeerStats = { p25: 3, p50: 5, p75: 10, min: 1, max: 15, n: 12 };

describe("<TeamMembersAttention>", () => {
  beforeEach(() => {
    authStore.reset();
    authStore.setTenantId("t-1");
    fetchCatalog.mockReset();
  });
  afterEach(() => {
    authStore.reset();
  });

  it("surfaces a member with an ok-row bullet scoring 'bottom' vs peers", async () => {
    fetchCatalog.mockResolvedValue(
      buildCatalogResponse([
        {
          metric_key: "task_delivery_bullet_rows.tasks_completed",
          higher_is_better: true,
          schema_status: "ok",
        },
      ]),
    );
    const bulletsByPerson = new Map<string, BulletMetric[]>([
      ["alice@example.com", [makeBullet({ value: "1" })]],
    ]);
    const cohortStats = new Map([["tasks_completed", STATS]]);
    renderWithCatalogClient(
      <TeamMembersAttention
        members={[makeMember()]}
        bulletsByPerson={bulletsByPerson}
        cohortStats={cohortStats}
        onMemberClick={() => {}}
      />,
    );
    // "1 members below peers" — the wording is unique to the title.
    await waitFor(() => {
      expect(
        screen.getByText("1 members below peers"),
      ).toBeInTheDocument();
    });
    expect(screen.getByText("Alice")).toBeInTheDocument();
  });

  it("schema_error bullets do NOT trigger the attention surface", async () => {
    fetchCatalog.mockResolvedValue(
      buildCatalogResponse([
        {
          metric_key: "task_delivery_bullet_rows.tasks_completed",
          higher_is_better: true,
          schema_status: "error",
        },
      ]),
    );
    const bulletsByPerson = new Map<string, BulletMetric[]>([
      ["alice@example.com", [makeBullet({ value: "1", schema_error: true })]],
    ]);
    const cohortStats = new Map([["tasks_completed", STATS]]);
    renderWithCatalogClient(
      <TeamMembersAttention
        members={[makeMember()]}
        bulletsByPerson={bulletsByPerson}
        cohortStats={cohortStats}
        onMemberClick={() => {}}
      />,
    );
    // With no member having a 'bottom' bullet, the component returns
    // null — the heading never appears. Wait through the loading
    // window where the compile-in fallback catalog renders the
    // attention surface.
    await waitFor(() => {
      expect(
        screen.queryByText("Members needing attention"),
      ).not.toBeInTheDocument();
    });
  });

  it("missing-id bullets (no catalog row) do NOT trigger the attention surface", async () => {
    fetchCatalog.mockResolvedValue(buildCatalogResponse([]));
    const bulletsByPerson = new Map<string, BulletMetric[]>([
      ["alice@example.com", [makeBullet({ value: "1" })]],
    ]);
    const cohortStats = new Map([["tasks_completed", STATS]]);
    renderWithCatalogClient(
      <TeamMembersAttention
        members={[makeMember()]}
        bulletsByPerson={bulletsByPerson}
        cohortStats={cohortStats}
        onMemberClick={() => {}}
      />,
    );
    await waitFor(() => {
      expect(
        screen.queryByText("Members needing attention"),
      ).not.toBeInTheDocument();
    });
  });
});

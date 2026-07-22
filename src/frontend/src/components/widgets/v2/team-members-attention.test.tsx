/**
 * Component-render coverage for `<TeamMembersAttention>`.
 *
 * Catalog-driven; each member is counted "below" against THAT member's own
 * department distribution (`deptCohorts` keyed by `org_unit_id → metric_key
 * → PeerStats`). Covers the wave-1 DESIGN §3.3 rendering rules:
 *   - bullet that lands in the bottom quartile of the member's dept counts
 *     toward the attention count (subtitle + per-row "trailing").
 *   - `schema_status='error'` bullets are filtered out of the count.
 *   - Missing-id bullets (no catalog row) are filtered out.
 *   - A degenerate department cohort (`n < MIN_DEPT_COHORT_N`) is not counted.
 */

import { screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { ReactNode } from "react";

import { authStore } from "@/auth/auth-store";

vi.mock("@/api/catalog-client", async () => {
  const actual = await vi.importActual<typeof import("@/api/catalog-client")>(
    "@/api/catalog-client",
  );
  return { ...actual, fetchCatalog: vi.fn() };
});

// `<TeamMembersAttention>` renders each member as a router `<Link>`; these
// render-rule tests don't exercise navigation, so stub Link to a plain anchor
// rather than standing up a full router context.
vi.mock("@tanstack/react-router", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("@tanstack/react-router")>();
  return {
    ...actual,
    Link: ({
      to,
      children,
    }: {
      to?: string;
      params?: unknown;
      children?: ReactNode;
    }) => <a href={to}>{children}</a>,
  };
});

import * as catalogClient from "@/api/catalog-client";
import {
  buildCatalogResponse,
  renderWithCatalogClient,
} from "@/test/catalog-test-utils";
import type { DeptCohorts, PeerStats } from "@/lib/peers";
import { TeamMembersAttention } from "./team-members-attention";
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
    org_unit_id: "Engineering",
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

function stats(overrides: Partial<PeerStats> = {}): PeerStats {
  return { p25: 8, p50: 10, p75: 12, min: 4, max: 20, n: 10, ...overrides };
}

// Attention compares member bullets, which live in the `bullet` family of
// the split DeptCohorts (the `kpi` family backs the heatmap's team_row
// columns and is irrelevant here).
function deptMap(
  rows: Array<[orgUnit: string, metricKey: string, s: PeerStats]>,
): DeptCohorts {
  const bullet = new Map<string, Map<string, PeerStats>>();
  for (const [orgUnit, metricKey, s] of rows) {
    let byMetric = bullet.get(orgUnit);
    if (!byMetric) {
      byMetric = new Map();
      bullet.set(orgUnit, byMetric);
    }
    byMetric.set(metricKey, s);
  }
  return { kpi: new Map(), bullet };
}

describe("<TeamMembersAttention>", () => {
  beforeEach(() => {
    authStore.reset();
    authStore.setAuthenticated({
      personId: "p-1",
      email: "bob.park@example.com",
      tenantId: "t-1",
      roles: ["user"],
    });
    fetchCatalog.mockReset();
  });
  afterEach(() => {
    authStore.reset();
  });

  it("surfaces a member whose bullet scores 'bottom' vs their department", async () => {
    fetchCatalog.mockResolvedValue(
      buildCatalogResponse([
        {
          metric_key: "task_delivery_bullet_rows.tasks_completed",
          higher_is_better: true,
          schema_status: "ok",
        },
      ]),
    );
    // Alice's value (1, higher = better) sits below her department's p25 (8)
    // ⇒ bottom quartile ⇒ counted. Per-department, not vs the displayed roster.
    const bulletsByPerson = new Map<string, BulletMetric[]>([
      ["alice@example.com", [makeBullet({ value: "1" })]],
    ]);
    const deptCohorts = deptMap([
      ["Engineering", "tasks_completed", stats()],
    ]);
    renderWithCatalogClient(
      <TeamMembersAttention
        members={[makeMember({ person_id: "alice@example.com", name: "Alice" })]}
        bulletsByPerson={bulletsByPerson}
        deptCohorts={deptCohorts}
      />,
    );
    await waitFor(() => {
      expect(
        screen.getByText("1 members · vs department peers"),
      ).toBeInTheDocument();
    });
    expect(screen.getByText("Alice")).toBeInTheDocument();
    expect(
      screen.getByText("1 members · vs department peers"),
    ).toBeInTheDocument();
  });

  it("compares an hours bullet directly against hours dept stats (no rescaling, #1475)", async () => {
    fetchCatalog.mockResolvedValue(
      buildCatalogResponse([
        {
          metric_key: "collab_bullet_rows.meeting_hours",
          higher_is_better: false,
          unit: "h",
          schema_status: "ok",
        },
      ]),
    );
    // meeting_hours is an hours metric — the FE never rescales it to days, so
    // the bullet stays in hours (192 h) and the department distribution is in
    // the same unit (p75 = 150 h). Alice sits above p75 (lower = better ⇒
    // bottom ⇒ counted). Display unit always matches the catalog unit.
    const bulletsByPerson = new Map<string, BulletMetric[]>([
      [
        "alice@example.com",
        [
          makeBullet({
            section: "collaboration",
            metric_key: "meeting_hours",
            value: "192",
            unit: "h",
          }),
        ],
      ],
    ]);
    const deptCohorts = deptMap([
      [
        "Engineering",
        "meeting_hours",
        stats({ p25: 50, p50: 100, p75: 150, min: 10, max: 200 }),
      ],
    ]);
    renderWithCatalogClient(
      <TeamMembersAttention
        members={[makeMember({ person_id: "alice@example.com", name: "Alice" })]}
        bulletsByPerson={bulletsByPerson}
        deptCohorts={deptCohorts}
      />,
    );
    await waitFor(() => {
      expect(screen.getByText("1 members · vs department peers")).toBeInTheDocument();
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
    const deptCohorts = deptMap([
      ["Engineering", "tasks_completed", stats()],
    ]);
    renderWithCatalogClient(
      <TeamMembersAttention
        members={[makeMember()]}
        bulletsByPerson={bulletsByPerson}
        deptCohorts={deptCohorts}
      />,
    );
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
    const deptCohorts = deptMap([
      ["Engineering", "tasks_completed", stats()],
    ]);
    renderWithCatalogClient(
      <TeamMembersAttention
        members={[makeMember()]}
        bulletsByPerson={bulletsByPerson}
        deptCohorts={deptCohorts}
      />,
    );
    await waitFor(() => {
      expect(
        screen.queryByText("Members needing attention"),
      ).not.toBeInTheDocument();
    });
  });

  it("a degenerate department cohort (n < 5) is NOT counted", async () => {
    fetchCatalog.mockResolvedValue(
      buildCatalogResponse([
        {
          metric_key: "task_delivery_bullet_rows.tasks_completed",
          higher_is_better: true,
          schema_status: "ok",
        },
      ]),
    );
    // Alice's value (1) would be bottom-quartile, but her dept cohort holds
    // only 3 people (< MIN_DEPT_COHORT_N) ⇒ not counted, surface stays hidden.
    const bulletsByPerson = new Map<string, BulletMetric[]>([
      ["alice@example.com", [makeBullet({ value: "1" })]],
    ]);
    const deptCohorts = deptMap([
      ["Engineering", "tasks_completed", stats({ n: 3 })],
    ]);
    renderWithCatalogClient(
      <TeamMembersAttention
        members={[makeMember()]}
        bulletsByPerson={bulletsByPerson}
        deptCohorts={deptCohorts}
      />,
    );
    await waitFor(() => {
      expect(
        screen.queryByText("Members needing attention"),
      ).not.toBeInTheDocument();
    });
  });
});

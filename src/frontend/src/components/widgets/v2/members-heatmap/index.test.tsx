/**
 * Component-render coverage for `<MembersHeatmap>`.
 *
 * Verifies the catalog-driven heatmap colors each member against THAT
 * member's own department distribution (`deptCohorts` keyed by
 * `org_unit_id → metric_key → PeerStats`) and respects the wave-1
 * DESIGN §3.3 rules on its bullet-derived cells:
 *   - bullet that lands in the bottom quartile of the member's dept drives
 *     the "N issues" chip.
 *   - `schema_error` rows are filtered from the "below peers" count.
 *   - Missing-id rows (no catalog row) are filtered out — without a
 *     `higher_is_better` signal the bullet-based worst-pick can't classify.
 *   - A degenerate department cohort (`n < MIN_DEPT_COHORT_N`) → neutral.
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
import type { DeptCohorts, PeerStats } from "@/lib/peers";
import { MembersHeatmap } from "./index";
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
    tasks_closed: 8,
    bugs_fixed: 2,
    dev_time_h: null,
    prs_merged: 3,
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
    metric_key: "mean_time_to_resolution",
    label: "Mean Time to Resolution",
    value: "30",
    unit: "d",
    range_min: "0",
    range_max: "60",
    median: "10",
    median_label: "",
    bar_left_pct: 0,
    bar_width_pct: 50,
    median_left_pct: 25,
    status: "bad",
    drill_id: "",
    ...overrides,
  };
}

/** PeerStats with a healthy cohort size (above MIN_DEPT_COHORT_N). */
function stats(overrides: Partial<PeerStats> = {}): PeerStats {
  return { p25: 4, p50: 5, p75: 6, min: 2, max: 10, n: 10, ...overrides };
}

// Mirrors the production family routing in `fetchDeptDistributions`: the
// team_row heatmap keys live in the `kpi` family, everything else in `bullet`.
const KPI_FAMILY_KEYS = new Set([
  "tasks_closed",
  "bugs_fixed",
  "prs_merged",
  "focus_time_pct",
  "ai_loc_share_pct",
]);

function deptMap(
  rows: Array<[orgUnit: string, metricKey: string, s: PeerStats]>,
): DeptCohorts {
  const out: DeptCohorts = { kpi: new Map(), bullet: new Map() };
  for (const [orgUnit, metricKey, s] of rows) {
    const target = KPI_FAMILY_KEYS.has(metricKey) ? out.kpi : out.bullet;
    let byMetric = target.get(orgUnit);
    if (!byMetric) {
      byMetric = new Map();
      target.set(orgUnit, byMetric);
    }
    byMetric.set(metricKey, s);
  }
  return out;
}

describe("<MembersHeatmap>", () => {
  beforeEach(() => {
    authStore.reset();
    authStore.setTenantId("t-1");
    fetchCatalog.mockReset();
  });
  afterEach(() => {
    authStore.reset();
  });

  it("counts a bottom-quartile bullet vs the member's department toward the 'N issues' chip", async () => {
    fetchCatalog.mockResolvedValue(
      buildCatalogResponse([
        {
          metric_key: "task_delivery_bullet_rows.mean_time_to_resolution",
          higher_is_better: false,
          schema_status: "ok",
        },
      ]),
    );
    // Alice's MTTR (30, higher = worse) sits above her department's p75 (6),
    // so her cell is 'bottom' ⇒ 1 issue. Coloring is per-department, not vs
    // the displayed roster.
    const bulletsByPerson = new Map<string, BulletMetric[]>([
      ["alice@example.com", [makeBullet({ value: "30" })]],
    ]);
    const deptCohorts = deptMap([
      ["Engineering", "mean_time_to_resolution", stats({ p25: 4, p50: 5, p75: 6 })],
    ]);
    renderWithCatalogClient(
      <MembersHeatmap
        members={[makeMember({ person_id: "alice@example.com", name: "Alice" })]}
        bulletsByPerson={bulletsByPerson}
        deptCohorts={deptCohorts}
      />,
    );
    await waitFor(() => {
      // The chip is rendered twice — once in the mobile triage list,
      // once in the desktop grid. Both should agree.
      expect(screen.getAllByText("1 issue").length).toBeGreaterThan(0);
    });
  });

  it("schema_error bullet does NOT contribute to the 'issues' count", async () => {
    fetchCatalog.mockResolvedValue(
      buildCatalogResponse([
        {
          metric_key: "task_delivery_bullet_rows.mean_time_to_resolution",
          higher_is_better: false,
          schema_status: "error",
        },
      ]),
    );
    const bulletsByPerson = new Map<string, BulletMetric[]>([
      [
        "alice@example.com",
        [makeBullet({ value: "30", schema_error: true })],
      ],
    ]);
    const deptCohorts = deptMap([
      ["Engineering", "mean_time_to_resolution", stats()],
    ]);
    renderWithCatalogClient(
      <MembersHeatmap
        members={[makeMember()]}
        bulletsByPerson={bulletsByPerson}
        deptCohorts={deptCohorts}
      />,
    );
    await waitFor(() => {
      expect(screen.queryByText(/^\d+ issues?$/)).not.toBeInTheDocument();
    });
  });

  it("a degenerate department cohort (n < 5) renders neutral — no 'issues' chip", async () => {
    fetchCatalog.mockResolvedValue(
      buildCatalogResponse([
        {
          metric_key: "task_delivery_bullet_rows.mean_time_to_resolution",
          higher_is_better: false,
          schema_status: "ok",
        },
      ]),
    );
    // Alice's MTTR (30) would be bottom-quartile, but her department's
    // cohort holds only 3 people (< MIN_DEPT_COHORT_N) ⇒ neutral, not counted.
    const bulletsByPerson = new Map<string, BulletMetric[]>([
      ["alice@example.com", [makeBullet({ value: "30" })]],
    ]);
    const deptCohorts = deptMap([
      ["Engineering", "mean_time_to_resolution", stats({ n: 3 })],
    ]);
    renderWithCatalogClient(
      <MembersHeatmap
        members={[makeMember()]}
        bulletsByPerson={bulletsByPerson}
        deptCohorts={deptCohorts}
      />,
    );
    await waitFor(() => {
      expect(fetchCatalog).toHaveBeenCalled();
    });
    expect(screen.queryByText(/^\d+ issues?$/)).not.toBeInTheDocument();
  });

  it("a member whose department is absent from the cohort map renders neutral", async () => {
    fetchCatalog.mockResolvedValue(
      buildCatalogResponse([
        {
          metric_key: "task_delivery_bullet_rows.mean_time_to_resolution",
          higher_is_better: false,
          schema_status: "ok",
        },
      ]),
    );
    const bulletsByPerson = new Map<string, BulletMetric[]>([
      ["alice@example.com", [makeBullet({ value: "30" })]],
    ]);
    // Map keyed for a different department than Alice's ⇒ no peer data.
    const deptCohorts = deptMap([
      ["Sales", "mean_time_to_resolution", stats()],
    ]);
    renderWithCatalogClient(
      <MembersHeatmap
        members={[makeMember({ org_unit_id: "Engineering" })]}
        bulletsByPerson={bulletsByPerson}
        deptCohorts={deptCohorts}
      />,
    );
    await waitFor(() => {
      expect(fetchCatalog).toHaveBeenCalled();
    });
    expect(screen.queryByText(/^\d+ issues?$/)).not.toBeInTheDocument();
  });
});

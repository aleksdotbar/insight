import { describe, expect, it } from "vitest";

import type { CatalogByKey } from "@/lib/insight/v2/peer-status";
import {
  MIN_DEPT_COHORT_N,
  memberMetricPeerStatus,
  teamSectionStatusByMetric,
} from "@/lib/insight/v2/team-member-status";
import type { DeptCohorts, PeerStats } from "@/lib/peers";
import type { CatalogMetric } from "@/api/catalog-client";
import type { BulletMetric, PeriodValue, TeamMember } from "@/types/insight";

// Catalog stub: every metric resolves as higher-is-better with unit "tasks".
// The rollup logic is what's under test, not the catalog wiring.
function cat(): CatalogMetric {
  return {
    id: "id",
    metric_key: "k",
    label: "L",
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
    unit: "tasks",
  };
}
const byMetricKey: CatalogByKey = () => cat();

function member(personId: string, orgUnitId: string | null): TeamMember {
  return {
    person_id: personId,
    period: "month" as PeriodValue,
    name: personId,
    seniority: "Senior",
    supervisor_email: null,
    org_unit_id: orgUnitId,
    tasks_closed: 0,
    bugs_fixed: 0,
    dev_time_h: null,
    prs_merged: null,
    build_success_pct: null,
    focus_time_pct: null,
    ai_tools: [],
    ai_loc_share_pct: null,
  };
}

function bullet(metricKey: string, value: number): BulletMetric {
  return {
    period: "month" as PeriodValue,
    section: "task_delivery",
    metric_key: metricKey,
    label: metricKey,
    value: String(value),
    unit: "tasks",
    range_min: "0",
    range_max: "20",
    median: "10",
    median_label: "",
    bar_left_pct: 0,
    bar_width_pct: 0,
    median_left_pct: 0,
    status: "good",
    drill_id: "",
  };
}

// p25=8, p75=12: value < 8 → bottom, value > 12 → top, else in_pack.
function stats(overrides: Partial<PeerStats> = {}): PeerStats {
  return { p25: 8, p50: 10, p75: 12, min: 4, max: 20, n: 10, ...overrides };
}

function deptMap(
  rows: Array<[orgUnit: string, metricKey: string, s: PeerStats]>,
): DeptCohorts {
  const bulletFamily = new Map<string, Map<string, PeerStats>>();
  for (const [orgUnit, metricKey, s] of rows) {
    let byMetric = bulletFamily.get(orgUnit);
    if (!byMetric) {
      byMetric = new Map();
      bulletFamily.set(orgUnit, byMetric);
    }
    byMetric.set(metricKey, s);
  }
  return { kpi: new Map(), bullet: bulletFamily };
}

function rosterBullets(
  rows: Array<[personId: string, value: number]>,
  metricKey: string,
): Map<string, BulletMetric[]> {
  const m = new Map<string, BulletMetric[]>();
  for (const [personId, value] of rows) {
    m.set(personId.toLowerCase(), [bullet(metricKey, value)]);
  }
  return m;
}

describe("teamSectionStatusByMetric", () => {
  const dept = deptMap([["eng", "tasks", stats()]]);
  const members = [member("a", "eng"), member("b", "eng"), member("c", "eng")];

  it("flags 'bad' when more members are below their department than above", () => {
    // a,b below p25 (bottom); c above p75 (top) → bottom 2 > top 1 → bad.
    const bullets = rosterBullets(
      [
        ["a", 1],
        ["b", 1],
        ["c", 15],
      ],
      "tasks",
    );
    const map = teamSectionStatusByMetric(
      [bullet("tasks", 0)],
      members,
      bullets,
      dept,
      byMetricKey,
    );
    expect(map.get("tasks")).toBe("bad");
  });

  it("flags 'good' when more members are above than below", () => {
    const bullets = rosterBullets(
      [
        ["a", 15],
        ["b", 15],
        ["c", 1],
      ],
      "tasks",
    );
    const map = teamSectionStatusByMetric(
      [bullet("tasks", 0)],
      members,
      bullets,
      dept,
      byMetricKey,
    );
    expect(map.get("tasks")).toBe("good");
  });

  it("falls to 'warn' on a top/bottom tie", () => {
    // one top, one bottom, one on-par → no plurality → warn.
    const bullets = rosterBullets(
      [
        ["a", 15],
        ["b", 1],
        ["c", 10],
      ],
      "tasks",
    );
    const map = teamSectionStatusByMetric(
      [bullet("tasks", 0)],
      members,
      bullets,
      dept,
      byMetricKey,
    );
    expect(map.get("tasks")).toBe("warn");
  });

  it("falls to 'warn' when most members are on-par (in_pack majority)", () => {
    // two on-par, one below → bottom(1) not > in_pack(2) → warn, not bad.
    const bullets = rosterBullets(
      [
        ["a", 10],
        ["b", 10],
        ["c", 1],
      ],
      "tasks",
    );
    const map = teamSectionStatusByMetric(
      [bullet("tasks", 0)],
      members,
      bullets,
      dept,
      byMetricKey,
    );
    expect(map.get("tasks")).toBe("warn");
  });

  it("returns 'neutral' when no member has a usable department cohort", () => {
    const thin = deptMap([["eng", "tasks", stats({ n: MIN_DEPT_COHORT_N - 1 })]]);
    const bullets = rosterBullets(
      [
        ["a", 1],
        ["b", 1],
        ["c", 1],
      ],
      "tasks",
    );
    const map = teamSectionStatusByMetric(
      [bullet("tasks", 0)],
      members,
      bullets,
      thin,
      byMetricKey,
    );
    expect(map.get("tasks")).toBe("neutral");
  });
});

describe("memberMetricPeerStatus", () => {
  it("scores each member against their OWN department", () => {
    // Same value (15); top vs eng (p75=12) but in_pack vs data (p75=20).
    const dept = deptMap([
      ["eng", "tasks", stats()],
      ["data", "tasks", stats({ p25: 10, p50: 15, p75: 20, max: 30 })],
    ]);
    expect(
      memberMetricPeerStatus(member("a", "eng"), bullet("tasks", 15), dept, byMetricKey),
    ).toBe("top");
    expect(
      memberMetricPeerStatus(member("b", "data"), bullet("tasks", 15), dept, byMetricKey),
    ).toBe("in_pack");
  });

  it("returns null for a degenerate department, schema error, or missing catalog row", () => {
    const dept = deptMap([["eng", "tasks", stats()]]);
    const thin = deptMap([["eng", "tasks", stats({ n: 2 })]]);
    expect(
      memberMetricPeerStatus(member("a", "eng"), bullet("tasks", 1), thin, byMetricKey),
    ).toBeNull();
    expect(
      memberMetricPeerStatus(
        member("a", "eng"),
        { ...bullet("tasks", 1), schema_error: true },
        dept,
        byMetricKey,
      ),
    ).toBeNull();
    expect(
      memberMetricPeerStatus(member("a", "eng"), bullet("tasks", 1), dept, () => undefined),
    ).toBeNull();
  });
});

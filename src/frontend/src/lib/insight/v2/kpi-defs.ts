/**
 * FE-only IC KPI → section mapping (Refs #80).
 *
 * Wave-3 of catalog hydration (#66) moved IC-KPI `label / sublabel /
 * description / unit / higher_is_better / format` onto the wire response
 * surfaced by `useCatalog()`. The single field the catalog doesn't carry
 * is `section`: the IC-dashboard owns the IC↔section grouping
 * (`task_delivery`, `git_output`, …), and it's a navigation rule, not a
 * metric property — used by `EngineeringDashboardV2.openSectionForMetric`
 * to land the drilldown sheet on the right section when a KPI tile is
 * clicked. When/if that mapping moves to the wire (e.g. an `ic_section`
 * tag on each catalog row) this map can go away.
 *
 * Lookup is by bare KPI `metric_key` (e.g. `bugs_fixed`) — the form
 * `IcKpi.metric_key` carries after `transforms.ts::transformIcKpis`
 * strips the `ic_kpis.` wire prefix.
 *
 * Exposed as a `Map` (not a `Record`) so an attacker-controlled key
 * like `"__proto__"` returns `undefined` instead of leaking
 * `Object.prototype` members.
 */

import type { IcSectionId } from "./sections";

/**
 * Ordered allowlist of IC KPI tiles for "At a glance" (bare metric_key). The
 * catalog may surface more `ic_kpis.*` rows than we tile; this both filters
 * (only these render) and fixes their order. Keys missing from the catalog
 * response are skipped.
 */
export const IC_KPI_ORDER: readonly string[] = [
  "tasks_closed",
  "focus_time_pct",
  "prs_merged",
  "ai_loc_share_pct",
  "ai_sessions",
];

const IC_KPI_RANK = new Map(IC_KPI_ORDER.map((key, i) => [key, i] as const));

export function orderIcKpis<T extends { metric_key: string }>(items: T[]): T[] {
  return items
    .filter((it) => IC_KPI_RANK.has(it.metric_key))
    .sort(
      (a, b) =>
        IC_KPI_RANK.get(a.metric_key)! - IC_KPI_RANK.get(b.metric_key)!,
    );
}

export const IC_KPI_SECTION_BY_KEY: ReadonlyMap<string, IcSectionId> =
  new Map<string, IcSectionId>([
    // Wire key is `ic_kpis.ai_loc_share_pct` (the seed migration carries the
    // `_pct` suffix). #81 parity capture flagged this rename; the entry
    // here matches the bare metric_key post-`ic_kpis.` strip.
    ["ai_loc_share_pct", "ai_adoption"],
    ["focus_time_pct", "collaboration"],
    ["tasks_closed", "task_delivery"],
    ["prs_merged", "git_output"],
    ["pr_cycle_time_h", "git_output"],
    ["ai_sessions", "ai_adoption"],
  ]);

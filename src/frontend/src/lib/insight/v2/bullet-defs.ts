/**
 * FE-only `description` strings for bullet metrics (Refs #80).
 *
 * Wave-3 of the catalog hydration (#66) moved every other piece of bullet
 * metadata — `label`, `sublabel`, `unit`, `higher_is_better`, `thresholds`,
 * `schema_status` — into the wire response surfaced by `useCatalog()`. The
 * one field the catalog doesn't yet carry for bullets is `description`:
 * the long-form explainer text shown under the metric label when the
 * Explanations toggle is on. IC KPIs already source `description` from
 * the catalog (see `mocks/catalog-factory.ts::buildKpiMetric`); bullets
 * have no equivalent. When the backend grows a `description` field for
 * bullet catalog rows, this map can fold into the wire.
 *
 * Lookup is by bare `metric_key` (e.g. `tasks_completed`) — the same
 * shape `BulletMetric.metric_key` carries. Missing entries return
 * `undefined`; consumers MUST tolerate that (`<MetricSublabel>` renders
 * `null` when description is absent).
 *
 * Exposed as a `Map` (not a `Record`) so an attacker-controlled or
 * malformed `metric_key` like `"__proto__"` / `"constructor"` /
 * `"hasOwnProperty"` returns `undefined` instead of leaking
 * `Object.prototype` members through bracket access — keeps render
 * paths in `<MetricSublabel>` immune to "function rendered as React
 * child" crashes if the backend ever ships an unsanitized key.
 */
export const BULLET_DESCRIPTION_BY_KEY: ReadonlyMap<string, string> = new Map<
  string,
  string
>(Object.entries({
  tasks_completed: "Closed tasks per developer",
  task_dev_time: "Median time in development statuses",
  task_reopen_rate: "Closed tasks reopened later",
  due_date_compliance: "Tasks closed by their due date",
  estimation_accuracy: "Estimate vs actual dev time",
  bugs_to_task_ratio: "Bugs as a share of closed tasks",
  mean_time_to_resolution: "Median issue lifetime, create to close",
  flow_efficiency: "Dev time vs total lifetime",
  pickup_time: "Created to first dev status",

  prs_per_dev: "Merged PRs per developer",
  build_success: "CI runs passed vs total",
  pr_cycle_time: "Hours from PR open to merge",
  bugs_fixed: "Bug-type issues closed",

  wiki_pages_created: "Wiki pages authored (Confluence/Outline)",
  wiki_edits: "Wiki page revisions authored",
  wiki_comments: "Comments received on the person's wiki pages",
  wiki_active_authors: "Members who authored or edited the wiki this period",
}));

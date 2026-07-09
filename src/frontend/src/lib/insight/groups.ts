import type { MetricCollectionConfig } from "@/lib/metrics/collection";

/**
 * Dashboard composition registry: named groups of metrics and the KPI row.
 * This file is the single place a new metrics-backed group is added — one
 * `MetricGroup` entry (plus a renderer only if it needs a chart kind outside
 * the vocabulary).
 *
 * A group is a named set of metrics with presentation defaults — nothing ties
 * it to a connector or source family; a group may mix metrics from any
 * families (today's groups align with connector classes only because AI is
 * the first migrated family). Meaning (labels, units, formats, directions,
 * explanations) is server-owned and rides the `/v1/metric-results` response;
 * groups carry metric KEYS and presentation choices only. Group titles are
 * dashboard composition, so they live here.
 */

export type TimeseriesChartKind = "line" | "stacked-bar";
export type BreakdownChartKind = "bars";
export type HistogramChartKind = "histogram";
export type ChartKind =
  | TimeseriesChartKind
  | BreakdownChartKind
  | HistogramChartKind;

/**
 * One chart in a group's drilldown. Blocks compose: a multi-metric chart
 * is `metrics: [a, b]` (axes config joins when a family needs dual-axis).
 * The supporting grid is not a block — every group metric renders
 * value + peer-comparison below the blocks by convention. Chart kinds are
 * constrained per view so a block can't pair a composition chart with a
 * timeseries payload.
 */
export type DrilldownBlock =
  | { view: "timeseries"; chart: TimeseriesChartKind; metrics: string[] }
  | { view: "breakdown"; chart: BreakdownChartKind; metrics: string[] }
  | { view: "histogram"; chart: HistogramChartKind; metrics: string[] };

export interface MetricGroup {
  kind: "metrics";
  id: GroupId;
  title: string;
  collection: MetricCollectionConfig;
  card: { preview: string[] };
  drilldown: DrilldownBlock[];
}

/** A group still rendered by the legacy data path; dies with it. */
export interface LegacyGroup {
  kind: "legacy";
  id: GroupId;
  title: string;
}

export type GroupDef = MetricGroup | LegacyGroup;

export type GroupId =
  | "task_delivery"
  | "git_output"
  | "collaboration"
  | "ai_adoption"
  | "wiki";

const AI_ADOPTION_COLLECTION: MetricCollectionConfig = {
  metrics: [
    {
      key: "ai.accepted_lines",
      views: [
        { view: "period" },
        { view: "peer" },
        { view: "timeseries", bucket: "auto", dimensions: ["tool"] },
      ],
    },
    { key: "ai.removed_lines", views: [{ view: "period" }, { view: "peer" }] },
    { key: "ai.active_days", views: [{ view: "period" }, { view: "peer" }] },
    { key: "ai.cost", views: [{ view: "period" }, { view: "peer" }] },
    {
      key: "ai.accepted_edit_actions",
      views: [{ view: "period" }, { view: "peer" }],
    },
    {
      key: "ai.tool_acceptance_rate",
      views: [{ view: "period" }, { view: "peer" }],
    },
    {
      key: "ai.assistant_messages",
      views: [{ view: "period" }, { view: "peer" }],
    },
    {
      key: "ai.assistant_actions",
      views: [{ view: "period" }, { view: "peer" }],
    },
    {
      key: "ai.dev_conversations",
      views: [{ view: "period" }, { view: "peer" }],
    },
    {
      key: "ai.chat_assistant_conversations",
      views: [{ view: "period" }, { view: "peer" }],
    },
  ],
};

const GIT_OUTPUT_COLLECTION: MetricCollectionConfig = {
  metrics: [
    {
      key: "git.commits",
      views: [
        { view: "period" },
        { view: "peer" },
        { view: "timeseries", bucket: "auto" },
      ],
    },
    {
      key: "git.prs_merged",
      views: [
        { view: "period" },
        { view: "peer" },
        { view: "timeseries", bucket: "auto" },
      ],
    },
    {
      key: "git.lines_added",
      views: [
        { view: "period" },
        { view: "peer" },
        { view: "timeseries", bucket: "auto", dimensions: ["category"] },
      ],
    },
    {
      key: "git.pr_cycle_time_h",
      views: [{ view: "period" }, { view: "peer" }, { view: "histogram" }],
    },
    {
      key: "git.pr_size",
      views: [{ view: "period" }, { view: "peer" }, { view: "histogram" }],
    },
    {
      key: "git.commit_size",
      views: [{ view: "period" }, { view: "peer" }, { view: "histogram" }],
    },
    { key: "git.code_lines", views: [{ view: "period" }, { view: "peer" }] },
    { key: "git.prs_created", views: [{ view: "period" }, { view: "peer" }] },
    { key: "git.merge_rate", views: [{ view: "period" }, { view: "peer" }] },
    {
      key: "git.commits_per_active_day",
      views: [{ view: "period" }, { view: "peer" }],
    },
  ],
};

export const GROUPS: readonly GroupDef[] = [
  { kind: "legacy", id: "task_delivery", title: "Task delivery" },
  {
    kind: "metrics",
    id: "git_output",
    title: "Git output",
    collection: GIT_OUTPUT_COLLECTION,
    card: {
      preview: ["git.commits", "git.prs_merged", "git.pr_cycle_time_h"],
    },
    drilldown: [
      {
        chart: "line",
        view: "timeseries",
        metrics: ["git.commits", "git.prs_merged"],
      },
      {
        chart: "stacked-bar",
        view: "timeseries",
        metrics: ["git.lines_added"],
      },
      {
        chart: "histogram",
        view: "histogram",
        metrics: ["git.pr_cycle_time_h"],
      },
      { chart: "histogram", view: "histogram", metrics: ["git.pr_size"] },
      { chart: "histogram", view: "histogram", metrics: ["git.commit_size"] },
    ],
  },
  { kind: "legacy", id: "collaboration", title: "Collaboration" },
  {
    kind: "metrics",
    id: "ai_adoption",
    title: "AI adoption",
    collection: AI_ADOPTION_COLLECTION,
    card: {
      preview: ["ai.active_days", "ai.cost", "ai.tool_acceptance_rate"],
    },
    drilldown: [
      {
        chart: "stacked-bar",
        view: "timeseries",
        metrics: ["ai.accepted_lines"],
      },
    ],
  },
  { kind: "legacy", id: "wiki", title: "Wiki" },
];

export function groupById(id: GroupId): GroupDef {
  const def = GROUPS.find((g) => g.id === id);
  if (!def) throw new Error(`Unknown group: ${id}`);
  return def;
}

export function metricGroups(): MetricGroup[] {
  return GROUPS.filter((g): g is MetricGroup => g.kind === "metrics");
}

export function legacyGroups(): LegacyGroup[] {
  return GROUPS.filter((g): g is LegacyGroup => g.kind === "legacy");
}

/**
 * The "At a glance" KPI row: array order is display order. `legacy` tiles
 * come from the legacy KPI batch; `metric` tiles come from the derived
 * KPI collection below. Both render through the same display-ready tile
 * intermediate — selectors own formatting and scoring.
 */
export type KpiTileSource =
  | { kind: "legacy"; key: string; groupId: GroupId }
  | { kind: "metric"; metricKey: string };

export const KPI_ROW: readonly KpiTileSource[] = [
  { kind: "legacy", key: "tasks_closed", groupId: "task_delivery" },
  { kind: "legacy", key: "focus_time_pct", groupId: "collaboration" },
  { kind: "metric", metricKey: "git.prs_merged" },
  { kind: "metric", metricKey: "ai.active_days" },
  { kind: "metric", metricKey: "ai.accepted_lines" },
];

export const KPI_ROW_COLLECTION: MetricCollectionConfig = {
  metrics: KPI_ROW.filter(
    (t): t is Extract<KpiTileSource, { kind: "metric" }> =>
      t.kind === "metric",
  ).map((t) => ({
    key: t.metricKey,
    views: [{ view: "period" }, { view: "peer" }],
  })),
};

/** Metrics-backed KPI tiles navigate to the group that owns their metric. */
export function groupIdForMetricKey(metricKey: string): GroupId | null {
  for (const def of GROUPS) {
    if (def.kind !== "metrics") continue;
    if (def.collection.metrics.some((m) => m.key === metricKey)) return def.id;
  }
  return null;
}

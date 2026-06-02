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

  commits: "Distinct commits authored",
  prs_created: "Pull requests authored",
  prs_merged: "Authored PRs that merged",
  clean_loc: "Lines added, excl. spec/config/generated",
  pr_cycle_time_h: "Avg hours from PR open to merge",
  pr_size: "Median lines changed per PR",
  merge_rate: "PRs merged vs created",
  lines_per_commit: "Average commit size",
  commits_per_active_day: "Commits per committing day",

  prs_per_dev: "Merged PRs per developer",
  build_success: "CI runs passed vs total",
  pr_cycle_time: "Hours from PR open to merge",
  bugs_fixed: "Bug-type issues closed",

  active_ai_members: "Members using any AI tool",
  cursor_active: "Members using Cursor",
  cc_active: "Members using Claude Code",
  team_ai_loc: "Lines accepted across AI tools",
  cursor_acceptance: "Cursor suggestions accepted vs shown",
  cc_tool_acceptance: "Claude Code calls accepted vs proposed",
  cursor_lines: "Lines accepted from Cursor",
  cc_lines: "Lines accepted from Claude Code",
  ai_loc_share2: "AI-assisted lines vs clean LOC",
  cc_cost: "Per-user cost from Claude Team plan in cents",
  prs_with_cc: "PRs where Claude Code was active (requires Anthropic GitHub-app)",
  prs_total: "Total PRs in measurement window — denominator for CC attribution rate",

  slack_messages_sent: "Chat messages authored",
  slack_active_days: "Days with a chat message",
  slack_dm_ratio: "Direct messages as a share of all",
  m365_emails_sent: "Emails sent",
  m365_emails_received: "Inbox email volume",
  m365_teams_chats: "Direct and group chats sent",
  meeting_hours: "Hours in scheduled meetings",
  meetings_count: "Distinct meetings attended",
  meeting_free: "Working days with no meetings",
}));

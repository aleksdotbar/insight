export const METRIC_ORDER_BY_SECTION: Record<string, string[]> = {
  task_delivery: [
    "tasks_completed",
    "mean_time_to_resolution",
    "pickup_time",
    "stale_in_progress",
    "task_reopen_rate",
    "bugs_to_task_ratio",
    "flow_efficiency",
    "estimation_accuracy",
    "due_date_compliance",
    "worklog_logging_accuracy",
    "task_dev_time",
  ],
  git_output: [
    "commits",
    "clean_loc",
    "prs_merged",
    "prs_created",
    "merge_rate",
    "pr_cycle_time_h",
    "pr_size",
    "lines_per_commit",
    "commits_per_active_day",
  ],
  code_quality: [
    "build_success",
    "bugs_fixed",
    "pr_cycle_time",
    "prs_per_dev",
  ],
  collaboration: [
    "meeting_free",
    "slack_dm_ratio",
    "m365_emails_received",
    "m365_emails_read",
  ],
  ai_adoption: [
    "active_ai_members",
    "ai_loc_share2",
    "cursor_active",
    "cc_active",
    "codex_active",
    "cursor_acceptance",
    "cc_tool_acceptance",
    "cursor_lines",
    "cc_lines",
    "team_ai_loc",
  ],
};

export function orderRowsForSection<T extends { metric_key: string }>(
  sectionKey: string,
  rows: T[],
): T[] {
  const order = METRIC_ORDER_BY_SECTION[sectionKey];
  if (!order) return rows;
  const index = new Map<string, number>();
  order.forEach((k, i) => index.set(k, i));
  return [...rows].sort((a, b) => {
    const ia = index.get(a.metric_key);
    const ib = index.get(b.metric_key);
    if (ia == null && ib == null) return a.metric_key.localeCompare(b.metric_key);
    if (ia == null) return 1;
    if (ib == null) return -1;
    return ia - ib;
  });
}

export function metricOrderIndex(sectionKey: string, metricKey: string): number {
  const order = METRIC_ORDER_BY_SECTION[sectionKey];
  if (!order) return Number.MAX_SAFE_INTEGER;
  const idx = order.indexOf(metricKey);
  return idx === -1 ? Number.MAX_SAFE_INTEGER : idx;
}

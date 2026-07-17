export const METRIC_ORDER_BY_SECTION: Record<string, string[]> = {
  code_quality: [
    "build_success",
    "bugs_fixed",
    "pr_cycle_time",
    "prs_per_dev",
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

import type { BulletMetric } from "@/types/insight";

export interface PartitionedBullets {
  counters: BulletMetric[];
  distributions: BulletMetric[];
}

const DISTRIBUTION_KEYS = new Set([
  "task_dev_time",
  "mean_time_to_resolution",
  "pickup_time",
  "pr_cycle_time_h",
  "pr_cycle_time",
  "pr_size",
]);

export function partitionBullets(rows: BulletMetric[]): PartitionedBullets {
  const counters: BulletMetric[] = [];
  const distributions: BulletMetric[] = [];
  for (const r of rows) {
    if (DISTRIBUTION_KEYS.has(r.metric_key)) distributions.push(r);
    else counters.push(r);
  }
  return { counters, distributions };
}

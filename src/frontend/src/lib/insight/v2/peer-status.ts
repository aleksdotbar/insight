import {
  peerStatusVsQuartiles,
  type PeerStats,
  type PeerStatusWithNeutral,
} from "@/lib/peers";
import type { Status } from "@/lib/status";
import type { BulletMetric } from "@/types/insight";

import { BULLET_DEFS_BY_KEY } from "./bullet-defs";

export function hasBulletValue(row: BulletMetric): boolean {
  if (row.value === "" || row.value === "—") return false;
  return Number.isFinite(Number(row.value));
}

export function peerStatusForRow(
  row: BulletMetric,
  cohortStats: Map<string, PeerStats> | undefined,
): PeerStatusWithNeutral {
  if (!cohortStats) return "neutral";
  const value = Number(row.value);
  if (!Number.isFinite(value)) return "neutral";
  const stats = cohortStats.get(row.metric_key);
  if (!stats) return "neutral";
  const def = BULLET_DEFS_BY_KEY[row.metric_key];
  const higherIsBetter = def?.higher_is_better ?? true;
  return peerStatusVsQuartiles(value, stats, higherIsBetter);
}

export function peerStatusToStatus(p: PeerStatusWithNeutral): Status {
  if (p === "top") return "good";
  if (p === "bottom") return "bad";
  if (p === "in_pack") return "warn";
  return "neutral";
}

export function rowStatus(
  row: BulletMetric,
  cohortStats: Map<string, PeerStats> | undefined,
): Status {
  return peerStatusToStatus(peerStatusForRow(row, cohortStats));
}

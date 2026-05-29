export type PeerStats = {
  p25: number
  p50: number
  p75: number
  min: number
  max: number
  n: number
}

export type PeerStatus = "top" | "in_pack" | "bottom"
export type PeerStatusWithNeutral = PeerStatus | "neutral"

export type PeerCohortLabel = "team" | "org"

export type FocusMode = "all" | "critical" | "rewards" | "neutral"

export function applyFocus(
  status: PeerStatusWithNeutral,
  mode: FocusMode,
): PeerStatusWithNeutral {
  if (mode === "all") return status
  if (mode === "critical") return status === "bottom" ? status : "neutral"
  if (mode === "rewards") return status === "top" ? status : "neutral"
  return "neutral"
}

export const PEER_CELL: Record<PeerStatusWithNeutral, string> = {
  top: "bg-success text-success-foreground",
  bottom: "bg-destructive text-destructive-foreground",
  in_pack: "bg-secondary text-secondary-foreground",
  neutral: "bg-muted text-muted-foreground",
}

export const PEER_TEXT: Record<PeerStatusWithNeutral, string> = {
  top: "text-success",
  bottom: "text-destructive",
  in_pack: "text-foreground",
  neutral: "text-muted-foreground",
}

export const PEER_BORDER: Record<PeerStatusWithNeutral, string> = {
  top: "border-success",
  bottom: "border-destructive",
  in_pack: "border-border",
  neutral: "border-muted",
}

export const PEER_LABEL: Record<PeerStatusWithNeutral, string> = {
  top: "top 25%",
  in_pack: "on par",
  bottom: "bottom 25%",
  neutral: "no peer data",
}

export const PEER_FILL: Record<PeerStatusWithNeutral, string> = {
  top: "bg-success",
  bottom: "bg-destructive",
  in_pack: "bg-muted-foreground/40",
  neutral: "bg-muted-foreground/20",
}

function percentile(sorted: number[], q: number): number {
  if (sorted.length === 0) return 0
  if (sorted.length === 1) return sorted[0]
  const pos = (sorted.length - 1) * q
  const lo = Math.floor(pos)
  const hi = Math.ceil(pos)
  if (lo === hi) return sorted[lo]
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo)
}

export function peerStatsFor(values: number[]): PeerStats | null {
  const filtered = values.filter((v) => Number.isFinite(v))
  if (filtered.length === 0) return null
  const sorted = [...filtered].sort((a, b) => a - b)
  return {
    p25: percentile(sorted, 0.25),
    p50: percentile(sorted, 0.5),
    p75: percentile(sorted, 0.75),
    min: sorted[0],
    max: sorted[sorted.length - 1],
    n: sorted.length,
  }
}

export function peerStatusVsQuartiles(
  value: number,
  stats: Pick<PeerStats, "p25" | "p75">,
  higherIsBetter: boolean
): PeerStatus {
  if (higherIsBetter) {
    if (value >= stats.p75) return "top"
    if (value <= stats.p25) return "bottom"
    return "in_pack"
  }
  if (value <= stats.p25) return "top"
  if (value >= stats.p75) return "bottom"
  return "in_pack"
}

export function peerStatusVsMedian(
  value: number,
  median: number,
  higherIsBetter: boolean,
  toleranceFrac = 0.1
): PeerStatus {
  const tolerance = Math.abs(median) * toleranceFrac
  const diff = value - median
  if (Math.abs(diff) <= tolerance) return "in_pack"
  const aboveMedian = diff > 0
  if (aboveMedian === higherIsBetter) return "top"
  return "bottom"
}

export function relativeGap(
  value: number,
  median: number,
  higherIsBetter: boolean
): number {
  const denom = Math.abs(median) > 1e-9 ? Math.abs(median) : 1
  return higherIsBetter ? (median - value) / denom : (value - median) / denom
}

import type { Status } from "./status"

export type ScoredMetric<T> = {
  row: T
  status: Status
}

export type SectionCounts = {
  good: number
  warn: number
  bad: number
  neutral: number
}

export function sectionCounts<T>(metrics: ScoredMetric<T>[]): SectionCounts {
  const c: SectionCounts = { good: 0, warn: 0, bad: 0, neutral: 0 }
  for (const m of metrics) c[m.status]++
  return c
}

export function aggregateSectionStatus<T>(
  metrics: ScoredMetric<T>[]
): Status {
  if (metrics.length === 0) return "neutral"
  const c = sectionCounts(metrics)
  const evaluated = c.good + c.warn + c.bad
  if (evaluated === 0) return "neutral"
  if (c.bad > 0) return "bad"
  if (c.warn >= c.good) return "warn"
  return "good"
}

export function pickSectionHeadline<T>(
  metrics: ScoredMetric<T>[]
): ScoredMetric<T> | null {
  const priority: Record<Status, number> = {
    bad: 3,
    warn: 2,
    good: 1,
    neutral: 0,
  }
  let best: ScoredMetric<T> | null = null
  let bestPriority = -1
  for (const m of metrics) {
    const p = priority[m.status]
    if (p > bestPriority) {
      best = m
      bestPriority = p
    }
  }
  return best
}

export const SECTION_STRIPE: Record<Status, string> = {
  good: "border-l-success",
  warn: "border-l-warning",
  bad: "border-l-destructive",
  neutral: "border-l-border",
}

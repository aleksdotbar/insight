import type { FocusMode } from "./peers"

export type Status = "good" | "warn" | "bad" | "neutral"

export type StatusDef = {
  good: number
  warn: number
  higher_is_better: boolean
}

export function statusForValue(
  value: number | null | undefined,
  def: StatusDef
): Status {
  if (value == null || Number.isNaN(value)) return "neutral"
  const { good, warn, higher_is_better } = def
  if (higher_is_better) {
    if (value >= good) return "good"
    if (value >= warn) return "warn"
    return "bad"
  }
  if (value <= good) return "good"
  if (value <= warn) return "warn"
  return "bad"
}

export function applyFocusStatus(status: Status, mode: FocusMode): Status {
  if (mode === "all") return status
  if (mode === "critical") return status === "bad" ? status : "neutral"
  if (mode === "rewards") return status === "good" ? status : "neutral"
  return "neutral"
}

export const STATUS_BG: Record<Status, string> = {
  good: "bg-success",
  warn: "bg-warning",
  bad: "bg-destructive",
  neutral: "bg-muted-foreground/40",
}

export const STATUS_TEXT: Record<Status, string> = {
  good: "text-success",
  warn: "text-warning",
  bad: "text-destructive",
  neutral: "text-muted-foreground",
}

export const STATUS_SURFACE: Record<Status, string> = {
  good: "bg-success/15 text-success",
  warn: "bg-warning/15 text-warning",
  bad: "bg-destructive/15 text-destructive",
  neutral: "bg-muted text-muted-foreground",
}


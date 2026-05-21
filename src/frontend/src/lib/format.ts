import { differenceInDays, format, parse, parseISO } from "date-fns";
import { enUS } from "date-fns/locale";

import { LOCALE } from "@/config/constants";

const NF_THOUSANDS = new Intl.NumberFormat(LOCALE);

const INTEGER_UNITS = new Set<string>([
  "",
  "%",
  "count",
  "tasks",
  "meetings",
  "messages",
  "messages/day",
  "emails",
  "files",
  "days",
  "lines",
]);

export const FILLER_UNITS = new Set([
  "count",
  "tasks",
  "messages",
  "meetings",
  "emails",
  "files",
  "lines",
  "LOC",
]);

const EM_DASH = "—";

function isIntegerUnit(unit?: string): boolean {
  return unit != null && INTEGER_UNITS.has(unit);
}

function trimTrailingZero(s: string): string {
  return s.endsWith(".0") ? s.slice(0, -2) : s;
}

export function isFillerUnit(unit?: string): boolean {
  return unit != null && FILLER_UNITS.has(unit);
}

export function formatNumber(
  v: number | null | undefined,
  unit?: string,
): string {
  if (v == null || Number.isNaN(v)) return EM_DASH;
  const abs = Math.abs(v);
  const integer = isIntegerUnit(unit);
  if (abs >= 10_000) return `${trimTrailingZero((v / 1000).toFixed(1))}k`;
  if (integer) return NF_THOUSANDS.format(Math.round(v));
  if (abs >= 1000) return NF_THOUSANDS.format(Math.round(v));
  if (abs >= 100) return v.toFixed(0);
  return trimTrailingZero(v.toFixed(1));
}

export function formatNumberWithUnit(
  v: number | null | undefined,
  unit?: string,
): string {
  const s = formatNumber(v, unit);
  if (!unit || s === EM_DASH) return s;
  if (FILLER_UNITS.has(unit)) return s;
  return unit === "%" ? `${s}%` : `${s} ${unit}`;
}

export function formatPercent(v: number, decimals = 0): string {
  return `${v.toFixed(decimals)}%`;
}

export function formatHours(v: number, decimals = 0): string {
  return `${v.toFixed(decimals)}h`;
}

export function formatPp(diff: number, decimals = 1): string {
  const sign = diff > 0 ? "+" : diff < 0 ? "-" : "";
  return `${sign}${Math.abs(diff).toFixed(decimals)} pp`;
}

export function formatCurrencyCompact(v: number): string {
  const abs = Math.abs(v);
  if (abs >= 1_000_000) return `$${(v / 1_000_000).toFixed(2)}M`;
  if (abs >= 1_000) return `$${(v / 1_000).toFixed(0)}k`;
  return `$${v.toFixed(0)}`;
}

export function formatWinRate(won: number, closed: number): string {
  if (closed <= 0) return "—";
  const pct = (won / closed) * 100;
  const fmt = NF_THOUSANDS.format(Math.round(won));
  const tot = NF_THOUSANDS.format(Math.round(closed));
  return `${pct.toFixed(0)}% (${fmt}/${tot})`;
}

export function formatPeriodProgress(from: string, to: string): string {
  // `new Date("YYYY-MM-DD")` parses as UTC midnight, but `new Date()` is the
  // local clock — subtracting them produces off-by-one results for users
  // outside UTC near the day boundary. Route everything through the existing
  // local-aware helpers (`parseISODate` + `toLocalISODate`) so all three
  // dates share the same reference frame.
  const totalDays = Math.max(1, diffDaysInclusive(from, to));
  const todayIso = toLocalISODate(new Date());
  const elapsedRaw = diffDaysInclusive(from, todayIso);
  const elapsedDays = Math.max(0, Math.min(totalDays, elapsedRaw));
  return `Day ${elapsedDays} of ${totalDays}`;
}

export function formatDeltaPct(cur: number, prev: number): string | null {
  if (prev <= 0) return null;
  const diff = ((cur - prev) / prev) * 100;
  const sign = diff >= 0 ? "+" : "";
  return `${sign}${diff.toFixed(0)}%`;
}

export function toLocalISODate(d: Date): string {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

export function parseISODate(iso: string): Date {
  return parse(iso, "yyyy-MM-dd", new Date());
}

export function formatDate(iso: string, pattern = "d MMM"): string {
  return format(parseISO(iso), pattern, { locale: enUS });
}

export function formatRange(from: string, to: string): string {
  const days = diffDaysInclusive(from, to);
  return `${formatDate(from)} – ${formatDate(to)} (${days}d)`;
}

export function diffDaysInclusive(from: string, to: string): number {
  return differenceInDays(parseISODate(to), parseISODate(from)) + 1;
}

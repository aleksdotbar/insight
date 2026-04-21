/**
 * Date-range resolution — maps a period preset or a user-picked custom range
 * to a {from, to} ISO pair and builds OData $filter expressions.
 *
 * Local-timezone aware: computes the range in the user's local timezone so a
 * "week" means 7 local days (not 7 UTC days). ISO output is YYYY-MM-DD without
 * timezone suffix — the backend filters by date string.
 */

import type { PeriodValue, CustomRange } from '../types';

export type DateRange = { from: string; to: string };

const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

function toISODate(d: Date): string {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

/** Local-midnight today. */
function localToday(): Date {
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  return today;
}

/**
 * Subtract `months` from a date, clamping the day so month-end dates don't
 * roll into the next month. JS's `setMonth(-1)` on 2026-03-31 would land on
 * 2026-03-03 (there's no Feb 31); this clamps to 2026-02-28 instead.
 */
function subtractMonths(d: Date, months: number): Date {
  const out = new Date(d);
  const targetMonth = out.getMonth() - months;
  const originalDay = out.getDate();
  out.setDate(1);
  out.setMonth(targetMonth);
  // Last day of the target month.
  const lastDay = new Date(out.getFullYear(), out.getMonth() + 1, 0).getDate();
  out.setDate(Math.min(originalDay, lastDay));
  return out;
}

/** Returns { from, to } date strings for the given period ending today. */
export function periodToDateRange(period: PeriodValue): DateRange {
  const today = localToday();
  let from: Date;

  switch (period) {
    case 'week':    from = new Date(today); from.setDate(from.getDate() - 7); break;
    case 'month':   from = subtractMonths(today, 1); break;
    case 'quarter': from = subtractMonths(today, 3); break;
    case 'year': {
      from = new Date(today);
      from.setFullYear(from.getFullYear() - 1);
      break;
    }
  }

  return { from: toISODate(from), to: toISODate(today) };
}

/**
 * Validate that `range.from` and `range.to` are YYYY-MM-DD and ordered
 * `from < to`. Throws on malformed input so a tampered or corrupt custom
 * range cannot slip into an OData $filter and inject predicates.
 */
function assertDateRange(range: DateRange): void {
  if (
    !ISO_DATE_RE.test(range.from) ||
    !ISO_DATE_RE.test(range.to) ||
    range.from >= range.to
  ) {
    throw new Error(`Invalid date range: from=${range.from} to=${range.to}`);
  }
}

/**
 * Resolves the effective date range: custom range if the user picked one,
 * otherwise the preset period's range.
 *
 * Custom ranges are validated as YYYY-MM-DD / ordered to prevent malformed
 * input reaching the OData filter; preset ranges are trusted (they're built
 * from `new Date()` locally).
 */
export function resolveDateRange(
  period: PeriodValue,
  customRange: CustomRange | null,
): DateRange {
  if (customRange) {
    const range: DateRange = { from: customRange.from, to: customRange.to };
    assertDateRange(range);
    return range;
  }
  return periodToDateRange(period);
}

/** Builds an OData $filter expression for a metric_date range. */
export function odataDateFilter(range: DateRange): string {
  return `metric_date ge '${range.from}' and metric_date lt '${range.to}'`;
}

/** Escape a value for use inside OData single-quoted string literals. */
export function odataEscapeValue(value: string): string {
  return value.replace(/'/g, "''");
}

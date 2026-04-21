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

/** Returns { from, to } date strings for the given period ending today. */
export function periodToDateRange(period: PeriodValue): DateRange {
  const today = localToday();
  const from = new Date(today);

  switch (period) {
    case 'week':    from.setDate(from.getDate() - 7);          break;
    case 'month':   from.setMonth(from.getMonth() - 1);        break;
    case 'quarter': from.setMonth(from.getMonth() - 3);        break;
    case 'year':    from.setFullYear(from.getFullYear() - 1);  break;
  }

  return { from: toISODate(from), to: toISODate(today) };
}

/**
 * Resolves the effective date range: custom range if the user picked one,
 * otherwise the preset period's range.
 */
export function resolveDateRange(
  period: PeriodValue,
  customRange: CustomRange | null,
): DateRange {
  if (customRange) return { from: customRange.from, to: customRange.to };
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

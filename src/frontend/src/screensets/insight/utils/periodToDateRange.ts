/**
 * periodToDateRange — maps a PeriodValue to an ISO date range {from, to}.
 * Used to build OData $filter date expressions for the Analytics API.
 */

import type { PeriodValue } from '../types';

function toISODate(d: Date): string {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

/** Returns { from, to } ISO date strings for the given period ending today. */
export function periodToDateRange(period: PeriodValue): { from: string; to: string } {
  const today = new Date();
  today.setUTCHours(0, 0, 0, 0);
  const from = new Date(today);

  switch (period) {
    case 'week':    from.setUTCDate(from.getUTCDate() - 7);              break;
    case 'month':   from.setUTCMonth(from.getUTCMonth() - 1);            break;
    case 'quarter': from.setUTCMonth(from.getUTCMonth() - 3);            break;
    case 'year':    from.setUTCFullYear(from.getUTCFullYear() - 1);      break;
  }

  return { from: toISODate(from), to: toISODate(today) };
}

/** Builds an OData $filter expression for metric_date within the given period. */
export function odataDateFilter(period: PeriodValue): string {
  const { from, to } = periodToDateRange(period);
  return `metric_date ge '${from}' and metric_date lt '${to}'`;
}

/** Escape a value for use inside OData single-quoted string literals. */
export function odataEscapeValue(value: string): string {
  return value.replace(/'/g, "''");
}

/**
 * Mock-side inverse: infer PeriodValue from an OData $filter string.
 * Used so a single mock handler can return period-appropriate data.
 */
export function inferPeriodFromODataFilter(filter: string): PeriodValue {
  const match = /metric_date ge '(\d{4}-\d{2}-\d{2})'/.exec(filter);
  if (!match) return 'month';
  const days = Math.round(
    (Date.now() - new Date(match[1]).getTime()) / 86_400_000,
  );
  if (days <= 10)  return 'week';
  if (days <= 35)  return 'month';
  if (days <= 100) return 'quarter';
  return 'year';
}

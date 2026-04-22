/**
 * Mock-side helper: infer a `PeriodValue` from an OData $filter string.
 *
 * Used only by mock handlers that want to return period-appropriate fake data
 * without taking period as an explicit argument. Lives in the mocks/ folder
 * so prod utils do not expose mock-specific heuristics.
 */

import type { PeriodValue } from '../../types';

export function inferPeriodFromODataFilter(filter: string): PeriodValue {
  const match = /metric_date ge '(\d{4}-\d{2}-\d{2})'/.exec(filter);
  if (!match) return 'month';
  const days = Math.round(
    (Date.now() - new Date(match[1]).getTime()) / 86_400_000,
  );
  if (days <= 10) return 'week';
  if (days <= 35) return 'month';
  if (days <= 100) return 'quarter';
  return 'year';
}

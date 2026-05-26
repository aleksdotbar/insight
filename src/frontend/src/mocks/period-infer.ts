import type { PeriodValue } from "@/types/insight";

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

/**
 * Shared helper for trailing period-suffix rendering (`/ wk`, `/ mo`, ...)
 * used by both KpiStrip and BulletChart.
 *
 * Units that are already rates (%, hours, ratios, average replies) read
 * weird with a period suffix — "% / mo" — so they're suppressed here.
 */

import { toLower } from 'lodash';

const SUPPRESS_SUFFIX_UNITS = ['%', '\u00d7', 'h', 'avg replies', 'avg', '/mo'];

const PERIOD_SUFFIX: Record<string, string> = {
  week:    '/ wk',
  month:   '/ mo',
  quarter: '/ qtr',
  year:    '/ yr',
};

export function getPeriodSuffix(unit: string | undefined, period?: string): string {
  if (!period || !unit) return '';
  // lodash `toLower` preserves punctuation (unlike `lowerCase` which would
  // strip `/` and break the `/mo` match); lint forbids the native method.
  const u = toLower(unit);
  if (SUPPRESS_SUFFIX_UNITS.some((s) => u.includes(s))) return '';
  return PERIOD_SUFFIX[period] ?? '';
}

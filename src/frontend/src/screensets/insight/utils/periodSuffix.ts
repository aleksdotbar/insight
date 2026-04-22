/**
 * Shared helper for trailing period-suffix rendering (`/ wk`, `/ mo`, ...)
 * used by both KpiStrip and BulletChart.
 *
 * Units that are already rates (%, hours, ratios, average replies) read
 * weird with a period suffix — "% / mo" — so they're suppressed here.
 */

import { toLower } from 'lodash';

const SUPPRESS_SUFFIX_UNITS = ['%', '\u00d7', 'h', 'avg replies', 'avg', '/mo'];

const PERIOD_SUFFIX = {
  week:    '/ wk',
  month:   '/ mo',
  quarter: '/ qtr',
  year:    '/ yr',
} as const;

type PeriodSuffixKey = keyof typeof PERIOD_SUFFIX;

export function getPeriodSuffix(unit: string | undefined, period?: string): string {
  if (!period || !unit) return '';
  // lodash `toLower` preserves punctuation (unlike `lowerCase` which would
  // strip `/` and break the `/mo` match); lint forbids the native method.
  const u = toLower(unit);
  if (SUPPRESS_SUFFIX_UNITS.some((s) => u.includes(s))) return '';
  // Own-property guard — `period` is an arbitrary string from the caller,
  // so plain `PERIOD_SUFFIX[period]` could read inherited properties
  // (e.g. `toString`) and return non-string values despite the type.
  return Object.prototype.hasOwnProperty.call(PERIOD_SUFFIX, period)
    ? PERIOD_SUFFIX[period as PeriodSuffixKey]
    : '';
}

// Units that are already rates (%, hours, ratios, average replies) read weird
// with a period suffix — "% / mo" — so they're suppressed.
const SUPPRESS_SUFFIX_UNITS = [
  "%",
  "\u00d7",
  "h",
  "avg replies",
  "avg",
  "/mo",
  "/day",
];

// `Map` avoids both prototype lookups and dynamic-indexing: `.get(arbitrary)`
// returns `string | undefined` without reading inherited properties like
// `toString`, so we don't need a `hasOwn` guard on a plain object.
const PERIOD_SUFFIX = new Map<string, string>([
  ['week',    '/ wk'],
  ['month',   '/ mo'],
  ['quarter', '/ qtr'],
  ['year',    '/ yr'],
]);

export function getPeriodSuffix(unit: string | undefined, period?: string): string {
  if (!period || !unit) return "";
  const u = unit.toLowerCase();
  if (SUPPRESS_SUFFIX_UNITS.some((s) => u.includes(s))) return "";
  return PERIOD_SUFFIX.get(period) ?? "";
}

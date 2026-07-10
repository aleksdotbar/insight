/**
 * Integer percentage shares of a whole that sum to exactly 100 (largest-
 * remainder / Hamilton rounding), so a "parts of a whole" legend never shows
 * 99% or 101%. Parts are assumed non-negative; a non-positive total has no
 * honest share and yields all zeros. Order is preserved (result[i] is the
 * share of values[i]).
 */
export function integerPercentShares(values: number[]): number[] {
  const total = values.reduce((sum, value) => sum + value, 0);
  if (!(total > 0)) return values.map(() => 0);

  const exact = values.map((value) => (value / total) * 100);
  const shares = exact.map((value) => Math.floor(value));
  let remainder = 100 - shares.reduce((sum, value) => sum + value, 0);

  // Hand the leftover points to the largest fractional parts first.
  const byFraction = exact
    .map((value, index) => ({ index, frac: value - Math.floor(value) }))
    .sort((a, b) => b.frac - a.frac);
  for (let i = 0; i < byFraction.length && remainder > 0; i += 1) {
    shares[byFraction[i]!.index] += 1;
    remainder -= 1;
  }
  return shares;
}

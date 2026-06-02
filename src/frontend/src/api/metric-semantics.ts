/**
 * Pure status / health helpers (Refs #79).
 *
 * Per-metric threshold values flow through `useCatalog()` (see
 * `view-configs.ts`); this module owns the rendering rule only. Compile-in
 * defaults were removed in #82 — when the catalog is unavailable the
 * surfaces render skeletons / error states instead of falling back to a
 * synthesized threshold.
 */

// Fractions of headcount — chip status scales with team size instead of
// using a fixed absolute rule that collapses for teams larger than a few
// people.
export const TEAM_HEALTH_THRESHOLDS = {
  /** From this fraction onward, and below `badPct` → status is 'warn'. */
  warnPct: 0.10,
  /** From this fraction onward → status is 'bad'. */
  badPct: 0.25,
} as const;

/**
 * Translate a count of affected members into a status chip, scaled against
 * team size. `count===0` is always 'good' so an all-healthy team still reads
 * 'good' regardless of how small the team is.
 */
export function teamHealthStatus(
  count: number,
  teamSize: number,
): 'good' | 'warn' | 'bad' {
  if (count <= 0 || teamSize <= 0) return 'good';
  const pct = count / teamSize;
  if (pct >= TEAM_HEALTH_THRESHOLDS.badPct) return 'bad';
  if (pct >= TEAM_HEALTH_THRESHOLDS.warnPct) return 'warn';
  return 'good';
}

/**
 * Shared status evaluator — same rule on every screen. `thresholds.good`
 * and `thresholds.warn` come from the catalog (`ResolvedThresholds`) or
 * from a synthesized object built by FE callers; the function is pure and
 * has no knowledge of the catalog.
 */
export function evaluateStatus(
  value: number,
  thresholds: { good: number; warn: number },
  higher_is_better: boolean,
): 'good' | 'warn' | 'bad' {
  if (higher_is_better) {
    if (value >= thresholds.good) return 'good';
    if (value >= thresholds.warn) return 'warn';
    return 'bad';
  }
  if (value <= thresholds.good) return 'good';
  if (value <= thresholds.warn) return 'warn';
  return 'bad';
}

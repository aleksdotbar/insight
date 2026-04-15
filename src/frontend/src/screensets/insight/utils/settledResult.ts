/**
 * Promise.allSettled helpers
 *
 * Utilities for safely extracting values from PromiseSettledResult
 * with fallback defaults. Used by action files to ensure individual
 * API failures do not crash the entire view.
 */

import type { ODataResponse } from '../types';

/**
 * Extract the value from a settled promise result, returning a fallback
 * if the promise was rejected. Logs a warning for rejected promises.
 */
export function settled<T>(
  result: PromiseSettledResult<T>,
  fallback: T,
  label: string,
): T {
  if (result.status === 'fulfilled') return result.value;
  console.warn(`[Insight] ${label} unavailable:`, result.reason);
  return fallback;
}

/**
 * Create an empty ODataResponse with no items.
 * Safe default for any metric query that fails.
 */
export function emptyOdata<T>(): ODataResponse<T> {
  return { items: [], page_info: { has_next: false, cursor: null } };
}

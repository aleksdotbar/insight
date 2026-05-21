import type { ODataResponse } from "@/types/insight";

export function settled<T>(
  result: PromiseSettledResult<T>,
  fallback: T,
  label: string,
): T {
  if (result.status === 'fulfilled') return result.value;
  if (import.meta.env.DEV) {
    console.warn(`[Insight] ${label} unavailable:`, result.reason);
  }
  return fallback;
}

export function emptyOdata<T>(): ODataResponse<T> {
  return { items: [], page_info: { has_next: false, cursor: null } };
}

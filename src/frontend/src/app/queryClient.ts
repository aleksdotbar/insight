/**
 * Single shared TanStack Query client for the app.
 *
 * Defaults are chosen for analytics-heavy, read-only screens:
 * - `staleTime: 5 min` — analytics-api responses are not real-time, so a
 *   modest stale window cuts duplicate fetches when users navigate away
 *   and back without making numbers feel frozen.
 * - `refetchOnWindowFocus: false` — re-querying every metric on tab-focus
 *   is noisy on a dashboard with 6+ panels.
 * - `retry: 1` — analytics-api 5xx is occasional; one retry without a
 *   backoff multiplier is cheap and saves the user a manual refresh.
 *
 * Mutations stay on RQ defaults (no automatic retry) — there are none in
 * the sales dashboard today, but the client is shared app-wide.
 */

import { QueryClient } from '@tanstack/react-query';

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 5 * 60_000,
      refetchOnWindowFocus: false,
      retry: 1,
    },
  },
});

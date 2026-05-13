/**
 * Identity query factory.
 *
 * Sales dashboard uses this directly. Other dashboards continue to load
 * identity through the bootstrap action for now; they can adopt the
 * factory whenever they migrate.
 *
 * `staleTime` is generous (30 min) — identity attributes (job title,
 * supervisor tree, department) are HRIS-driven and rarely change in a
 * single session. Cuts re-fetch on remount when the user navigates away
 * and back to the dashboard.
 */

import { queryOptions } from '@tanstack/react-query';
import { apiRegistry } from '@hai3/react';
import { IdentityApiService } from '@/app/api/IdentityApiService';
import { identityKeys } from './keys';

export const identityQueries = {
  byEmail: (email: string) => queryOptions({
    queryKey: identityKeys.byEmail(email),
    queryFn:  () => apiRegistry.getService(IdentityApiService).getPersonByEmail(email),
    enabled:  !!email,
    staleTime: 30 * 60_000,
  }),
};

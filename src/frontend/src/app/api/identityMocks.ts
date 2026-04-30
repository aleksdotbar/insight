/**
 * Identity Resolution mock map.
 *
 * Mirrors the pattern used by `src/screensets/insight/api/mocks.ts` for the
 * analytics endpoints: the mock map is a separate, dynamically-imported
 * module that the consuming `*ApiService` registers when `mocksEnabled()`
 * is true. Identity tree is sourced from the screenset's `PEOPLE` registry
 * via `buildIdentityTree`, so the demo-tenant hierarchy lives in exactly
 * one place.
 */

import type { MockMap } from '@hai3/react';
import { buildIdentityTree } from '@/screensets/insight/api/mocks/registry';
import type { IdentityPersonRaw } from '@/app/types/identity';

/**
 * Error shaped to look like the real backend's 404 so consumers don't have
 * to special-case the mock. `IdentityApiService.getPersonByEmail` calls
 * `toIdentityPerson(raw)` on the response — returning `null` body would
 * crash there. Throwing mirrors what `RestProtocol` surfaces from a real
 * 404 response.
 */
class MockNotFoundError extends Error {
  status = 404;
  constructor(email: string) {
    super(`Identity mock: no person matches '${email}'`);
    this.name = 'MockNotFoundError';
  }
}

export const identityMockMap: MockMap = {
  'GET /api/identity-resolution/v1/persons/:email': (
    _body: unknown,
    params?: Record<string, string>,
  ): IdentityPersonRaw => {
    const email = decodeURIComponent(params?.email ?? '');
    const tree = buildIdentityTree(email);
    if (!tree) throw new MockNotFoundError(email);
    return tree as IdentityPersonRaw;
  },
};

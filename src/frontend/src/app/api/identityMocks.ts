/**
 * Identity Resolution mock map.
 *
 * Identity tree sourced from screenset's `PEOPLE` registry via
 * `buildIdentityTree`. One mock entry per known email — hai3's
 * `RestMockPlugin` matches `:email` patterns but does NOT pass extracted
 * params to the factory (only body), so the previous `:email`-placeholder
 * implementation always saw `params.email === undefined` and threw.
 * Per-email keys avoid that limitation.
 */

import type { MockMap } from '@hai3/react';
import { PEOPLE, buildIdentityTree } from '@/screensets/insight/api/mocks/registry';
import type { IdentityPersonRaw } from '@/app/types/identity';

class MockNotFoundError extends Error {
  status = 404;
  constructor(email: string) {
    super(`Identity mock: no person matches '${email}'`);
    this.name = 'MockNotFoundError';
  }
}

const map: Record<string, () => IdentityPersonRaw> = {};
for (const p of PEOPLE) {
  const factory = (): IdentityPersonRaw => {
    const tree = buildIdentityTree(p.person_id);
    if (!tree) throw new MockNotFoundError(p.person_id);
    return tree as IdentityPersonRaw;
  };
  // hai3 RestMockPlugin matches against `context.url`, which may be either
  // the absolute request URL or the baseURL-relative path depending on how
  // axios is configured. Register both so neither path falls through to the
  // real backend (analytics mocks use the same dual-key strategy).
  for (const id of [encodeURIComponent(p.person_id), p.person_id]) {
    map[`GET /api/identity-resolution/v1/persons/${id}`] = factory;
    map[`GET /persons/${id}`] = factory;
  }
}

export const identityMockMap: MockMap = map;

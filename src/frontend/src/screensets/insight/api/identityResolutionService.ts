/**
 * IdentityResolutionService
 *
 * Fetches person profiles from the Identity Resolution Service.
 * Spec: GET /api/identity-resolution/v1/persons/{id}
 * See: docs/components/backend/specs/analytics-views-api.md §4.6
 */

import { BaseApiService, RestProtocol, RestMockPlugin, apiRegistry } from '@hai3/react';
import type { MockMap } from '@hai3/react';
import { AuthPlugin } from '@/app/plugins/AuthPlugin';
import type { PersonData } from '../types';

// ---------------------------------------------------------------------------
// Mock data — names/seniority aligned with TEAM_MEMBERS_MONTH in team-view-base.ts
// ---------------------------------------------------------------------------

// Derived from the single source of truth: registry.ts
import { PEOPLE } from './mocks/registry';

const MOCK_PERSONS: Record<string, PersonData> = Object.fromEntries([
  // p0 — executive persona (not a team member)
  ['p0', { person_id: 'p0', name: 'David Park', role: 'Engineering Director', seniority: 'Lead' }],
  // All people from registry
  ...PEOPLE.map((p) => [
    p.person_id,
    { person_id: p.person_id, name: p.name, role: p.role, seniority: p.seniority },
  ]),
]);

const MOCK_IDENTITY_MAP: MockMap = Object.fromEntries(
  Object.entries(MOCK_PERSONS).map(([id, person]): [string, () => PersonData] => [
    `GET /api/identity-resolution/v1/persons/${id}`,
    () => person,
  ]),
);

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

export class IdentityResolutionService extends BaseApiService {
  constructor() {
    const restProtocol = new RestProtocol();
    super({ baseURL: '/api/identity-resolution/v1' }, restProtocol);
    this.registerPlugin(
      restProtocol,
      new RestMockPlugin({ mockMap: MOCK_IDENTITY_MAP, delay: 50 }),
    );
    restProtocol.plugins.add(new AuthPlugin());
  }

  async getPerson(personId: string): Promise<PersonData> {
    return this.protocol(RestProtocol).get<PersonData>(`/persons/${personId}`);
  }
}

apiRegistry.register(IdentityResolutionService);

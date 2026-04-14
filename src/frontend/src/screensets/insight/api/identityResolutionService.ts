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

const MOCK_PERSONS: Record<string, PersonData> = {
  // p0 — executive persona (not a team member, has no IC metrics row)
  p0:  { person_id: 'p0',  name: 'David Park',    role: 'Engineering Director',   seniority: 'Lead'   },
  // p1–p12 match TEAM_MEMBERS_MONTH exactly
  p1:  { person_id: 'p1',  name: 'Alice Kim',     role: 'Senior Software Engineer', seniority: 'Senior' },
  p2:  { person_id: 'p2',  name: 'Ben Clarke',    role: 'Software Engineer',        seniority: 'Middle' },
  p3:  { person_id: 'p3',  name: 'Tom Sullivan',  role: 'Software Engineer',        seniority: 'Middle' },
  p4:  { person_id: 'p4',  name: 'Sara Jansen',   role: 'Junior Software Engineer', seniority: 'Junior' },
  p5:  { person_id: 'p5',  name: 'Mike Chen',     role: 'Senior Software Engineer', seniority: 'Senior' },
  p6:  { person_id: 'p6',  name: 'Emma Davis',    role: 'Tech Lead',                seniority: 'Lead'   },
  p7:  { person_id: 'p7',  name: 'Carlos Ruiz',   role: 'Software Engineer',        seniority: 'Middle' },
  p8:  { person_id: 'p8',  name: 'Priya Patel',   role: 'Senior Software Engineer', seniority: 'Senior' },
  p9:  { person_id: 'p9',  name: 'James Wilson',  role: 'Junior Software Engineer', seniority: 'Junior' },
  p10: { person_id: 'p10', name: 'Ana Kovac',     role: 'Software Engineer',        seniority: 'Middle' },
  p11: { person_id: 'p11', name: 'Leo Zhang',     role: 'Senior Software Engineer', seniority: 'Senior' },
  p12: { person_id: 'p12', name: 'Nina Ross',     role: 'Junior Software Engineer', seniority: 'Junior' },
};

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

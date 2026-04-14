/**
 * Identity API Service
 * Fetches current user identity from the identity service
 */

import { BaseApiService, RestProtocol, RestMockPlugin } from '@hai3/react';
import { AuthPlugin } from '@/app/plugins/AuthPlugin';
import type { PersonData } from '@/app/types/identity';

const identityMockMap = {
  'GET /api/v1/identity/persons/me': (): PersonData => ({
    person_id: 'p1',
    name: 'Alice Kim',
    role: 'executive',
    seniority: 'senior',
  }),
};

export class IdentityApiService extends BaseApiService {
  constructor() {
    const restProtocol = new RestProtocol({ timeout: 10000 });

    super({ baseURL: '/api/v1/identity' }, restProtocol);

    this.registerPlugin(
      restProtocol,
      new RestMockPlugin({ mockMap: identityMockMap, delay: 50 })
    );

    // Inject auth headers on every request
    restProtocol.plugins.add(new AuthPlugin());
  }

  async getMe(): Promise<PersonData> {
    return this.protocol(RestProtocol).get<PersonData>('/persons/me');
  }
}

/**
 * IdentityResolutionService
 *
 * Fetches person profiles from the Identity Resolution Service.
 * GET /api/identity-resolution/v1/persons/{email}
 */

import { BaseApiService, RestProtocol, apiRegistry } from '@hai3/react';
import { AuthPlugin } from '@/app/plugins/AuthPlugin';
import type { IdentityPerson, IdentityPersonRaw } from '@/app/types/identity';
import { toIdentityPerson } from '@/app/types/identity';

export class IdentityResolutionService extends BaseApiService {
  constructor() {
    const restProtocol = new RestProtocol();
    super({ baseURL: '/api/identity-resolution/v1' }, restProtocol);
    restProtocol.plugins.add(new AuthPlugin());
  }

  async getPerson(email: string): Promise<IdentityPerson> {
    const raw = await this.protocol(RestProtocol).get<IdentityPersonRaw>(`/persons/${encodeURIComponent(email)}`);
    return toIdentityPerson(raw);
  }
}

apiRegistry.register(IdentityResolutionService);

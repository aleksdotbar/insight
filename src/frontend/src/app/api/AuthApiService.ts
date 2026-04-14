/**
 * Auth API Service
 * Fetches OIDC bootstrap config from the API gateway (public endpoint, no token needed)
 */

import { BaseApiService, RestProtocol, RestMockPlugin } from '@hai3/react';
import type { OidcConfig } from '@/app/types/auth';

const authMockMap = {
  'GET /auth/config': (): OidcConfig => ({
    issuer_url: 'https://integrator-4985807.okta.com',
    client_id: '0oa11soqqraMNbjZK698',
    redirect_uri: `${window.location.origin}/callback`,
    scopes: ['openid', 'profile', 'email'],
    response_type: 'code',
  }),
};

export class AuthApiService extends BaseApiService {
  constructor() {
    const restProtocol = new RestProtocol({ timeout: 10000 });

    super({ baseURL: '/' }, restProtocol);

    this.registerPlugin(
      restProtocol,
      new RestMockPlugin({ mockMap: authMockMap, delay: 50 })
    );
  }

  async getConfig(): Promise<OidcConfig> {
    return this.protocol(RestProtocol).get<OidcConfig>('/auth/config');
  }
}

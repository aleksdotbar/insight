/**
 * Auth API Service
 * Fetches OIDC bootstrap config from the API gateway (public endpoint, no token needed)
 */

import { BaseApiService, RestProtocol, RestMockPlugin, apiRegistry } from '@hai3/react';
import type { OidcConfig } from '@/app/types/auth';

// Runtime OIDC config injected by Docker entrypoint via window.__OIDC_CONFIG__
declare global { interface Window { __OIDC_CONFIG__?: Partial<OidcConfig> } }
const runtimeConfig = window.__OIDC_CONFIG__;

const authMockMap = {
  'GET /api/v1/auth/config': (): OidcConfig => ({
    issuer_url: runtimeConfig?.issuer_url ?? '',
    client_id: runtimeConfig?.client_id ?? '',
    redirect_uri: `${window.location.origin}/callback`,
    scopes: ['openid', 'profile', 'email'],
    response_type: 'code',
  }),
};

export class AuthApiService extends BaseApiService {
  constructor() {
    const restProtocol = new RestProtocol({ timeout: 10000 });

    super({ baseURL: '/api/v1' }, restProtocol);

    this.registerPlugin(
      restProtocol,
      new RestMockPlugin({ mockMap: authMockMap, delay: 50 })
    );
  }

  async getConfig(): Promise<OidcConfig> {
    return this.protocol(RestProtocol).get<OidcConfig>('/auth/config');
  }
}

apiRegistry.register(AuthApiService);

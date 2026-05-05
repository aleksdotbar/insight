/**
 * Auth API Service
 * Fetches OIDC bootstrap config from the API gateway (public endpoint, no token needed).
 *
 * NOTE: the RestMockPlugin here is NOT a dev-only mock — it's the primary
 * production path. OIDC config is injected at container start as
 * `window.__OIDC_CONFIG__` (docker entrypoint rewrites index.html) and this
 * "mock" handler just reads it back synchronously so the rest of the app
 * can consume it through the normal api-service contract. There is no real
 * `/api/v1/auth/config` backend endpoint. This is deliberately NOT gated
 * by VITE_ENABLE_MOCKS — disabling it would break auth in every deploy.
 */

import { BaseApiService, RestProtocol, RestMockPlugin, apiRegistry } from '@hai3/react';
import type { OidcConfig } from '@/app/types/auth';

// Runtime OIDC config injected by Docker entrypoint via window.__OIDC_CONFIG__.
// Shape differs from OidcConfig: scopes is a space-separated string here
// (easier for shell to emit safely), split into string[] before consumption.
type RuntimeOidcConfig = {
  issuer_url?: string;
  client_id?: string;
  scopes?: string;
};
declare global { interface Window { __OIDC_CONFIG__?: RuntimeOidcConfig } }
const runtimeConfig = window.__OIDC_CONFIG__;

const authConfigMap = {
  'GET /api/v1/auth/config': (): OidcConfig => ({
    issuer_url: runtimeConfig?.issuer_url ?? '',
    client_id: runtimeConfig?.client_id ?? '',
    redirect_uri: `${window.location.origin}/callback`,
    // Scopes are IdP-specific (Entra wants api://<clientId>/Access.Default,
    // Okta uses bare names). Configured per-deploy via OIDC_SCOPES env →
    // window.__OIDC_CONFIG__.scopes (space-separated string).
    scopes: (runtimeConfig?.scopes ?? '').split(/\s+/).filter(Boolean),
    response_type: 'code',
  }),
};

export class AuthApiService extends BaseApiService {
  constructor() {
    const restProtocol = new RestProtocol({ timeout: 10000 });

    super({ baseURL: '/api/v1' }, restProtocol);

    // See comment at top of file — this RestMockPlugin is the prod OIDC
    // config handler, not fake data. Do not gate it behind mocksEnabled().
    this.registerPlugin(
      restProtocol,
      new RestMockPlugin({ mockMap: authConfigMap, delay: 50 })
    );
  }

  async getConfig(): Promise<OidcConfig> {
    return this.protocol(RestProtocol).get<OidcConfig>('/auth/config');
  }
}

apiRegistry.register(AuthApiService);

/**
 * Accounts Domain - API Service
 * Service for accounts domain (users, tenants, authentication, permissions)
 *
 * Application-specific service (copied from CLI template)
 */

import { BaseApiService, RestProtocol, RestMockPlugin, apiRegistry } from '@hai3/react';
import type { GetCurrentUserResponse } from './types';
import { mocksEnabled } from '@/app/config/mocksEnabled';

/**
 * Accounts API Service
 * Manages accounts domain endpoints:
 * - User management (current user, profile, preferences)
 * - Tenant management (current tenant, switching)
 * - Authentication (login, logout, tokens)
 * - Permissions and roles
 *
 * Mocks are dynamically loaded only when VITE_ENABLE_MOCKS=true in dev;
 * prod bundles never include the "Demo User" payload from ./mocks.ts.
 */
export class AccountsApiService extends BaseApiService {
  constructor() {
    const restProtocol = new RestProtocol({
      timeout: 30000,
    });

    super({ baseURL: '/api/accounts' }, restProtocol);

    if (mocksEnabled()) {
      void import('./mocks').then(({ accountsMockMap }) => {
        this.registerPlugin(
          restProtocol,
          new RestMockPlugin({
            mockMap: accountsMockMap,
            delay: 100,
          }),
        );
      });
    }
  }

  /**
   * Get current authenticated user
   */
  async getCurrentUser(): Promise<GetCurrentUserResponse> {
    return this.protocol(RestProtocol).get<GetCurrentUserResponse>('/user/current');
  }
}

apiRegistry.register(AccountsApiService);

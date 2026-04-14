/**
 * auth API Service
 * Domain-specific API service for this screenset
 */

import { BaseApiService, RestProtocol, apiRegistry } from '@hai3/react';

/**
 * API request/response types
 * Add your API types here
 */

/**
 * auth API Service
 * Extends BaseApiService with domain-specific methods
 */
export class AuthApiService extends BaseApiService {
  constructor() {
    super(
      { baseURL: '/api/auth' },
      new RestProtocol()
    );
  }

  /**
   * Add your API methods here
   *
   * Example:
   * async getItems(): Promise<Item[]> {
   *   return this.protocol(RestProtocol).get<Item[]>('/items');
   * }
   */
}

// Register API service using class-based registration
apiRegistry.register(AuthApiService);

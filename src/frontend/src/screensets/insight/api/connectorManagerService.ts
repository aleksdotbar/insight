/**
 * ConnectorManagerService
 *
 * Fetches connector availability from the Connector Manager API.
 * Spec: GET /api/connectors/v1/connections/{id}/status
 * See: docs/components/backend/specs/analytics-views-api.md §8
 */

import { BaseApiService, RestProtocol, RestMockPlugin, apiRegistry } from '@hai3/react';
import type { MockMap } from '@hai3/react';
import { AuthPlugin } from '@/app/plugins/AuthPlugin';
import type { DataAvailability, ConnectorAvailability, ConnectorStatus } from '../types';

type ConnectionId = keyof DataAvailability;

const CONNECTION_IDS: ConnectionId[] = ['git', 'tasks', 'ci', 'comms', 'hr', 'ai'];

// ---------------------------------------------------------------------------
// Mock data
// ---------------------------------------------------------------------------

const MOCK_CONNECTOR_STATUS: MockMap = Object.fromEntries(
  CONNECTION_IDS.map((id): [string, () => ConnectorStatus] => [
    `GET /api/connectors/v1/connections/${id}/status`,
    () => ({ id, name: id, status: 'available' as ConnectorAvailability }),
  ]),
);

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

export class ConnectorManagerService extends BaseApiService {
  constructor() {
    const restProtocol = new RestProtocol();
    super({ baseURL: '/api/connectors/v1' }, restProtocol);
    this.registerPlugin(
      restProtocol,
      new RestMockPlugin({ mockMap: MOCK_CONNECTOR_STATUS, delay: 50 }),
    );
    restProtocol.plugins.add(new AuthPlugin());
  }

  /** Fetches availability for all known connectors in parallel. */
  async getDataAvailability(): Promise<DataAvailability> {
    const settled = await Promise.allSettled(
      CONNECTION_IDS.map((id) =>
        this.protocol(RestProtocol).get<ConnectorStatus>(`/connections/${id}/status`),
      ),
    );
    return Object.fromEntries(
      CONNECTION_IDS.map((id, i) => {
        const result = settled[i];
        const status: ConnectorAvailability =
          result?.status === 'fulfilled' ? result.value.status : 'no-connector';
        return [id, status];
      }),
    ) as DataAvailability;
  }
}

apiRegistry.register(ConnectorManagerService);

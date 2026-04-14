/**
 * Executive View Events
 * Domain-specific events for executive view screen
 */

import '@hai3/react';
import { INSIGHT_SCREENSET_ID } from '../ids';
import type { ExecViewData, DataAvailability } from '../types';

const DOMAIN_ID = 'executiveView';

/**
 * Events enum
 */
export enum ExecutiveViewEvents {
  ExecutiveViewLoadStarted        = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/loadStarted`,
  ExecutiveViewLoaded             = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/loaded`,
  ExecutiveViewAvailabilityLoaded = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/availabilityLoaded`,
  ExecutiveViewLoadFailed         = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/loadFailed`,
}

/**
 * Module augmentation for type-safe event payloads
 */
declare module '@hai3/react' {
  interface EventPayloadMap {
    [ExecutiveViewEvents.ExecutiveViewLoadStarted]:        void;
    [ExecutiveViewEvents.ExecutiveViewLoaded]:             ExecViewData;
    [ExecutiveViewEvents.ExecutiveViewAvailabilityLoaded]: DataAvailability;
    [ExecutiveViewEvents.ExecutiveViewLoadFailed]:         string;
  }
}

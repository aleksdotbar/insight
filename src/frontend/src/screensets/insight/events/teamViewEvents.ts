/**
 * Team View Events
 * Domain-specific events for team view screen
 */

import '@hai3/react';
import { INSIGHT_SCREENSET_ID } from '../ids';
import type { TeamViewData } from '../types';

const DOMAIN_ID = 'teamView';

/**
 * Events enum
 */
export enum TeamViewEvents {
  TeamViewLoadStarted = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/loadStarted`,
  TeamViewLoaded = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/loaded`,
  TeamViewLoadFailed = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/loadFailed`,
}

/**
 * Module augmentation for type-safe event payloads
 */
declare module '@hai3/react' {
  interface EventPayloadMap {
    [TeamViewEvents.TeamViewLoadStarted]: void;
    [TeamViewEvents.TeamViewLoaded]: TeamViewData;
    [TeamViewEvents.TeamViewLoadFailed]: string;
  }
}

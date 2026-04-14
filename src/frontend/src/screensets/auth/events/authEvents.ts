/**
 * auth Events
 * Domain-specific events for this screenset
 */

import '@hai3/react';
import { AUTH_SCREENSET_ID } from '../ids';

const DOMAIN_ID = 'auth';

/**
 * Events enum
 * Add your events here following the pattern:
 * EventName = `${AUTH_SCREENSET_ID}/${DOMAIN_ID}/eventName`
 */
export enum AuthEvents {
  // Example: Selected = `${AUTH_SCREENSET_ID}/${DOMAIN_ID}/selected`,
}

// These are used in the event enum pattern above
void AUTH_SCREENSET_ID;
void DOMAIN_ID;

/**
 * Module augmentation for type-safe event payloads
 * Add your event payload types here
 */
declare module '@hai3/react' {
  interface EventPayloadMap {
    /** Placeholder - remove when adding real events */
    'auth/_placeholder'?: never;
    // Example: [AuthEvents.Selected]: { id: string };
  }
}

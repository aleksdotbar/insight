/**
 * insight Events
 * Domain-specific events for this screenset
 */

import '@hai3/react';
import { INSIGHT_SCREENSET_ID } from '../ids';
import type { DashboardData, SpeedData } from '../types';

const DOMAIN_ID = 'insight';

/**
 * Events enum
 */
export enum InsightEvents {
  DashboardLoaded = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/dashboardLoaded`,
  SpeedLoaded = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/speedLoaded`,
}

/**
 * Module augmentation for type-safe event payloads
 */
declare module '@hai3/react' {
  interface EventPayloadMap {
    [InsightEvents.DashboardLoaded]: DashboardData;
    [InsightEvents.SpeedLoaded]: SpeedData;
  }
}

/**
 * Insight UI Events
 * Cross-screen UI preference events (viewMode, etc).
 */

import '@hai3/react';
import { INSIGHT_SCREENSET_ID } from '../ids';
import type { ViewMode } from '../types';

const DOMAIN_ID = 'insightUi';

export enum InsightUiEvents {
  ViewModeChanged = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/viewModeChanged`,
}

declare module '@hai3/react' {
  interface EventPayloadMap {
    [InsightUiEvents.ViewModeChanged]: ViewMode;
  }
}

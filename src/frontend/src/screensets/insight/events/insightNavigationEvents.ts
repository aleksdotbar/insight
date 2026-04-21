/**
 * Insight Navigation Events
 *
 * Typed replacement for the layout-level `'layout/menu/itemParam'` string
 * event. Keeps the Menu / Layout layer domain-neutral: they emit a neutral
 * "a param-bearing item was clicked" signal, and insight's own effect layer
 * decodes it into a proper SelectionRequested with a TeamRef shape.
 */

import '@hai3/react';
import {
  INSIGHT_SCREENSET_ID,
  IC_DASHBOARD_SCREEN_ID,
  TEAM_VIEW_SCREEN_ID,
  MY_DASHBOARD_SCREEN_ID,
  EXECUTIVE_VIEW_SCREEN_ID,
} from '../ids';
import type { TeamRef } from '../slices/userContextSlice';

const DOMAIN_ID = 'navigation';

export enum InsightNavigationEvents {
  SelectionRequested = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/selectionRequested`,
}

/**
 * Payload is a discriminated union keyed by `screen`, so the reducer does not
 * need to second-guess what `param` means for each screen.
 */
export type SelectionRequest =
  | { screen: typeof IC_DASHBOARD_SCREEN_ID; personId: string }
  | { screen: typeof TEAM_VIEW_SCREEN_ID; teamRef: TeamRef }
  | { screen: typeof MY_DASHBOARD_SCREEN_ID }
  | { screen: typeof EXECUTIVE_VIEW_SCREEN_ID };

declare module '@hai3/react' {
  interface EventPayloadMap {
    [InsightNavigationEvents.SelectionRequested]: SelectionRequest;
  }
}

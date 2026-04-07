/**
 * Current User Events
 */

import '@hai3/react';
import { INSIGHT_SCREENSET_ID } from '../ids';
import type { CurrentUser } from '../types';

const DOMAIN_ID = 'currentUser';

export enum CurrentUserEvents {
  UserChanged = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/userChanged`,
}

declare module '@hai3/react' {
  interface EventPayloadMap {
    [CurrentUserEvents.UserChanged]: CurrentUser;
    'layout/menu/itemParam': { screenId: string; param: string };
  }
}

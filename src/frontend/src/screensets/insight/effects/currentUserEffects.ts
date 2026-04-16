/**
 * Current User Effects
 * On user change: update slice + rebuild menu for the new role.
 *
 * Menu is built from real Identity Resolution data (subordinates) when
 * available, falling back to mock registry for dev mode.
 */

import { type AppDispatch, eventBus, setMenuItems } from '@hai3/react';
import { CurrentUserEvents } from '../events/currentUserEvents';
import { setCurrentUser } from '../slices/currentUserSlice';
import { setSelectedPersonId } from '../slices/icDashboardSlice';
import { setSelectedTeamId } from '../slices/teamViewSlice';
import type { CurrentUser } from '../types';
import type { IdentityPerson } from '@/app/types/identity';
import {
  INSIGHT_SCREENSET_ID,
  EXECUTIVE_VIEW_SCREEN_ID,
  TEAM_VIEW_SCREEN_ID,
  IC_DASHBOARD_SCREEN_ID,
  MY_DASHBOARD_SCREEN_ID,
} from '../ids';

const menuKey = (key: string) => `screenset.${INSIGHT_SCREENSET_ID}:menu_items.${key}.label`;

// ---------------------------------------------------------------------------
// Menu builder — real identity data
// ---------------------------------------------------------------------------

function buildMenuFromIdentity(user: CurrentUser) {
  const { role, _identity: identity } = user;
  const myDashItem = { id: MY_DASHBOARD_SCREEN_ID, label: menuKey('my-dashboard'), icon: 'lucide:user-circle' };

  if (!identity) {
    return [myDashItem];
  }

  /** Recursively build menu items from subordinate tree */
  const toMenuItems = (subs: IdentityPerson[]): object[] =>
    subs.map((sub) => {
      const children = toMenuItems(sub.subordinates);
      const item: Record<string, unknown> = {
        id: children.length > 0
          ? `${TEAM_VIEW_SCREEN_ID}::${sub.email}`
          : `${IC_DASHBOARD_SCREEN_ID}::${sub.email}`,
        label: sub.display_name,
        icon: children.length > 0 ? 'lucide:users' : 'lucide:user',
      };
      if (children.length > 0) {
        item.children = children;
      }
      return item;
    });

  const subordinateItems = toMenuItems(identity.subordinates);

  switch (role) {
    case 'executive':
      return [
        { id: EXECUTIVE_VIEW_SCREEN_ID, label: menuKey('executive-view'), icon: 'lucide:building-2' },
        ...(subordinateItems.length > 0
          ? [{
              id: TEAM_VIEW_SCREEN_ID,
              label: identity.department || menuKey('team-view'),
              icon: 'lucide:users',
              children: subordinateItems,
            }]
          : []),
      ];

    case 'team_lead':
      return [
        myDashItem,
        ...(subordinateItems.length > 0
          ? [{
              id: TEAM_VIEW_SCREEN_ID,
              label: identity.department || menuKey('team-view'),
              icon: 'lucide:users',
              children: subordinateItems,
            }]
          : [{ id: TEAM_VIEW_SCREEN_ID, label: menuKey('team-view'), icon: 'lucide:users' }]),
      ];

    case 'ic':
      return [myDashItem];
  }
}

// ---------------------------------------------------------------------------
// Effect initializer
// ---------------------------------------------------------------------------
export const initializeCurrentUserEffects = (dispatch: AppDispatch): void => {
  eventBus.on(CurrentUserEvents.UserChanged, (payload) => {
    const user: CurrentUser = payload;

    dispatch(setCurrentUser(user));
    dispatch(setMenuItems(buildMenuFromIdentity(user) as Parameters<typeof setMenuItems>[0]));
    dispatch(setSelectedPersonId(user.personId));
    if (user.teamId) dispatch(setSelectedTeamId(user.teamId));
  });

  // Menu items with "screenId::param" — set context before navigation
  eventBus.on('layout/menu/itemParam', ({ screenId, param }) => {
    if (screenId === IC_DASHBOARD_SCREEN_ID) {
      dispatch(setSelectedPersonId(param));
    }
    if (screenId === TEAM_VIEW_SCREEN_ID) {
      dispatch(setSelectedTeamId(param));
    }
  });
};

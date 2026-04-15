/**
 * Current User Actions
 * Switch the active user context (role demo)
 */

import { eventBus } from '@hai3/react';
import { CurrentUserEvents } from '../events/currentUserEvents';
import { PEOPLE, TEAMS } from '../api/mocks/registry';
import type { CurrentUser } from '../types';

// Demo users derived from registry — executive + one lead + one IC per team
export const MOCK_USERS: CurrentUser[] = [
  { personId: 'p0', name: 'David Park', role: 'executive', teamId: TEAMS[0]?.id ?? '' },
  ...PEOPLE.map((p) => ({
    personId: p.person_id,
    name: p.name,
    role: (p.is_lead ? 'team_lead' : 'ic') as CurrentUser['role'],
    teamId: p.team_id,
  })),
];

export const switchUser = (user: CurrentUser): void => {
  eventBus.emit(CurrentUserEvents.UserChanged, user);
};

/**
 * Current User Actions
 * Switch the active user context (role demo)
 */

import { eventBus } from '@hai3/react';
import { CurrentUserEvents } from '../events/currentUserEvents';
import type { CurrentUser } from '../types';

export const MOCK_USERS: CurrentUser[] = [
  { personId: 'p1',  name: 'Alice Kim',   role: 'executive', teamId: '' },
  { personId: 'p6',  name: 'Emma Davis',  role: 'team_lead', teamId: 'infra' },
  { personId: 'p11', name: 'Leo Zhang',   role: 'team_lead', teamId: 'devx' },
  { personId: 'p4',  name: 'Sara Jansen', role: 'ic',        teamId: 'infra' },
];

export const switchUser = (user: CurrentUser): void => {
  eventBus.emit(CurrentUserEvents.UserChanged, user);
};

/**
 * Current User Actions
 * Switch the active user context (role demo)
 */

import { eventBus } from '@hai3/react';
import { CurrentUserEvents } from '../events/currentUserEvents';
import type { CurrentUser } from '../types';

export const MOCK_USERS: CurrentUser[] = [
  { personId: 'p0',  name: 'David Park',  role: 'executive', teamId: '' },
  { personId: 'p5',  name: 'Mike Chen',   role: 'team_lead', teamId: 'backend' },
  { personId: 'p6',  name: 'Emma Davis',  role: 'team_lead', teamId: 'platform' },
  { personId: 'p11', name: 'Leo Zhang',   role: 'team_lead', teamId: 'frontend' },
  { personId: 'p1',  name: 'Alice Kim',   role: 'ic',        teamId: 'backend' },
];

export const switchUser = (user: CurrentUser): void => {
  eventBus.emit(CurrentUserEvents.UserChanged, user);
};

/**
 * Current User Actions — DEV-ONLY mock-user helpers.
 *
 * `MOCK_USERS` and `switchUser` exist so local/dev builds can switch between
 * demo roles without going through OIDC. Real user context comes from
 * `bootstrapActions.fetchCurrentUser` (JWT sub → Identity Resolution). These
 * exports are unused by production code paths; Vite ESM tree-shaking keeps
 * them out of the prod bundle. The `import.meta.env.DEV` guard makes the
 * population explicitly empty in prod as a belt-and-braces safeguard.
 */

import { eventBus } from '@hai3/react';
import { CurrentUserEvents } from '../events/currentUserEvents';
import { PEOPLE, TEAMS } from '../api/mocks/registry';
import type { CurrentUser } from '../types';

// Demo users — executive + one lead + one IC per team.
export const MOCK_USERS: CurrentUser[] = import.meta.env.DEV
  ? [
      { personId: 'p0', name: 'David Park', role: 'executive', teamId: TEAMS[0]?.id ?? '' },
      ...PEOPLE.map((p) => ({
        personId: p.person_id,
        name: p.name,
        role: (p.is_lead ? 'team_lead' : 'ic') as CurrentUser['role'],
        teamId: p.team_id,
      })),
    ]
  : [];

export const switchUser = (user: CurrentUser): void => {
  eventBus.emit(CurrentUserEvents.UserChanged, user);
};

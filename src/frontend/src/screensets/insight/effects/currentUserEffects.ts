/**
 * Current User Effects
 * On user change: update slice + rebuild menu for the new role
 */

import { type AppDispatch, eventBus, setMenuItems } from '@hai3/react';
import { CurrentUserEvents } from '../events/currentUserEvents';
import { setCurrentUser } from '../slices/currentUserSlice';
import { setSelectedPersonId } from '../slices/icDashboardSlice';
import { setSelectedTeamId } from '../slices/teamViewSlice';
import { TEAM_MEMBERS_MONTH } from '../api/mocks';
import type { CurrentUser, UserRole, TeamMember } from '../types';
import {
  INSIGHT_SCREENSET_ID,
  EXECUTIVE_VIEW_SCREEN_ID,
  TEAM_VIEW_SCREEN_ID,
  IC_DASHBOARD_SCREEN_ID,
  MY_DASHBOARD_SCREEN_ID,
} from '../ids';

const menuKey = (key: string) => `screenset.${INSIGHT_SCREENSET_ID}:menu_items.${key}.label`;

// ---------------------------------------------------------------------------
// Org structure — single source of truth for hierarchy
// ---------------------------------------------------------------------------
type OrgTeam = {
  teamId: string;
  label: string;
  leadId: string;
  memberIds: string[];
  subTeams?: OrgTeam[];
};

export const ORG: OrgTeam[] = [
  {
    teamId: 'backend',
    label: 'Backend',
    leadId: 'p5',
    memberIds: ['p1', 'p3', 'p4'],
  },
  {
    teamId: 'platform',
    label: 'Platform',
    leadId: 'p6',
    memberIds: ['p2', 'p8'],
  },
  {
    teamId: 'frontend',
    label: 'Frontend',
    leadId: 'p11',
    memberIds: ['p7', 'p9', 'p10', 'p12'],
  },
];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
const byId = (id: string): TeamMember | undefined =>
  TEAM_MEMBERS_MONTH.find((m: TeamMember) => m.person_id === id);

const memberItem = (m: TeamMember | undefined, isLead = false) =>
  m
    ? {
        id: `${IC_DASHBOARD_SCREEN_ID}::${m.person_id}`,
        label: isLead ? `${m.name} · Lead` : m.name,
        icon: isLead ? 'lucide:user-check' : 'lucide:user',
      }
    : null;

/** Recursively build MenuItem children for a team, excluding the viewer */
function teamToMenuItem(team: OrgTeam, excludeId?: string): object {
  const lead = byId(team.leadId);
  const members = team.memberIds
    .filter((id) => id !== excludeId)
    .map((id) => memberItem(byId(id)))
    .filter(Boolean);
  const subTeamItems = (team.subTeams ?? []).map((t) => teamToMenuItem(t, excludeId));

  return {
    id: `${TEAM_VIEW_SCREEN_ID}::${team.teamId}`,
    label: team.label,
    icon: 'lucide:users',
    children: [
      ...(lead && lead.person_id !== excludeId ? [memberItem(lead, true)!] : []),
      ...members,
      ...subTeamItems,
    ],
  };
}

// ---------------------------------------------------------------------------
// Menu builder
// ---------------------------------------------------------------------------
function buildMenu(role: UserRole, user: CurrentUser) {
  const execItem   = { id: EXECUTIVE_VIEW_SCREEN_ID, label: menuKey('executive-view'), icon: 'lucide:building-2' };
  const teamItem   = { id: TEAM_VIEW_SCREEN_ID,      label: menuKey('team-view'),      icon: 'lucide:users' };
  const myDashItem = { id: MY_DASHBOARD_SCREEN_ID,   label: menuKey('my-dashboard'),   icon: 'lucide:user-circle' };

  switch (role) {
    case 'executive':
      return [
        execItem,
        ...ORG.map((t) => teamToMenuItem(t, user.personId)),
      ];

    case 'team_lead': {
      const myTeam = ORG.find((t) => t.leadId === user.personId);
      const myMembers = (myTeam?.memberIds ?? [])
        .filter((id) => id !== user.personId)
        .map((id) => memberItem(byId(id)))
        .filter(Boolean) as object[];

      return [
        myDashItem,
        teamItem,
        ...(myTeam
          ? [{ id: `team-members-${myTeam.teamId}`, label: menuKey('team-members'), icon: 'lucide:users-2', children: myMembers }]
          : []),
      ];
    }

    case 'ic':
      return [myDashItem];
  }
}

// ---------------------------------------------------------------------------
// Effect initializer
// ---------------------------------------------------------------------------
export const initializeCurrentUserEffects = (dispatch: AppDispatch): void => {
  eventBus.on(CurrentUserEvents.UserChanged, (user) => {
    dispatch(setCurrentUser(user));
    dispatch(setMenuItems(buildMenu(user.role, user) as Parameters<typeof setMenuItems>[0]));
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

  // Build initial menu for default user on startup
  const defaultUser: CurrentUser = { personId: 'p0', name: 'David Park', role: 'executive', teamId: '' };
  dispatch(setMenuItems(buildMenu(defaultUser.role, defaultUser) as Parameters<typeof setMenuItems>[0]));
};

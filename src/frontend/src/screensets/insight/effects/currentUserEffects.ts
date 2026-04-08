/**
 * Current User Effects
 * On user change: update slice + rebuild menu for the new role
 */

import { type AppDispatch, eventBus, setMenuItems } from '@hai3/react';
import { CurrentUserEvents } from '../events/currentUserEvents';
import { setCurrentUser } from '../slices/currentUserSlice';
import { setSelectedPersonId } from '../slices/icDashboardSlice';
import { TEAM_MEMBERS_MONTH } from '../api/mocks';
import type { CurrentUser, UserRole, TeamMember } from '../types';
import {
  EXECUTIVE_VIEW_SCREEN_ID,
  TEAM_VIEW_SCREEN_ID,
  IC_DASHBOARD_SCREEN_ID,
  MY_DASHBOARD_SCREEN_ID,
} from '../ids';

// ---------------------------------------------------------------------------
// Org structure — single source of truth for hierarchy
// ---------------------------------------------------------------------------
interface OrgTeam {
  teamId: string;
  label: string;
  leadId: string;
  memberIds: string[];
  subTeams?: OrgTeam[];
}

export const ORG: OrgTeam[] = [
  {
    teamId: 'infra',
    label: 'Infrastructure Squad',
    leadId: 'p6',
    memberIds: ['p1', 'p2'],
    subTeams: [
      {
        teamId: 'infra-backend',
        label: 'Backend',
        leadId: 'p5',
        memberIds: ['p3', 'p4'],
      },
    ],
  },
  {
    teamId: 'devx',
    label: 'Developer Experience',
    leadId: 'p11',
    memberIds: ['p7', 'p8', 'p9', 'p10', 'p12'],
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

/** Recursively build MenuItem children for a team */
function teamToMenuItem(team: OrgTeam): object {
  const lead = byId(team.leadId);
  const members = team.memberIds.map((id) => memberItem(byId(id))).filter(Boolean);
  const subTeamItems = (team.subTeams ?? []).map(teamToMenuItem);

  return {
    id: `team-group-${team.teamId}`,
    label: team.label,
    icon: 'lucide:users',
    children: [
      ...(lead ? [memberItem(lead, true)!] : []),
      ...members,
      ...subTeamItems,
    ],
  };
}

// ---------------------------------------------------------------------------
// Menu builder
// ---------------------------------------------------------------------------
function buildMenu(role: UserRole, user: CurrentUser) {
  const execItem   = { id: EXECUTIVE_VIEW_SCREEN_ID, label: 'Executive View',  icon: 'lucide:bar-chart-2' };
  const teamItem   = { id: TEAM_VIEW_SCREEN_ID,      label: 'Team View',       icon: 'lucide:layout-dashboard' };
  const myDashItem = { id: MY_DASHBOARD_SCREEN_ID,   label: 'My Dashboard',    icon: 'lucide:user-circle' };

  switch (role) {
    case 'executive':
      // Show all teams with their members
      return [
        execItem,
        ...ORG.map(teamToMenuItem),
      ];

    case 'team_lead': {
      // Find which team this lead owns
      const myTeam = ORG.find((t) => t.leadId === user.personId);
      const myMembers = (myTeam?.memberIds ?? [])
        .filter((id) => id !== user.personId)
        .map((id) => memberItem(byId(id)))
        .filter(Boolean) as object[];

      return [
        myDashItem,
        teamItem,
        ...(myTeam
          ? [{ id: `team-members-${myTeam.teamId}`, label: 'Team Members', icon: 'lucide:users-2', children: myMembers }]
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
  });

  // Menu items with "screenId::param" — set context before navigation
  eventBus.on('layout/menu/itemParam', ({ screenId, param }) => {
    if (screenId === IC_DASHBOARD_SCREEN_ID) {
      dispatch(setSelectedPersonId(param));
    }
  });
};

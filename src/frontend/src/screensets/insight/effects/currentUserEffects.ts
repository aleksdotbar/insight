/**
 * Current User Effects
 * On user change: update slice + rebuild menu for the new role
 */

import { type AppDispatch, eventBus, setMenuItems } from '@hai3/react';
import { CurrentUserEvents } from '../events/currentUserEvents';
import { setCurrentUser } from '../slices/currentUserSlice';
import { setSelectedPersonId } from '../slices/icDashboardSlice';
import { setSelectedTeamId } from '../slices/teamViewSlice';
import { TEAMS, PEOPLE_BY_ID, teamMembers } from '../api/mocks/registry';
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
// Org structure — derived from registry (single source of truth)
// ---------------------------------------------------------------------------
type OrgTeam = {
  teamId: string;
  label: string;
  leadId: string;
  memberIds: string[];
  subTeams?: OrgTeam[];
};

// Lead determined by is_lead flag in registry; rest are members
export const ORG: OrgTeam[] = TEAMS.map((t) => {
  const members = teamMembers(t.id);
  const lead = members.find((m) => m.is_lead);
  const rest = members.filter((m) => !m.is_lead);
  return {
    teamId: t.id,
    label: t.name,
    leadId: lead?.person_id ?? '',
    memberIds: rest.map((m) => m.person_id),
  };
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
const byId = (id: string): TeamMember | undefined => {
  const p = PEOPLE_BY_ID[id];
  if (!p) return undefined;
  return { person_id: p.person_id, name: p.name, seniority: p.seniority, period: 'month',
    tasks_closed: 0, bugs_fixed: 0, dev_time_h: 0, prs_merged: 0,
    build_success_pct: null, focus_time_pct: 0, ai_tools: p.ai_tools, ai_loc_share_pct: 0 };
};

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
  const defaultUser: CurrentUser = { personId: 'p0', name: 'David Park', role: 'executive', teamId: TEAMS[0]?.id ?? '' };
  dispatch(setMenuItems(buildMenu(defaultUser.role, defaultUser) as Parameters<typeof setMenuItems>[0]));
};

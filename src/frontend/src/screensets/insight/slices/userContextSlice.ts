/**
 * User Context Slice — single source of truth for "who is signed in" and
 * "what is currently selected in the UI".
 *
 * Replaces three previously independent pieces of state:
 *   - currentUserSlice.currentUser.personId / teamId
 *   - icDashboardSlice.selectedPersonId
 *   - teamViewSlice.selectedTeamId
 *
 * Screens compute the effective selection via `selectActivePerson` /
 * `selectActiveTeam` so they no longer need to hand-roll fallbacks like
 * `selectedTeamId || currentUser.teamId || 'backend'`.
 */

import { createSlice, type PayloadAction } from '@reduxjs/toolkit';
import type { RootState } from '@hai3/react';
import { INSIGHT_SCREENSET_ID } from '../ids';
import type { UserRole } from '../types';
import type { IdentityPerson } from '@/app/types/identity';

const SLICE_KEY = `${INSIGHT_SCREENSET_ID}/userContext` as const;

/**
 * Team reference — discriminated so the future backend switch to canonical
 * org_unit UUIDs or subtree-aggregation does not require rewriting every
 * consumer. Today the analytics backend accepts only `org_unit_name`; the
 * `person_subtree` variant is a forward-compatible marker.
 */
export type TeamRef =
  | { kind: 'org_unit_name'; value: string }
  | { kind: 'person_subtree'; email: string };

export type Viewer = {
  /** Viewer's email (also used as person_id in CH analytics today). */
  id: string;
  /** Display name from identity (BambooHR). */
  name: string;
  role: UserRole;
  /** Full identity payload (subordinates tree, department, job_title). */
  identity: IdentityPerson | null;
};

export type Selection = {
  /** When null, the viewer is looking at themselves (My Dashboard). */
  person: string | null;
  /** Null = no team selected; otherwise a discriminated TeamRef. */
  team: TeamRef | null;
};

export interface UserContextState {
  viewer: Viewer;
  selection: Selection;
}

const initialState: UserContextState = {
  viewer: {
    id: '',
    name: '',
    role: 'ic',
    identity: null,
  },
  selection: {
    person: null,
    team: null,
  },
};

export const userContextSlice = createSlice({
  name: SLICE_KEY,
  initialState,
  reducers: {
    setViewer: (state, action: PayloadAction<Viewer>) => {
      state.viewer = action.payload;
    },
    setSelectedPerson: (state, action: PayloadAction<string | null>) => {
      state.selection.person = action.payload;
    },
    setSelectedTeam: (state, action: PayloadAction<TeamRef | null>) => {
      state.selection.team = action.payload;
    },
    clearSelection: (state) => {
      state.selection.person = null;
      state.selection.team = null;
    },
  },
});

export const {
  setViewer,
  setSelectedPerson,
  setSelectedTeam,
  clearSelection,
} = userContextSlice.actions;
export default userContextSlice;

declare module '@hai3/react' {
  interface RootState {
    [SLICE_KEY]: UserContextState;
  }
}

// ---------------------------------------------------------------------------
// Selectors
// ---------------------------------------------------------------------------

export const selectViewer = (state: RootState): Viewer =>
  state[SLICE_KEY]?.viewer ?? initialState.viewer;

export const selectViewerRole = (state: RootState): UserRole =>
  state[SLICE_KEY]?.viewer.role ?? 'ic';

export const selectSelection = (state: RootState): Selection =>
  state[SLICE_KEY]?.selection ?? initialState.selection;

/**
 * Effective person for data-fetching — selected IC if any, otherwise the
 * viewer themself (My Dashboard semantics). Returns `null` before the viewer
 * is hydrated so callers can short-circuit empty-id requests instead of
 * firing `person_id eq ''` filters.
 */
export const selectActivePerson = (state: RootState): string | null => {
  const s = state[SLICE_KEY];
  if (!s) return null;
  const active = s.selection.person ?? s.viewer.id;
  return active && active.length > 0 ? active : null;
};

/**
 * Effective team for data-fetching — the explicit selection, or the viewer's
 * own department (team-lead case), or null (no team context available).
 */
export const selectActiveTeam = (state: RootState): TeamRef | null => {
  const s = state[SLICE_KEY];
  if (!s) return null;
  if (s.selection.team) return s.selection.team;
  const dept = s.viewer.identity?.department;
  if (dept && dept.length > 0) {
    return { kind: 'org_unit_name', value: dept };
  }
  return null;
};

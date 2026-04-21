/**
 * Team View Slice
 * Redux state management for team view screen
 * Following Flux: Effects dispatch these reducers after listening to events
 */

import { createSlice, type PayloadAction } from '@reduxjs/toolkit';
import type { RootState } from '@hai3/react';
import { INSIGHT_SCREENSET_ID } from '../ids';
import type { TeamMember, TeamKpi, BulletSection, TeamViewData, TeamViewConfig, DataAvailability, DrillData } from '../types';
import { selectActiveTeam } from './userContextSlice';

const SLICE_KEY = `${INSIGHT_SCREENSET_ID}/teamView` as const;

/**
 * State interface
 *
 * `selectedTeamId` previously lived here; it has been promoted to
 * `userContextSlice.selection.team` (as a discriminated TeamRef) as part of
 * the single-source-of-truth refactor (Phase 4). Use `selectActiveTeam` /
 * `selectActiveTeamId` from userContextSlice instead.
 */
export interface TeamViewState {
  teamName: string;
  members: TeamMember[];
  teamKpis: TeamKpi[];
  bulletSections: BulletSection[];
  config: TeamViewConfig | null;
  availability: DataAvailability | null;
  loading: boolean;
  error: string | null;
  drillId: string | null;
  drillData: DrillData | null;
}

const initialState: TeamViewState = {
  teamName: '',
  members: [],
  teamKpis: [],
  bulletSections: [],
  config: null,
  availability: null,
  loading: false,
  error: null,
  drillId: null,
  drillData: null,
};

export const teamViewSlice = createSlice({
  name: SLICE_KEY,
  initialState,
  reducers: {
    setLoading: (state, action: PayloadAction<boolean>) => {
      state.loading = action.payload;
    },
    setTeamViewData: (state, action: PayloadAction<TeamViewData>) => {
      state.teamName = action.payload.teamName;
      state.members = action.payload.members;
      state.teamKpis = action.payload.teamKpis;
      state.bulletSections = action.payload.bulletSections;
      state.config = action.payload.config;
      state.loading = false;
    },
    setAvailability: (state, action: PayloadAction<DataAvailability>) => {
      state.availability = action.payload;
    },
    setError: (state, action: PayloadAction<string>) => {
      state.error = action.payload;
    },
    setDrillState: (
      state,
      action: PayloadAction<{ drillId: string; drillData: DrillData }>,
    ) => {
      state.drillId = action.payload.drillId;
      state.drillData = action.payload.drillData;
    },
    clearDrill: (state) => {
      state.drillId = null;
      state.drillData = null;
    },
  },
});

// Export actions
export const {
  setLoading,
  setTeamViewData,
  setAvailability,
  setError,
  setDrillState,
  clearDrill,
} = teamViewSlice.actions;

// Export the slice object (not just the reducer) for registerSlice()
export default teamViewSlice;

// Module augmentation - extends uicore RootState
declare module '@hai3/react' {
  interface RootState {
    [SLICE_KEY]: TeamViewState;
  }
}

/**
 * Type-safe selectors
 */
export const selectMembers = (state: RootState): TeamMember[] => {
  return state[SLICE_KEY]?.members ?? [];
};

export const selectTeamKpis = (state: RootState): TeamKpi[] => {
  return state[SLICE_KEY]?.teamKpis ?? [];
};

export const selectBulletSections = (state: RootState): BulletSection[] => {
  return state[SLICE_KEY]?.bulletSections ?? [];
};

export const selectTeamViewLoading = (state: RootState): boolean => {
  return state[SLICE_KEY]?.loading ?? false;
};

export const selectTeamName = (state: RootState): string => {
  return state[SLICE_KEY]?.teamName ?? '';
};

export const selectTeamViewConfig = (state: RootState): TeamViewConfig | null => {
  return state[SLICE_KEY]?.config ?? null;
};

/**
 * Effective team identifier for analytics queries — collapses the active
 * TeamRef to the single string the backend currently understands
 * (`org_unit_name` or a subordinate email). Returns '' when no team context
 * is established; callers should render an empty state rather than fire a
 * filter against the empty string.
 */
export const selectSelectedTeamId = (state: RootState): string => {
  const ref = selectActiveTeam(state);
  if (!ref) return '';
  return ref.kind === 'org_unit_name' ? ref.value : ref.email;
};

export const selectTeamAvailability = (state: RootState): DataAvailability | null => {
  return state[SLICE_KEY]?.availability ?? null;
};

export const selectTeamDrillId = (state: RootState): string | null => {
  return state[SLICE_KEY]?.drillId ?? null;
};

export const selectTeamDrillData = (state: RootState): DrillData | null => {
  return state[SLICE_KEY]?.drillData ?? null;
};

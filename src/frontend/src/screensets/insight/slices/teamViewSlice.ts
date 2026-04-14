/**
 * Team View Slice
 * Redux state management for team view screen
 * Following Flux: Effects dispatch these reducers after listening to events
 */

import { createSlice, type PayloadAction } from '@reduxjs/toolkit';
import type { RootState } from '@hai3/react';
import { INSIGHT_SCREENSET_ID } from '../ids';
import type { TeamMember, TeamKpi, BulletSection, TeamViewData, TeamViewConfig, DataAvailability } from '../types';

const SLICE_KEY = `${INSIGHT_SCREENSET_ID}/teamView` as const;

/**
 * State interface
 */
export interface TeamViewState {
  selectedTeamId: string;
  teamName: string;
  members: TeamMember[];
  teamKpis: TeamKpi[];
  bulletSections: BulletSection[];
  config: TeamViewConfig | null;
  availability: DataAvailability | null;
  loading: boolean;
  error: string | null;
}

const initialState: TeamViewState = {
  selectedTeamId: '',
  teamName: '',
  members: [],
  teamKpis: [],
  bulletSections: [],
  config: null,
  availability: null,
  loading: false,
  error: null,
};

export const teamViewSlice = createSlice({
  name: SLICE_KEY,
  initialState,
  reducers: {
    setSelectedTeamId: (state, action: PayloadAction<string>) => {
      state.selectedTeamId = action.payload;
    },
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
  },
});

// Export actions
export const { setSelectedTeamId, setLoading, setTeamViewData, setAvailability, setError } = teamViewSlice.actions;

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

export const selectSelectedTeamId = (state: RootState): string => {
  return state[SLICE_KEY]?.selectedTeamId ?? '';
};

export const selectTeamAvailability = (state: RootState): DataAvailability | null => {
  return state[SLICE_KEY]?.availability ?? null;
};

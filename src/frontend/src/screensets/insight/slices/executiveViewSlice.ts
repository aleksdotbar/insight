/**
 * Executive View Slice
 * Redux state management for executive view screen
 * Following Flux: Effects dispatch these reducers after listening to events
 */

import { createSlice, type PayloadAction } from '@reduxjs/toolkit';
import type { RootState } from '@hai3/react';
import { INSIGHT_SCREENSET_ID } from '../ids';
import type { ExecTeamRow, OrgKpis, ExecViewData, ExecViewConfig, DataAvailability } from '../types';

const SLICE_KEY = `${INSIGHT_SCREENSET_ID}/executiveView` as const;

/**
 * State interface
 */
export interface ExecutiveViewState {
  teams: ExecTeamRow[];
  orgKpis: OrgKpis | null;
  config: ExecViewConfig | null;
  availability: DataAvailability | null;
  loading: boolean;
  error: string | null;
}

const initialState: ExecutiveViewState = {
  teams: [],
  orgKpis: null,
  config: null,
  availability: null,
  loading: false,
  error: null,
};

export const executiveViewSlice = createSlice({
  name: SLICE_KEY,
  initialState,
  reducers: {
    setLoading: (state, action: PayloadAction<boolean>) => {
      state.loading = action.payload;
    },
    setExecutiveViewData: (state, action: PayloadAction<ExecViewData>) => {
      state.teams    = action.payload.teams;
      state.orgKpis  = action.payload.orgKpis;
      state.config   = action.payload.config;
      state.loading  = false;
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
export const { setLoading, setExecutiveViewData, setAvailability, setError } = executiveViewSlice.actions;

// Export the slice object (not just the reducer) for registerSlice()
export default executiveViewSlice;

// Module augmentation - extends uicore RootState
declare module '@hai3/react' {
  interface RootState {
    [SLICE_KEY]: ExecutiveViewState;
  }
}

/**
 * Type-safe selectors
 */
export const selectTeams = (state: RootState): ExecTeamRow[] => {
  return state[SLICE_KEY]?.teams ?? [];
};

export const selectOrgKpis = (state: RootState): OrgKpis | null => {
  return state[SLICE_KEY]?.orgKpis ?? null;
};

export const selectExecLoading = (state: RootState): boolean => {
  return state[SLICE_KEY]?.loading ?? false;
};

export const selectExecError = (state: RootState): string | null => {
  return state[SLICE_KEY]?.error ?? null;
};

export const selectExecViewConfig = (state: RootState): ExecViewConfig | null => {
  return state[SLICE_KEY]?.config ?? null;
};

export const selectExecAvailability = (state: RootState): DataAvailability | null => {
  return state[SLICE_KEY]?.availability ?? null;
};

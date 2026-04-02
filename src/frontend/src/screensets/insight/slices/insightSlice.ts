/**
 * insight Slice
 * Redux state management for this screenset
 * Following Flux: Effects dispatch these reducers after listening to events
 */

import { createSlice, type PayloadAction } from '@reduxjs/toolkit';
import type { RootState } from '@hai3/react';
import { INSIGHT_SCREENSET_ID } from '../ids';
import type { DashboardData, SpeedData } from '../types';

const SLICE_KEY = `${INSIGHT_SCREENSET_ID}/insight` as const;

/**
 * State interface
 */
export interface InsightState {
  dashboard: DashboardData | null;
  speed: SpeedData | null;
}

const initialState: InsightState = {
  dashboard: null,
  speed: null,
};

export const insightSlice = createSlice({
  name: SLICE_KEY,
  initialState,
  reducers: {
    setDashboard: (state, action: PayloadAction<DashboardData>) => {
      state.dashboard = action.payload;
    },
    setSpeed: (state, action: PayloadAction<SpeedData>) => {
      state.speed = action.payload;
    },
  },
});

// Export actions
export const { setDashboard, setSpeed } = insightSlice.actions;

// Export the slice object (not just the reducer) for registerSlice()
export default insightSlice;

// Module augmentation - extends uicore RootState
declare module '@hai3/react' {
  interface RootState {
    [SLICE_KEY]: InsightState;
  }
}

/**
 * Type-safe selectors
 */
export const selectDashboard = (state: RootState): DashboardData | null => {
  return state[SLICE_KEY].dashboard;
};


export const selectSpeed = (state: RootState): SpeedData | null => {
  return state[SLICE_KEY].speed;
};

/**
 * Period Slice
 * Redux state management for period selection
 * Following Flux: Effects dispatch these reducers after listening to events
 */

import { createSlice, type PayloadAction } from '@reduxjs/toolkit';
import type { RootState } from '@hai3/react';
import { INSIGHT_SCREENSET_ID } from '../ids';
import type { PeriodValue, CustomRange } from '../types';

const SLICE_KEY = `${INSIGHT_SCREENSET_ID}/period` as const;

/**
 * State interface
 */
export interface PeriodSliceState {
  period: PeriodValue;
  customRange: CustomRange | null;
  scale: number;
}

const initialState: PeriodSliceState = {
  period: 'month',
  customRange: null,
  scale: 1.0,
};

export const periodSlice = createSlice({
  name: SLICE_KEY,
  initialState,
  reducers: {
    setPeriod: (state, action: PayloadAction<PeriodValue>) => {
      state.period = action.payload;
    },
    setCustomRange: (state, action: PayloadAction<CustomRange | null>) => {
      state.customRange = action.payload;
    },
    setScale: (state, action: PayloadAction<number>) => {
      state.scale = action.payload;
    },
  },
});

// Export actions
export const { setPeriod, setCustomRange, setScale } = periodSlice.actions;

// Export the slice object (not just the reducer) for registerSlice()
export default periodSlice;

// Module augmentation - extends uicore RootState
declare module '@hai3/react' {
  interface RootState {
    [SLICE_KEY]: PeriodSliceState;
  }
}

/**
 * Type-safe selectors
 */
export const selectPeriod = (state: RootState): PeriodValue => {
  return state[SLICE_KEY]?.period ?? 'month';
};

export const selectCustomRange = (state: RootState): CustomRange | null => {
  return state[SLICE_KEY]?.customRange ?? null;
};

export const selectScale = (state: RootState): number => {
  return state[SLICE_KEY]?.scale ?? 1.0;
};

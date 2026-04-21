/**
 * Insight UI Slice
 *
 * Cross-screen UI preferences that persist across navigation within the
 * insight screenset. Currently holds `viewMode` (chart vs tile) so switching
 * on one screen carries to another. Previously each screen had its own local
 * useState, which reset on navigation.
 */

import { createSlice, type PayloadAction } from '@reduxjs/toolkit';
import type { RootState } from '@hai3/react';
import { INSIGHT_SCREENSET_ID } from '../ids';
import type { ViewMode } from '../types';

const SLICE_KEY = `${INSIGHT_SCREENSET_ID}/insightUi` as const;

export interface InsightUiState {
  viewMode: ViewMode;
}

const initialState: InsightUiState = {
  viewMode: 'chart',
};

export const insightUiSlice = createSlice({
  name: SLICE_KEY,
  initialState,
  reducers: {
    setInsightViewMode: (state, action: PayloadAction<ViewMode>) => {
      state.viewMode = action.payload;
    },
  },
});

export const { setInsightViewMode } = insightUiSlice.actions;
export default insightUiSlice;

declare module '@hai3/react' {
  interface RootState {
    [SLICE_KEY]: InsightUiState;
  }
}

export const selectInsightViewMode = (state: RootState): ViewMode => {
  return state[SLICE_KEY]?.viewMode ?? 'chart';
};

/**
 * auth Slice
 * Redux state management for this screenset
 * Following Flux: Effects dispatch these reducers after listening to events
 */

import { createSlice } from '@reduxjs/toolkit';
import type { RootState } from '@hai3/react';
import { AUTH_SCREENSET_ID } from '../ids';

const SLICE_KEY = `${AUTH_SCREENSET_ID}/auth` as const;

/**
 * State interface
 * Add your state properties here
 */
export interface AuthState {
  /** Placeholder property - remove when adding real state */
  _placeholder?: never;
}

const initialState: AuthState = {
  // Initialize your state here
};

export const authSlice = createSlice({
  name: SLICE_KEY,
  initialState,
  reducers: {
    // Add your reducers here
    // Example:
    // setData: (state, action: PayloadAction<Data>) => {
    //   state.data = action.payload;
    // },
  },
});

// Export actions
// export const { } = authSlice.actions;

// Export the slice object (not just the reducer) for registerSlice()
export default authSlice;

// Module augmentation - extends uicore RootState
declare module '@hai3/react' {
  interface RootState {
    [SLICE_KEY]: AuthState;
  }
}

/**
 * Type-safe selector for this slice's state
 */
export const selectAuthState = (state: RootState): AuthState => {
  return state[SLICE_KEY];
};

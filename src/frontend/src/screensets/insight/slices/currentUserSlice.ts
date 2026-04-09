/**
 * Current User Slice
 * Stores the active user context (role + personId)
 */

import { createSlice, type PayloadAction } from '@reduxjs/toolkit';
import type { RootState } from '@hai3/react';
import { INSIGHT_SCREENSET_ID } from '../ids';
import type { CurrentUser, UserRole } from '../types';

const SLICE_KEY = `${INSIGHT_SCREENSET_ID}/currentUser` as const;

type CurrentUserState = {
  currentUser: CurrentUser;
};

const initialState: CurrentUserState = {
  currentUser: {
    personId: 'p0',
    name: 'David Park',
    role: 'executive',
    teamId: '',
  },
};

export const currentUserSlice = createSlice({
  name: SLICE_KEY,
  initialState,
  reducers: {
    setCurrentUser: (state, action: PayloadAction<CurrentUser>) => {
      state.currentUser = action.payload;
    },
  },
});

export const { setCurrentUser } = currentUserSlice.actions;
export default currentUserSlice;

declare module '@hai3/react' {
  interface RootState {
    [SLICE_KEY]: CurrentUserState;
  }
}

export const selectCurrentUser = (state: RootState): CurrentUser =>
  state[SLICE_KEY]?.currentUser ?? initialState.currentUser;

export const selectUserRole = (state: RootState): UserRole =>
  state[SLICE_KEY]?.currentUser.role ?? 'executive';

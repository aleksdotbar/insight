/**
 * Auth Slice
 * Redux state for OIDC auth lifecycle
 */

import { createSlice, type PayloadAction } from '@reduxjs/toolkit';
import type { RootState } from '@hai3/react';
import type { OidcConfig } from '@/app/types/auth';

const SLICE_KEY = 'auth' as const;

export type AuthState = {
  token: string | null;
  config: OidcConfig | null;
  status: 'idle' | 'loading' | 'authenticated' | 'expired';
}

const initialState: AuthState = {
  token: null,
  config: null,
  status: 'idle',
};

export const authSlice = createSlice({
  name: SLICE_KEY,
  initialState,
  reducers: {
    setToken: (state, action: PayloadAction<string | null>) => {
      state.token = action.payload;
    },
    setConfig: (state, action: PayloadAction<OidcConfig>) => {
      state.config = action.payload;
    },
    setStatus: (state, action: PayloadAction<AuthState['status']>) => {
      state.status = action.payload;
    },
    clearAuth: (state) => {
      state.token = null;
      state.config = null;
      state.status = 'idle';
    },
  },
});

export const { setToken, setConfig, setStatus, clearAuth } = authSlice.actions;
export default authSlice;

declare module '@hai3/react' {
  interface RootState {
    [SLICE_KEY]: AuthState;
  }
}

export const selectAuthToken = (state: RootState): string | null =>
  state[SLICE_KEY]?.token ?? null;

export const selectOidcConfig = (state: RootState): OidcConfig | null =>
  state[SLICE_KEY]?.config ?? null;

export const selectAuthStatus = (state: RootState): AuthState['status'] =>
  state[SLICE_KEY]?.status ?? 'idle';

/**
 * IC Dashboard Slice
 * Redux state management for IC dashboard screen
 * Following Flux: Effects dispatch these reducers after listening to events
 */

import { createSlice, type PayloadAction } from '@reduxjs/toolkit';
import type { RootState } from '@hai3/react';
import { INSIGHT_SCREENSET_ID } from '../ids';
import type {
  IcKpi,
  BulletMetric,
  IcChartsData,
  TimeOffNotice,
  DrillData,
  IcDashboardData,
  DataAvailability,
} from '../types';
import type { IdentityPerson } from '@/app/types/identity';

const SLICE_KEY = `${INSIGHT_SCREENSET_ID}/icDashboard` as const;

/**
 * State interface
 */
export interface IcDashboardState {
  selectedPersonId: string;
  /** Loaded separately via IdentityApiService */
  person: IdentityPerson | null;
  kpis: IcKpi[];
  bulletMetrics: BulletMetric[];
  charts: IcChartsData | null;
  timeOffNotice: TimeOffNotice | null;
  drillId: string | null;
  drillData: DrillData | null;
  /** Loaded separately via ConnectorManagerService */
  availability: DataAvailability | null;
  loading: boolean;
  error: string | null;
  /**
   * Section IDs whose last fetch actually rejected (network / 5xx / etc.),
   * as opposed to returning an empty result. Consumed by composites to
   * switch ComingSoon placeholders from "No data" to "Unable to load · Retry".
   */
  erroredSections: string[];
}

const initialState: IcDashboardState = {
  selectedPersonId: 'p1',
  person: null,
  kpis: [],
  bulletMetrics: [],
  charts: null,
  timeOffNotice: null,
  drillId: null,
  drillData: null,
  availability: null,
  loading: false,
  error: null,
  erroredSections: [],
};

export const icDashboardSlice = createSlice({
  name: SLICE_KEY,
  initialState,
  reducers: {
    setSelectedPersonId: (state, action: PayloadAction<string>) => {
      state.selectedPersonId = action.payload;
    },
    setLoading: (state, action: PayloadAction<boolean>) => {
      state.loading = action.payload;
    },
    setIcDashboardData: (state, action: PayloadAction<IcDashboardData>) => {
      state.kpis          = action.payload.kpis;
      state.bulletMetrics = action.payload.bulletMetrics;
      state.charts        = action.payload.charts;
      state.timeOffNotice = action.payload.timeOffNotice;
      state.loading       = false;
      state.error         = null;
    },
    setPerson: (state, action: PayloadAction<IdentityPerson>) => {
      state.person = action.payload;
    },
    setAvailability: (state, action: PayloadAction<DataAvailability>) => {
      state.availability = action.payload;
    },
    setError: (state, action: PayloadAction<string>) => {
      state.error = action.payload;
      state.loading = false;
    },
    setDrillState: (
      state,
      action: PayloadAction<{ drillId: string; drillData: DrillData }>
    ) => {
      state.drillId = action.payload.drillId;
      state.drillData = action.payload.drillData;
    },
    clearDrill: (state) => {
      state.drillId = null;
      state.drillData = null;
    },
    setErroredSections: (state, action: PayloadAction<string[]>) => {
      state.erroredSections = action.payload;
    },
  },
});

// Export actions
export const {
  setSelectedPersonId,
  setLoading,
  setIcDashboardData,
  setPerson,
  setAvailability,
  setError,
  setDrillState,
  clearDrill,
  setErroredSections,
} = icDashboardSlice.actions;

// Export the slice object (not just the reducer) for registerSlice()
export default icDashboardSlice;

// Module augmentation - extends uicore RootState
declare module '@hai3/react' {
  interface RootState {
    [SLICE_KEY]: IcDashboardState;
  }
}

/**
 * Type-safe selectors
 */
export const selectPerson = (state: RootState): IdentityPerson | null => {
  return state[SLICE_KEY]?.person ?? null;
};

export const selectIcKpis = (state: RootState): IcKpi[] => {
  return state[SLICE_KEY]?.kpis ?? [];
};

export const selectBulletMetrics = (state: RootState): BulletMetric[] => {
  return state[SLICE_KEY]?.bulletMetrics ?? [];
};

export const selectIcCharts = (state: RootState): IcChartsData | null => {
  return state[SLICE_KEY]?.charts ?? null;
};

export const selectTimeOffNotice = (state: RootState): TimeOffNotice | null => {
  return state[SLICE_KEY]?.timeOffNotice ?? null;
};

export const selectDrillId = (state: RootState): string | null => {
  return state[SLICE_KEY]?.drillId ?? null;
};

export const selectDrillData = (state: RootState): DrillData | null => {
  return state[SLICE_KEY]?.drillData ?? null;
};

export const selectIcLoading = (state: RootState): boolean => {
  return state[SLICE_KEY]?.loading ?? false;
};

export const selectIcAvailability = (state: RootState): DataAvailability | null => {
  return state[SLICE_KEY]?.availability ?? null;
};

export const selectSelectedPersonId = (state: RootState): string => {
  return state[SLICE_KEY]?.selectedPersonId ?? 'p1';
};

export const selectIcErroredSections = (state: RootState): string[] => {
  return state[SLICE_KEY]?.erroredSections ?? [];
};

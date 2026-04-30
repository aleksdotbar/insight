/**
 * IC Dashboard Slice
 * Redux state management for IC dashboard screen
 * Following Flux: Effects dispatch these reducers after listening to events
 *
 * As of the per-section progressive-loading refactor, the slice tracks status
 * (`loading | loaded | errored`) and an error message per section. Each
 * section's data slot fills in independently as queries complete; one slow
 * section never blocks the others.
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
  DataAvailability,
} from '../types';
import type { IdentityPerson } from '@/app/types/identity';
import type { IcDashboardSectionData } from '../events/icDashboardEvents';

const SLICE_KEY = `${INSIGHT_SCREENSET_ID}/icDashboard` as const;

export type SectionStatus = 'loading' | 'loaded' | 'errored';

/**
 * State interface
 *
 * `selectedPersonId` previously lived here; it has been promoted to
 * `userContextSlice.selection.person` as part of the single-source-of-truth
 * refactor (Phase 4). Use `selectActivePerson` from userContextSlice instead.
 */
export interface IcDashboardState {
  /** Loaded separately via IdentityApiService */
  person: IdentityPerson | null;
  kpis: IcKpi[];
  bulletMetrics: BulletMetric[];
  charts: IcChartsData;
  timeOffNotice: TimeOffNotice | null;
  drillId: string | null;
  drillData: DrillData | null;
  /** Loaded separately via ConnectorManagerService */
  availability: DataAvailability | null;
  /**
   * Whole-dashboard error — only set when identity itself is unavailable
   * (the screen cannot render at all). Per-section failures live in
   * `sectionErrors` and don't trigger this.
   */
  error: string | null;
  /** Per-section status: 'loading' before query, 'loaded' / 'errored' after. */
  sectionStatus: Record<string, SectionStatus>;
  /** Per-section last error message (only set when status === 'errored'). */
  sectionErrors: Record<string, string>;
}

const initialChartsState: IcChartsData = {
  locTrend: [],
  deliveryTrend: [],
};

const initialState: IcDashboardState = {
  person: null,
  kpis: [],
  bulletMetrics: [],
  charts: initialChartsState,
  timeOffNotice: null,
  drillId: null,
  drillData: null,
  availability: null,
  error: null,
  sectionStatus: {},
  sectionErrors: {},
};

export const icDashboardSlice = createSlice({
  name: SLICE_KEY,
  initialState,
  reducers: {
    /**
     * Reset section status, error, and aggregate data slots when a new load
     * cycle starts. Identity / availability are left intact — they're not
     * "sections" and re-emit on every load anyway.
     */
    resetForLoad: (state) => {
      state.kpis = [];
      state.bulletMetrics = [];
      state.charts = { locTrend: [], deliveryTrend: [] };
      state.timeOffNotice = null;
      state.error = null;
      state.sectionStatus = {};
      state.sectionErrors = {};
    },
    setPerson: (state, action: PayloadAction<IdentityPerson>) => {
      state.person = action.payload;
    },
    setAvailability: (state, action: PayloadAction<DataAvailability>) => {
      state.availability = action.payload;
    },
    setError: (state, action: PayloadAction<string>) => {
      state.error = action.payload;
    },
    setSectionLoading: (state, action: PayloadAction<{ sectionId: string }>) => {
      state.sectionStatus[action.payload.sectionId] = 'loading';
      delete state.sectionErrors[action.payload.sectionId];
    },
    setSectionLoaded: (
      state,
      action: PayloadAction<{ sectionId: string; data: IcDashboardSectionData }>,
    ) => {
      const { sectionId, data } = action.payload;
      state.sectionStatus[sectionId] = 'loaded';
      delete state.sectionErrors[sectionId];

      switch (data.kind) {
        case 'kpis':
          state.kpis = data.kpis;
          break;
        case 'bullet': {
          // Bullets for all sections live in one flat list — replace the slice
          // for this section, leaving other sections' rows untouched.
          const sid = data.sectionId;
          const others = state.bulletMetrics.filter((m) => m.section !== sid);
          state.bulletMetrics = [...others, ...data.metrics];
          break;
        }
        case 'locTrend':
          state.charts = { ...state.charts, locTrend: data.trend };
          break;
        case 'deliveryTrend':
          state.charts = { ...state.charts, deliveryTrend: data.trend };
          break;
        case 'timeOff':
          state.timeOffNotice = data.notice;
          break;
      }
    },
    setSectionFailed: (
      state,
      action: PayloadAction<{ sectionId: string; error: string }>,
    ) => {
      state.sectionStatus[action.payload.sectionId] = 'errored';
      state.sectionErrors[action.payload.sectionId] = action.payload.error;
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
  resetForLoad,
  setPerson,
  setAvailability,
  setError,
  setSectionLoading,
  setSectionLoaded,
  setSectionFailed,
  setDrillState,
  clearDrill,
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

export const selectIcCharts = (state: RootState): IcChartsData => {
  return state[SLICE_KEY]?.charts ?? initialChartsState;
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

/**
 * Aggregate "loading" — true while any section is still loading. Composites
 * usually want per-section status from `selectSectionStatus` instead, but
 * top-level "is the dashboard still working" remains useful for things like
 * the Person-Not-Found guard.
 */
export const selectIcLoading = (state: RootState): boolean => {
  const statuses = state[SLICE_KEY]?.sectionStatus ?? {};
  return Object.values(statuses).some((s) => s === 'loading');
};

export const selectIcAvailability = (state: RootState): DataAvailability | null => {
  return state[SLICE_KEY]?.availability ?? null;
};

export const selectIcError = (state: RootState): string | null => {
  return state[SLICE_KEY]?.error ?? null;
};

/** Re-export of selectActivePerson so callers can import from this slice
 *  without knowing about the userContext refactor. Prefer importing from
 *  userContextSlice directly in new code. Returns `null` when the viewer
 *  is not yet hydrated. */
export { selectActivePerson as selectSelectedPersonId } from './userContextSlice';

/** Status for a single section, or `undefined` if it hasn't started yet. */
export const selectSectionStatus = (sectionId: string) => (state: RootState): SectionStatus | undefined => {
  return state[SLICE_KEY]?.sectionStatus[sectionId];
};

/** Last error for a section, or `undefined` if not in errored state. */
export const selectSectionError = (sectionId: string) => (state: RootState): string | undefined => {
  return state[SLICE_KEY]?.sectionErrors[sectionId];
};

/**
 * Backwards-compat selector for callers that still expect a flat list of
 * errored section IDs (matches the pre-refactor API).
 */
export const selectIcErroredSections = (state: RootState): string[] => {
  const status = state[SLICE_KEY]?.sectionStatus ?? {};
  return Object.keys(status).filter((k) => status[k] === 'errored');
};

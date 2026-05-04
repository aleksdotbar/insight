/**
 * Team View Slice
 *
 * Per-section progressive loading: each section query (team_summary, members,
 * task_delivery, code_quality, collaboration, ai_adoption) emits its own
 * Loading / Loaded / Failed events. The slice tracks status and an error
 * message per section and merges incoming payloads into the existing data
 * slot. One slow section never blocks the others.
 */

import { createSlice, type PayloadAction } from '@reduxjs/toolkit';
import type { RootState } from '@hai3/react';
import { INSIGHT_SCREENSET_ID } from '../ids';
import type {
  TeamMember,
  TeamKpi,
  BulletSection,
  TeamViewConfig,
  DataAvailability,
  DrillData,
} from '../types';
import { selectActiveTeam } from './userContextSlice';
import type { TeamViewSectionData } from '../events/teamViewEvents';

const SLICE_KEY = `${INSIGHT_SCREENSET_ID}/teamView` as const;

/** See `icDashboardSlice.SectionStatus` for the `revalidating` rationale. */
export type SectionStatus = 'loading' | 'revalidating' | 'loaded' | 'errored';

export interface TeamViewState {
  teamName: string;
  members: TeamMember[];
  teamKpis: TeamKpi[];
  bulletSections: BulletSection[];
  config: TeamViewConfig | null;
  availability: DataAvailability | null;
  /**
   * Whole-screen error — only set when something prevents the screen from
   * rendering at all. Per-section failures live in `sectionErrors`.
   */
  error: string | null;
  drillId: string | null;
  drillData: DrillData | null;
  sectionStatus: Record<string, SectionStatus>;
  sectionErrors: Record<string, string>;
}

const initialState: TeamViewState = {
  teamName: '',
  members: [],
  teamKpis: [],
  bulletSections: [],
  config: null,
  availability: null,
  error: null,
  drillId: null,
  drillData: null,
  sectionStatus: {},
  sectionErrors: {},
};

/**
 * Apply a section payload into the corresponding slot of state. Pulled out
 * so the bullet path can rebuild `bulletSections` (slice replaces only the
 * incoming section, leaving others untouched).
 */
function applySectionData(state: TeamViewState, data: TeamViewSectionData): void {
  switch (data.kind) {
    case 'team_summary':
      state.teamName = data.teamName;
      state.teamKpis = data.teamKpis;
      state.config   = data.config;
      break;
    case 'members':
      state.members = data.members;
      break;
    case 'bullet': {
      const sid = data.sectionId;
      const others = state.bulletSections.filter((s) => s.id !== sid);
      state.bulletSections = [
        ...others,
        { id: sid, title: sid, metrics: data.metrics },
      ];
      break;
    }
  }
}

export const teamViewSlice = createSlice({
  name: SLICE_KEY,
  initialState,
  reducers: {
    /** Hard reset — used when the *team* changes. */
    resetForLoad: (state) => {
      state.teamName = '';
      state.members = [];
      state.teamKpis = [];
      state.bulletSections = [];
      state.config = null;
      state.error = null;
      state.sectionStatus = {};
      state.sectionErrors = {};
    },
    /** Soft reset — period change, keep current values on screen as stale. */
    revalidateForLoad: (state) => {
      state.error = null;
      state.sectionErrors = {};
    },
    setAvailability: (state, action: PayloadAction<DataAvailability>) => {
      state.availability = action.payload;
    },
    setError: (state, action: PayloadAction<string>) => {
      state.error = action.payload;
    },
    setSectionLoading: (state, action: PayloadAction<{ sectionId: string }>) => {
      const { sectionId } = action.payload;
      // sectionId is dispatched by our own action layer with a literal string
      // from a fixed enum; no user input reaches this lookup. See the IC
      // slice for the full rationale.
      const wasLoaded = state.sectionStatus[sectionId] === 'loaded'
        || state.sectionStatus[sectionId] === 'revalidating';
      state.sectionStatus[sectionId] = wasLoaded ? 'revalidating' : 'loading';
      delete state.sectionErrors[sectionId];
    },
    setSectionLoaded: (
      state,
      action: PayloadAction<{ sectionId: string; data: TeamViewSectionData }>,
    ) => {
      const { sectionId, data } = action.payload;
      state.sectionStatus[sectionId] = 'loaded';
      delete state.sectionErrors[sectionId];
      applySectionData(state, data);
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
  revalidateForLoad,
  setAvailability,
  setError,
  setSectionLoading,
  setSectionLoaded,
  setSectionFailed,
  setDrillState,
  clearDrill,
} = teamViewSlice.actions;

// Export the slice object (not just the reducer) for registerSlice()
export default teamViewSlice;

// Module augmentation - extends uicore RootState
declare module '@hai3/react' {
  interface RootState {
    [SLICE_KEY]: TeamViewState;
  }
}

// ---------------------------------------------------------------------------
// Selectors
// ---------------------------------------------------------------------------

export const selectMembers = (state: RootState): TeamMember[] => {
  return state[SLICE_KEY]?.members ?? [];
};

export const selectTeamKpis = (state: RootState): TeamKpi[] => {
  return state[SLICE_KEY]?.teamKpis ?? [];
};

export const selectBulletSections = (state: RootState): BulletSection[] => {
  return state[SLICE_KEY]?.bulletSections ?? [];
};

/** Aggregate "loading" — any section still in flight. */
export const selectTeamViewLoading = (state: RootState): boolean => {
  const statuses = state[SLICE_KEY]?.sectionStatus ?? {};
  return Object.values(statuses).some((s) => s === 'loading');
};

export const selectTeamName = (state: RootState): string => {
  return state[SLICE_KEY]?.teamName ?? '';
};

export const selectTeamViewConfig = (state: RootState): TeamViewConfig | null => {
  return state[SLICE_KEY]?.config ?? null;
};

/**
 * Effective team identifier for analytics queries — collapses the active
 * TeamRef to the single string the backend currently understands
 * (`org_unit_name` or a subordinate email). Returns '' when no team context
 * is established; callers should render an empty state rather than fire a
 * filter against the empty string.
 */
export const selectSelectedTeamId = (state: RootState): string => {
  const ref = selectActiveTeam(state);
  if (!ref) return '';
  return ref.kind === 'org_unit_name' ? ref.value : ref.email;
};

export const selectTeamAvailability = (state: RootState): DataAvailability | null => {
  return state[SLICE_KEY]?.availability ?? null;
};

export const selectTeamDrillId = (state: RootState): string | null => {
  return state[SLICE_KEY]?.drillId ?? null;
};

export const selectTeamDrillData = (state: RootState): DrillData | null => {
  return state[SLICE_KEY]?.drillData ?? null;
};

export const selectTeamViewError = (state: RootState): string | null => {
  return state[SLICE_KEY]?.error ?? null;
};

export const selectTeamSectionStatus = (sectionId: string) =>
  (state: RootState): SectionStatus | undefined =>
    state[SLICE_KEY]?.sectionStatus[sectionId];

export const selectTeamSectionError = (sectionId: string) =>
  (state: RootState): string | undefined =>
    state[SLICE_KEY]?.sectionErrors[sectionId];

/**
 * Backwards-compat selector returning the flat list of currently-errored
 * section IDs (matches the pre-refactor API shape).
 */
export const selectTeamErroredSections = (state: RootState): string[] => {
  const status = state[SLICE_KEY]?.sectionStatus ?? {};
  return Object.keys(status).filter((k) => status[k] === 'errored');
};

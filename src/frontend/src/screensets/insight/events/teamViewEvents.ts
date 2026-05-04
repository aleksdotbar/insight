/**
 * Team View Events
 * Domain-specific events for team view screen
 *
 * Per-section progressive loading: each TEAM_BULLET_* / TEAM_MEMBER query
 * runs independently and emits its own SectionLoading / SectionLoaded /
 * SectionFailed events. The legacy TeamViewLoaded is no longer the canonical
 * data path but is kept in the enum for backwards compat.
 */

import '@hai3/react';
import { INSIGHT_SCREENSET_ID } from '../ids';
import type {
  TeamViewData,
  DataAvailability,
  DrillData,
  TeamMember,
  BulletMetric,
  TeamKpi,
  TeamViewConfig,
} from '../types';

const DOMAIN_ID = 'teamView';

/**
 * Discriminated union of per-section payloads emitted by
 * TeamViewSectionLoaded. Field names match the existing TeamViewData type so
 * the slice can drop each section into the existing data slot.
 */
export type TeamViewSectionData =
  | { kind: 'team_summary'; teamName: string; teamKpis: TeamKpi[]; config: TeamViewConfig }
  | { kind: 'members';      members: TeamMember[] }
  | { kind: 'bullet';       sectionId: string; metrics: BulletMetric[] };

/**
 * Events enum
 */
export enum TeamViewEvents {
  TeamViewLoadStarted        = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/loadStarted`,
  TeamViewLoaded             = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/loaded`,
  TeamViewAvailabilityLoaded = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/availabilityLoaded`,
  TeamViewLoadFailed         = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/loadFailed`,
  TeamViewSectionLoading     = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/sectionLoading`,
  TeamViewSectionLoaded      = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/sectionLoaded`,
  TeamViewSectionFailed      = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/sectionFailed`,
  DrillOpened                = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/drillOpened`,
  DrillClosed                = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/drillClosed`,
}

/**
 * Module augmentation for type-safe event payloads
 */
declare module '@hai3/react' {
  interface EventPayloadMap {
    [TeamViewEvents.TeamViewLoadStarted]:        { reason: 'team' | 'period' } | void;
    [TeamViewEvents.TeamViewLoaded]:             TeamViewData;
    [TeamViewEvents.TeamViewAvailabilityLoaded]: DataAvailability;
    [TeamViewEvents.TeamViewLoadFailed]:         string;
    [TeamViewEvents.TeamViewSectionLoading]:     { sectionId: string };
    [TeamViewEvents.TeamViewSectionLoaded]:      { sectionId: string; data: TeamViewSectionData };
    [TeamViewEvents.TeamViewSectionFailed]:      { sectionId: string; error: string };
    [TeamViewEvents.DrillOpened]:                { drillId: string; drillData: DrillData };
    [TeamViewEvents.DrillClosed]:                void;
  }
}

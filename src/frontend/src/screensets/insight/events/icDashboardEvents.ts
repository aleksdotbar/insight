/**
 * IC Dashboard Events
 * Domain-specific events for IC dashboard screen
 */

import '@hai3/react';
import { INSIGHT_SCREENSET_ID } from '../ids';
import type {
  IcDashboardData,
  IcKpi,
  BulletMetric,
  LocDataPoint,
  DeliveryDataPoint,
  TimeOffNotice,
  DrillData,
  DataAvailability,
} from '../types';
import type { IdentityPerson } from '@/app/types/identity';

const DOMAIN_ID = 'icDashboard';

/**
 * Discriminated union of per-section payloads emitted by
 * IcDashboardSectionLoaded. Field names match the existing IcDashboardData
 * type so the slice can drop each section into the existing data slot.
 */
export type IcDashboardSectionData =
  | { kind: 'kpis';          kpis: IcKpi[] }
  | { kind: 'bullet';        sectionId: string; metrics: BulletMetric[] }
  | { kind: 'locTrend';      trend: LocDataPoint[] }
  | { kind: 'deliveryTrend'; trend: DeliveryDataPoint[] }
  | { kind: 'timeOff';       notice: TimeOffNotice | null };

/**
 * Events enum
 */
export enum IcDashboardEvents {
  PersonSelected              = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/personSelected`,
  IcDashboardLoadStarted      = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/loadStarted`,
  IcDashboardLoaded           = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/loaded`,
  IcPersonLoaded              = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/personLoaded`,
  IcDashboardAvailabilityLoaded = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/availabilityLoaded`,
  IcDashboardLoadFailed       = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/loadFailed`,
  /** A single section started loading (per-section progressive rendering). */
  IcDashboardSectionLoading   = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/sectionLoading`,
  /** A single section's data arrived. */
  IcDashboardSectionLoaded    = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/sectionLoaded`,
  /** A single section's query rejected. */
  IcDashboardSectionFailed    = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/sectionFailed`,
  DrillOpened                 = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/drillOpened`,
  DrillClosed                 = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/drillClosed`,
}

/**
 * Module augmentation for type-safe event payloads
 */
declare module '@hai3/react' {
  interface EventPayloadMap {
    [IcDashboardEvents.PersonSelected]:                string;
    [IcDashboardEvents.IcDashboardLoadStarted]:        { reason: 'person' | 'period' } | void;
    [IcDashboardEvents.IcDashboardLoaded]:             IcDashboardData;
    [IcDashboardEvents.IcPersonLoaded]:                IdentityPerson;
    [IcDashboardEvents.IcDashboardAvailabilityLoaded]: DataAvailability;
    [IcDashboardEvents.IcDashboardLoadFailed]:         string;
    [IcDashboardEvents.IcDashboardSectionLoading]:     { sectionId: string };
    [IcDashboardEvents.IcDashboardSectionLoaded]:      { sectionId: string; data: IcDashboardSectionData };
    [IcDashboardEvents.IcDashboardSectionFailed]:      { sectionId: string; error: string };
    [IcDashboardEvents.DrillOpened]:                   { drillId: string; drillData: DrillData };
    [IcDashboardEvents.DrillClosed]:                   void;
  }
}

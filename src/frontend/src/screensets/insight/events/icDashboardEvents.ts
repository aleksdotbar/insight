/**
 * IC Dashboard Events
 * Domain-specific events for IC dashboard screen
 */

import '@hai3/react';
import { INSIGHT_SCREENSET_ID } from '../ids';
import type { IcDashboardData, DrillData, DataAvailability } from '../types';
import type { IdentityPerson } from '@/app/types/identity';

const DOMAIN_ID = 'icDashboard';

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
  DrillOpened                 = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/drillOpened`,
  DrillClosed                 = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/drillClosed`,
}

/**
 * Module augmentation for type-safe event payloads
 */
declare module '@hai3/react' {
  interface EventPayloadMap {
    [IcDashboardEvents.PersonSelected]:               string;
    [IcDashboardEvents.IcDashboardLoadStarted]:       void;
    [IcDashboardEvents.IcDashboardLoaded]:            IcDashboardData;
    [IcDashboardEvents.IcPersonLoaded]:               IdentityPerson;
    [IcDashboardEvents.IcDashboardAvailabilityLoaded]: DataAvailability;
    [IcDashboardEvents.IcDashboardLoadFailed]:        string;
    [IcDashboardEvents.DrillOpened]:                  { drillId: string; drillData: DrillData };
    [IcDashboardEvents.DrillClosed]:                  void;
  }
}

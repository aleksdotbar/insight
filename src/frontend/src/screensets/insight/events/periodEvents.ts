/**
 * Period Events
 * Domain-specific events for period selection
 */

import '@hai3/react';
import { INSIGHT_SCREENSET_ID } from '../ids';
import type { PeriodValue, CustomRange } from '../types';

const DOMAIN_ID = 'period';

/**
 * Events enum
 */
export enum PeriodEvents {
  PeriodChanged = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/periodChanged`,
  DateRangeSet = `${INSIGHT_SCREENSET_ID}/${DOMAIN_ID}/dateRangeSet`,
}

/**
 * Scale values: week=0.23, month=1.0, quarter=3.0, year=12.0
 */

/**
 * Module augmentation for type-safe event payloads
 */
declare module '@hai3/react' {
  interface EventPayloadMap {
    [PeriodEvents.PeriodChanged]: { period: PeriodValue; scale: number };
    [PeriodEvents.DateRangeSet]: CustomRange;
  }
}

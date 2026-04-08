/**
 * Period Actions
 * Emit events for period selection (Flux pattern)
 * Following Flux: Actions emit events for effects to update Redux
 */

import { eventBus } from '@hai3/react';
import { PeriodEvents } from '../events/periodEvents';
import type { PeriodValue, CustomRange } from '../types';

/**
 * Scale map: week=0.23, month=1.0, quarter=3.0, year=12.0
 */
const PERIOD_SCALE: Record<PeriodValue, number> = {
  week: 0.23,
  month: 1.0,
  quarter: 3.0,
  year: 12.0,
};

/**
 * Change the active period
 */
export const changePeriod = (period: PeriodValue): void => {
  const scale = PERIOD_SCALE[period];
  eventBus.emit(PeriodEvents.PeriodChanged, { period, scale });
};

/**
 * Set a custom date range
 */
export const setDateRange = (range: CustomRange): void => {
  eventBus.emit(PeriodEvents.DateRangeSet, range);
};

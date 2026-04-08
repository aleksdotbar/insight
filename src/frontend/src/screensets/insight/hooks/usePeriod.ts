/**
 * usePeriod — hook to read the active period from Redux state.
 * Hooks folder may import from slices.
 */

import { useAppSelector } from '@hai3/react';
import { selectPeriod } from '../slices/periodSlice';
import type { PeriodValue } from '../types';

export const usePeriod = (): PeriodValue => useAppSelector(selectPeriod);

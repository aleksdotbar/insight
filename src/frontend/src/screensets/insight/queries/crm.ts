/**
 * CRM query-options factory.
 *
 * One typed `queryOptions(...)` per dashboard section. The same options
 * object plugs into `useQuery`, `useSuspenseQuery`, `useQueries`, and any
 * `queryClient.{prefetch,fetch,invalidate}Queries` call — that's the
 * payoff over per-section custom hooks.
 *
 * `select:` is where the snake_case → UI shape transform happens (pure
 * functions imported from `../api/transforms`). The cache stores the raw
 * response; each consumer gets a memoised projection.
 *
 * `placeholderData: keepPreviousData` keeps previous values on screen
 * while a new period's data is in flight — the soft-revalidate pattern
 * the old slice tracked manually as `revalidating`.
 *
 * Identity / period / view-mode state stays in Redux. React Query owns
 * server cache; Redux owns UI state. Two boundaries, two systems.
 */

import { queryOptions, keepPreviousData } from '@tanstack/react-query';
import { toLower } from 'lodash';
import { apiRegistry } from '@hai3/react';
import { InsightApiService } from '../api/insightApiService';
import { METRIC_REGISTRY } from '../api/metricRegistry';
import {
  transformCrmKpis,
  transformCrmFlow,
  transformCrmBullets,
  CRM_BULLET_DEFS_QUALITY,
  CRM_BULLET_DEFS_ACTIVITY,
} from '../api/transforms';
import { odataEscapeValue, type DateRange } from '../utils/periodToDateRange';
import { crmKeys } from './keys';
import type { PeriodValue } from '../types';
import type {
  RawCrmKpisRow,
  RawCrmFlowRow,
  RawBulletAggregateRow,
} from '../api/rawTypes';

const api = () => apiRegistry.getService(InsightApiService);
const personScope = (personId: string) =>
  `person_id eq '${odataEscapeValue(toLower(personId))}'`;

/**
 * Shift a date range back by exactly one year. Only `prevKpis` uses this,
 * so it stays scoped to this module.
 */
function priorYearRange(range: DateRange): DateRange {
  const shift = (iso: string): string => {
    const [y, m, d] = iso.split('-').map(Number);
    const date = new Date(y, (m ?? 1) - 1, d ?? 1);
    date.setFullYear(date.getFullYear() - 1);
    const yy = date.getFullYear();
    const mm = String(date.getMonth() + 1).padStart(2, '0');
    const dd = String(date.getDate()).padStart(2, '0');
    return `${yy}-${mm}-${dd}`;
  };
  return { from: shift(range.from), to: shift(range.to) };
}

export const crmQueries = {
  kpis: (personId: string, range: DateRange) => queryOptions({
    queryKey: crmKeys.kpis(personId, range),
    queryFn:  () => api().queryMetric<RawCrmKpisRow>(
      METRIC_REGISTRY.CRM_KPIS,
      range,
      { $filter: personScope(personId) },
    ),
    select: (r) => transformCrmKpis(r.items[0] ?? null),
    placeholderData: keepPreviousData,
  }),

  prevKpis: (personId: string, range: DateRange) => queryOptions({
    queryKey: crmKeys.prevKpis(personId, range),
    queryFn:  () => api().queryMetric<RawCrmKpisRow>(
      METRIC_REGISTRY.CRM_KPIS,
      priorYearRange(range),
      { $filter: personScope(personId) },
    ),
    select: (r) => transformCrmKpis(r.items[0] ?? null),
    placeholderData: keepPreviousData,
  }),

  flow: (personId: string, range: DateRange) => queryOptions({
    queryKey: crmKeys.flow(personId, range),
    queryFn:  () => api().queryMetric<RawCrmFlowRow>(
      METRIC_REGISTRY.CRM_CHART_FLOW,
      range,
      { $filter: personScope(personId) },
    ),
    select: (r) => transformCrmFlow(r.items),
    placeholderData: keepPreviousData,
  }),

  /**
   * Velocity & Quality (`kind: 'quality'`) and Outreach Activity
   * (`kind: 'activity'`) share the same view + bullet transform shape;
   * only the metric UUID, def list, and `section` label differ.
   */
  bullet: (
    personId: string,
    range: DateRange,
    kind: 'quality' | 'activity',
    period: PeriodValue,
  ) => {
    const uuid      = kind === 'quality' ? METRIC_REGISTRY.CRM_BULLET_QUALITY : METRIC_REGISTRY.CRM_BULLET_ACTIVITY;
    const defs      = kind === 'quality' ? CRM_BULLET_DEFS_QUALITY            : CRM_BULLET_DEFS_ACTIVITY;
    const sectionId = kind === 'quality' ? 'velocity_quality'                 : 'outreach_activity';
    return queryOptions({
      queryKey: crmKeys.bullet(personId, range, kind),
      queryFn:  () => api().queryMetric<RawBulletAggregateRow>(
        uuid,
        range,
        { $filter: personScope(personId) },
      ),
      select: (r) => transformCrmBullets(r.items, period, sectionId, defs),
      placeholderData: keepPreviousData,
    });
  },
};

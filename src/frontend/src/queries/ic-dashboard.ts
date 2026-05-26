import { useQuery, type UseQueryResult } from "@tanstack/react-query";

import { queryMetric } from "@/api/analytics-client";
import { METRIC_REGISTRY } from "@/api/metric-registry";
import {
  previousPeriodRange,
  type DateRange,
} from "@/api/period-to-date-range";
import { odataEscapeValue } from "@/api/odata";
import { getPerson } from "@/api/identity-client";
import {
  transformBulletMetrics,
  transformDeliveryTrend,
  transformDrill,
  transformIcKpis,
  transformLocTrend,
  transformTimeOff,
} from "@/api/transforms";
import type {
  RawBulletAggregateRow,
  RawDeliveryTrendRow,
  RawDrillRow,
  RawIcAggregateRow,
  RawLocTrendRow,
  RawTimeOffRow,
} from "@/api/raw-types";
import type {
  BulletMetric,
  DeliveryDataPoint,
  DrillData,
  IcKpi,
  IdentityPerson,
  LocDataPoint,
  PeriodValue,
  TimeOffNotice,
} from "@/types/insight";

function personScope(personId: string): string {
  return `person_id eq '${odataEscapeValue(personId.toLowerCase())}'`;
}

export function useIcPerson(personId: string): UseQueryResult<IdentityPerson> {
  return useQuery({
    queryKey: ["identity", "person", personId.toLowerCase()],
    queryFn: () => getPerson(personId),
    enabled: Boolean(personId),
  });
}

export function useIcKpis(
  personId: string,
  period: PeriodValue,
  range: DateRange
): UseQueryResult<IcKpi[]> {
  const prevRange = previousPeriodRange(range, period);
  return useQuery({
    queryKey: ["ic", "kpis", personId, period, range.from, range.to],
    queryFn: async () => {
      const [cur, prev] = await Promise.all([
        queryMetric<RawIcAggregateRow>(METRIC_REGISTRY.IC_KPIS, range, {
          $filter: personScope(personId),
        }),
        queryMetric<RawIcAggregateRow>(METRIC_REGISTRY.IC_KPIS, prevRange, {
          $filter: personScope(personId),
        }),
      ]);
      return transformIcKpis(
        cur.items[0] ?? null,
        prev.items[0] ?? null,
        period
      );
    },
  });
}

const BULLET_SECTIONS = {
  task_delivery: METRIC_REGISTRY.IC_BULLET_DELIVERY,
  code_quality: METRIC_REGISTRY.IC_BULLET_DELIVERY,
  collaboration: METRIC_REGISTRY.IC_BULLET_COLLAB,
  ai_adoption: METRIC_REGISTRY.IC_BULLET_AI,
  git_output: METRIC_REGISTRY.IC_BULLET_GIT,
} as const;

export type IcBulletSectionId = keyof typeof BULLET_SECTIONS;

export function useIcBulletSection(
  sectionId: IcBulletSectionId,
  personId: string,
  period: PeriodValue,
  range: DateRange
): UseQueryResult<BulletMetric[]> {
  const metricId = BULLET_SECTIONS[sectionId];
  return useQuery({
    queryKey: [
      "ic",
      "bullet",
      sectionId,
      personId,
      period,
      range.from,
      range.to,
    ],
    queryFn: async () => {
      const resp = await queryMetric<RawBulletAggregateRow>(metricId, range, {
        $filter: personScope(personId),
      });
      return transformBulletMetrics(resp.items, sectionId, period);
    },
  });
}

export function useIcLocTrend(
  personId: string,
  period: PeriodValue,
  range: DateRange
): UseQueryResult<LocDataPoint[]> {
  return useQuery({
    queryKey: ["ic", "loc-trend", personId, period, range.from, range.to],
    queryFn: async () => {
      const resp = await queryMetric<RawLocTrendRow>(
        METRIC_REGISTRY.IC_CHART_LOC,
        range,
        { $filter: personScope(personId) }
      );
      return transformLocTrend(resp.items, period);
    },
  });
}

export function useIcDeliveryTrend(
  personId: string,
  period: PeriodValue,
  range: DateRange
): UseQueryResult<DeliveryDataPoint[]> {
  return useQuery({
    queryKey: ["ic", "delivery-trend", personId, period, range.from, range.to],
    queryFn: async () => {
      const resp = await queryMetric<RawDeliveryTrendRow>(
        METRIC_REGISTRY.IC_CHART_DELIVERY,
        range,
        { $filter: personScope(personId) }
      );
      return transformDeliveryTrend(resp.items, period);
    },
  });
}

export function useIcTimeOff(
  personId: string,
  range: DateRange
): UseQueryResult<TimeOffNotice | null> {
  return useQuery({
    queryKey: ["ic", "timeoff", personId, range.from, range.to],
    queryFn: async () => {
      const resp = await queryMetric<RawTimeOffRow>(
        METRIC_REGISTRY.IC_TIMEOFF,
        range,
        { $filter: personScope(personId) }
      );
      return resp.items[0] ? transformTimeOff(resp.items[0]) : null;
    },
  });
}

export function useIcDrill(
  personId: string,
  drillId: string | null,
  range: DateRange
): UseQueryResult<DrillData | null> {
  return useQuery({
    queryKey: ["ic", "drill", drillId, personId, range.from, range.to],
    enabled: Boolean(drillId),
    queryFn: async () => {
      if (!drillId) return null;
      const filter = `${personScope(personId)} and drill_id eq '${odataEscapeValue(drillId)}'`;
      const resp = await queryMetric<RawDrillRow>(
        METRIC_REGISTRY.IC_DRILL,
        range,
        { $filter: filter }
      );
      return resp.items[0] ? transformDrill(resp.items[0]) : null;
    },
  });
}

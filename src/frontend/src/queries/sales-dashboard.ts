import { useQueries, useQuery, type UseQueryResult } from "@tanstack/react-query";

import { queryMetric } from "@/api/analytics-client";
import { METRIC_REGISTRY } from "@/api/metric-registry";
import { odataEscapeValue } from "@/api/odata";
import { type DateRange, toISODate } from "@/api/period-to-date-range";
import {
  CRM_BULLET_DEFS_ACTIVITY,
  CRM_BULLET_DEFS_QUALITY,
  transformCrmFlow,
  transformCrmBullets,
  transformCrmKpis,
} from "@/api/transforms";
import type {
  RawBulletAggregateRow,
  RawCrmFlowRow,
  RawCrmKpisRow,
} from "@/api/raw-types";
import type {
  BulletMetric,
  CrmFlowPoint,
  CrmKpis,
  PeriodValue,
} from "@/types/insight";

function personScope(personId: string): string {
  return `person_id eq '${odataEscapeValue(personId.toLowerCase())}'`;
}

/**
 * Shift both endpoints back by exactly one calendar year.
 *
 * Intentionally distinct from `previousPeriodRange()` in
 * `@/api/period-to-date-range`: that helper is period-aware (week → prior
 * week, month → prior month, etc.) for the engineering dashboard's
 * delta-vs-last-period KPIs. The sales pacing band specifically wants a
 * year-over-year comparison ("Closed $ this period vs prior-year same
 * period") — same calendar window, one year earlier — regardless of which
 * period bucket the user selected. Do NOT consolidate the two helpers.
 */
function priorYearRange(range: DateRange): DateRange {
  const shift = (iso: string): string => {
    const [y, m, d] = iso.split("-").map(Number);
    const date = new Date(y, (m ?? 1) - 1, d ?? 1);
    date.setFullYear(date.getFullYear() - 1);
    return toISODate(date);
  };
  return { from: shift(range.from), to: shift(range.to) };
}

export function useSalesKpis(
  personId: string,
  range: DateRange,
): UseQueryResult<CrmKpis | null> {
  return useQuery({
    queryKey: ["crm", "kpis", personId, range.from, range.to],
    queryFn: async () => {
      const resp = await queryMetric<RawCrmKpisRow>(
        METRIC_REGISTRY.CRM_KPIS,
        range,
        { $filter: personScope(personId) },
      );
      return transformCrmKpis(resp.items[0] ?? null);
    },
  });
}

export function useSalesPrevKpis(
  personId: string,
  range: DateRange,
): UseQueryResult<CrmKpis | null> {
  const prev = priorYearRange(range);
  return useQuery({
    queryKey: ["crm", "prev-kpis", personId, range.from, range.to],
    queryFn: async () => {
      const resp = await queryMetric<RawCrmKpisRow>(
        METRIC_REGISTRY.CRM_KPIS,
        prev,
        { $filter: personScope(personId) },
      );
      return transformCrmKpis(resp.items[0] ?? null);
    },
  });
}

export function useSalesFlow(
  personId: string,
  range: DateRange,
): UseQueryResult<CrmFlowPoint[]> {
  return useQuery({
    queryKey: ["crm", "flow", personId, range.from, range.to],
    queryFn: async () => {
      const resp = await queryMetric<RawCrmFlowRow>(
        METRIC_REGISTRY.CRM_CHART_FLOW,
        range,
        { $filter: personScope(personId) },
      );
      return transformCrmFlow(resp.items);
    },
  });
}

export type SalesBulletKind = "quality" | "activity";

export function useSalesBulletSection(
  kind: SalesBulletKind,
  personId: string,
  period: PeriodValue,
  range: DateRange,
): UseQueryResult<BulletMetric[]> {
  const uuid =
    kind === "quality"
      ? METRIC_REGISTRY.CRM_BULLET_QUALITY
      : METRIC_REGISTRY.CRM_BULLET_ACTIVITY;
  const defs =
    kind === "quality" ? CRM_BULLET_DEFS_QUALITY : CRM_BULLET_DEFS_ACTIVITY;
  const sectionId =
    kind === "quality" ? "velocity_quality" : "outreach_activity";
  return useQuery({
    queryKey: [
      "crm",
      "bullet",
      kind,
      personId,
      period,
      range.from,
      range.to,
    ],
    queryFn: async () => {
      const resp = await queryMetric<RawBulletAggregateRow>(uuid, range, {
        $filter: personScope(personId),
      });
      return transformCrmBullets(resp.items, period, sectionId, defs);
    },
  });
}

export function useSalesDashboardQueries(
  personId: string,
  period: PeriodValue,
  range: DateRange,
): {
  kpisQ: UseQueryResult<CrmKpis | null>;
  prevKpisQ: UseQueryResult<CrmKpis | null>;
  flowQ: UseQueryResult<CrmFlowPoint[]>;
  qualityQ: UseQueryResult<BulletMetric[]>;
  activityQ: UseQueryResult<BulletMetric[]>;
} {
  const prevRange = priorYearRange(range);
  const filter = personScope(personId);
  const results = useQueries({
    queries: [
      {
        queryKey: ["crm", "kpis", personId, range.from, range.to],
        queryFn: async () => {
          const resp = await queryMetric<RawCrmKpisRow>(
            METRIC_REGISTRY.CRM_KPIS,
            range,
            { $filter: filter },
          );
          return transformCrmKpis(resp.items[0] ?? null);
        },
      },
      {
        queryKey: ["crm", "prev-kpis", personId, range.from, range.to],
        queryFn: async () => {
          const resp = await queryMetric<RawCrmKpisRow>(
            METRIC_REGISTRY.CRM_KPIS,
            prevRange,
            { $filter: filter },
          );
          return transformCrmKpis(resp.items[0] ?? null);
        },
      },
      {
        queryKey: ["crm", "flow", personId, range.from, range.to],
        queryFn: async () => {
          const resp = await queryMetric<RawCrmFlowRow>(
            METRIC_REGISTRY.CRM_CHART_FLOW,
            range,
            { $filter: filter },
          );
          return transformCrmFlow(resp.items);
        },
      },
      {
        queryKey: ["crm", "bullet", "quality", personId, period, range.from, range.to],
        queryFn: async () => {
          const resp = await queryMetric<RawBulletAggregateRow>(
            METRIC_REGISTRY.CRM_BULLET_QUALITY,
            range,
            { $filter: filter },
          );
          return transformCrmBullets(
            resp.items,
            period,
            "velocity_quality",
            CRM_BULLET_DEFS_QUALITY,
          );
        },
      },
      {
        queryKey: ["crm", "bullet", "activity", personId, period, range.from, range.to],
        queryFn: async () => {
          const resp = await queryMetric<RawBulletAggregateRow>(
            METRIC_REGISTRY.CRM_BULLET_ACTIVITY,
            range,
            { $filter: filter },
          );
          return transformCrmBullets(
            resp.items,
            period,
            "outreach_activity",
            CRM_BULLET_DEFS_ACTIVITY,
          );
        },
      },
    ],
  });
  return {
    kpisQ: results[0] as UseQueryResult<CrmKpis | null>,
    prevKpisQ: results[1] as UseQueryResult<CrmKpis | null>,
    flowQ: results[2] as UseQueryResult<CrmFlowPoint[]>,
    qualityQ: results[3] as UseQueryResult<BulletMetric[]>,
    activityQ: results[4] as UseQueryResult<BulletMetric[]>,
  };
}

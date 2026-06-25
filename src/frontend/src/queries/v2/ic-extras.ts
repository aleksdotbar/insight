import { keepPreviousData, useQuery, type UseQueryResult } from "@tanstack/react-query";

import {
  queryBatchWithRange,
  queryMetric,
  type BatchQueryItem,
  type BatchQueryResult,
} from "@/api/analytics-client";
import { METRIC_REGISTRY } from "@/api/metric-registry";
import { odataEscapeValue } from "@/api/odata";
import type { DateRange } from "@/api/period-to-date-range";
import type {
  RawDeliveryTrendRow,
  RawLocTrendRow,
} from "@/api/raw-types";
import type {
  DeliveryDataPoint,
  LocDataPoint,
  PeriodValue,
} from "@/types/insight";
import {
  transformDeliveryTrend,
  transformLocTrend,
} from "@/api/transforms";

function personFilter(personId: string): string {
  return `person_id eq '${odataEscapeValue(personId.toLowerCase())}'`;
}

export interface HistogramBin {
  metric_key: string;
  bin: number;
  bin_end: number;
  count: number;
}

export function icHistogramQueryOptions(
  personId: string,
  metricKey: string,
  range: DateRange,
) {
  return {
    queryKey: ["v2", "ic-histogram", personId, metricKey, range.from, range.to],
    enabled: Boolean(personId && metricKey),
    queryFn: async () => {
      const resp = await queryMetric<HistogramBin>(
        METRIC_REGISTRY.V2_IC_HISTOGRAM,
        range,
        {
          $filter: `${personFilter(personId)} and metric_key eq '${odataEscapeValue(metricKey)}'`,
        },
      );
      return resp.items;
    },
  };
}

export function useIcHistogram(
  personId: string,
  metricKey: string,
  range: DateRange,
): UseQueryResult<HistogramBin[]> {
  return useQuery(icHistogramQueryOptions(personId, metricKey, range));
}

export type {
  CompositionRow,
  CollabActivityRow,
} from "@/lib/insight/v2/derivations";

export interface SectionTrendPointRow {
  date: string;
  [key: string]: number | string;
}

interface SectionTrendLongRow {
  metric_date?: string;
  date?: string;
  series_key: string;
  value: number;
}

export interface AiToolSummaryRow {
  person_id: string;
  tool: string;
  accepted_lines_added: number;
  accepted_lines_removed: number;
  cost_cents: number | null;
  active_days: number;
}

export interface AiToolTrendRow {
  person_id: string;
  metric_date: string;
  tool: string;
  accepted_lines_added: number;
}

export interface AiPeerCounterRow {
  person_id: string;
  org_unit_id: string | null;
  metric_key: string;
  value: number | null;
  median: number | null;
  p25: number | null;
  p75: number | null;
  n: number | null;
  range_min: number | null;
  range_max: number | null;
}

function pivotLongToWide(
  rows: ReadonlyArray<SectionTrendLongRow | SectionTrendPointRow>,
): SectionTrendPointRow[] {
  if (rows.length === 0) return [];
  const first = rows[0] as Record<string, unknown>;
  const isLong =
    typeof first.series_key === "string" && typeof first.value === "number";
  if (!isLong) return rows as SectionTrendPointRow[];

  const byDate = new Map<string, SectionTrendPointRow>();
  for (const r of rows as SectionTrendLongRow[]) {
    const date = r.metric_date ?? r.date ?? "";
    if (!date) continue;
    let point = byDate.get(date);
    if (!point) {
      point = { date };
      byDate.set(date, point);
    }
    point[r.series_key] = r.value;
  }
  return Array.from(byDate.values()).sort((a, b) =>
    String(a.date) < String(b.date) ? -1 : 1,
  );
}

export function useIcSectionTrend(
  personId: string,
  sectionId: string,
  range: DateRange,
): UseQueryResult<SectionTrendPointRow[]> {
  return useQuery({
    queryKey: [
      "v2",
      "ic-section-trend",
      personId,
      sectionId,
      range.from,
      range.to,
    ],
    enabled: Boolean(personId && sectionId),
    placeholderData: keepPreviousData,
    queryFn: async () => {
      const resp = await queryMetric<SectionTrendLongRow>(
        METRIC_REGISTRY.V2_IC_SECTION_TREND,
        range,
        {
          $filter: `${personFilter(personId)} and section_id eq '${sectionId}'`,
        },
      );
      return pivotLongToWide(resp.items);
    },
  });
}

export function useIcAiToolSummary(
  personId: string,
  range: DateRange,
): UseQueryResult<AiToolSummaryRow[]> {
  return useQuery({
    queryKey: ["v2", "ic-ai-tool-summary", personId, range.from, range.to],
    enabled: Boolean(personId && range.from && range.to),
    placeholderData: keepPreviousData,
    queryFn: async () => {
      const resp = await queryMetric<AiToolSummaryRow>(
        METRIC_REGISTRY.V2_IC_AI_TOOL_SUMMARY,
        range,
        { $filter: personFilter(personId) },
      );
      return resp.items;
    },
  });
}

export function useIcAiToolTrend(
  personId: string,
  range: DateRange,
): UseQueryResult<AiToolTrendRow[]> {
  return useQuery({
    queryKey: ["v2", "ic-ai-tool-trend", personId, range.from, range.to],
    enabled: Boolean(personId && range.from && range.to),
    placeholderData: keepPreviousData,
    queryFn: async () => {
      const resp = await queryMetric<AiToolTrendRow>(
        METRIC_REGISTRY.V2_IC_AI_TOOL_TREND,
        range,
        { $filter: personFilter(personId) },
      );
      return resp.items;
    },
  });
}

export function useIcAiPeerCounters(
  personId: string,
  range: DateRange,
): UseQueryResult<AiPeerCounterRow[]> {
  return useQuery({
    queryKey: ["v2", "ic-ai-peer-counters", personId, range.from, range.to],
    enabled: Boolean(personId && range.from && range.to),
    placeholderData: keepPreviousData,
    queryFn: async () => {
      const resp = await queryMetric<AiPeerCounterRow>(
        METRIC_REGISTRY.V2_IC_AI_PEER_COUNTERS,
        range,
        { $filter: personFilter(personId) },
      );
      return resp.items;
    },
  });
}

export interface DrilldownBatchData {
  histograms: Map<string, HistogramBin[]>;
  delivery: DeliveryDataPoint[] | null;
  loc: LocDataPoint[] | null;
  sectionTrend: SectionTrendPointRow[] | null;
}

type DrilldownItem = unknown;

function getItems<T>(
  byId: Map<string, BatchQueryResult<DrilldownItem>>,
  id: string,
): T[] | null {
  const r = byId.get(id);
  if (!r || r.status !== "ok") return null;
  return r.items as T[];
}

export interface IcDrilldownBatchOpts {
  sectionId: string | null;
  personId: string | null;
  range: DateRange | null;
  period: PeriodValue | null;
}

export function icDrilldownBatchQueryOptions(opts: IcDrilldownBatchOpts) {
  const { sectionId, personId, range, period } = opts;
  return {
    queryKey: [
      "v2",
      "ic-drilldown-batch",
      sectionId,
      personId?.toLowerCase() ?? "",
      period ?? "",
      range?.from ?? "",
      range?.to ?? "",
    ],
    enabled: Boolean(sectionId && personId && range && period),
    placeholderData: keepPreviousData,
    queryFn: async (): Promise<DrilldownBatchData> => {
      if (!sectionId || !personId || !range || !period) {
        return {
          histograms: new Map(),
          delivery: null,
          loc: null,
          sectionTrend: null,
        };
      }

      const pfilter = personFilter(personId);
      const items: BatchQueryItem[] = [
        {
          id: "histograms",
          metric_id: METRIC_REGISTRY.V2_IC_HISTOGRAM,
          $filter: pfilter,
        },
      ];

      if (sectionId === "task_delivery" || sectionId === "git_output") {
        items.push({
          id: "delivery",
          metric_id: METRIC_REGISTRY.IC_CHART_DELIVERY,
          $filter: pfilter,
        });
      }
      if (sectionId === "git_output") {
        items.push({
          id: "loc",
          metric_id: METRIC_REGISTRY.IC_CHART_LOC,
          $filter: pfilter,
        });
      }
      if (sectionId === "code_quality" || sectionId === "ai_adoption") {
        items.push({
          id: "section_trend",
          metric_id: METRIC_REGISTRY.V2_IC_SECTION_TREND,
          $filter: `${pfilter} and section_id eq '${sectionId}'`,
        });
      }
      const resp = await queryBatchWithRange<DrilldownItem>(range, items);
      const byId = new Map<string, BatchQueryResult<DrilldownItem>>();
      for (const r of resp.results) {
        if (r.id) byId.set(r.id, r);
      }

      const histograms = new Map<string, HistogramBin[]>();
      const histRows = getItems<HistogramBin>(byId, "histograms");
      if (histRows) {
        for (const row of histRows) {
          let arr = histograms.get(row.metric_key);
          if (!arr) {
            arr = [];
            histograms.set(row.metric_key, arr);
          }
          arr.push(row);
        }
      }

      const deliveryRows = getItems<RawDeliveryTrendRow>(byId, "delivery");
      const locRows = getItems<RawLocTrendRow>(byId, "loc");
      const sectionTrendRows = getItems<SectionTrendLongRow>(byId, "section_trend");

      return {
        histograms,
        delivery: deliveryRows ? transformDeliveryTrend(deliveryRows, period) : null,
        loc: locRows ? transformLocTrend(locRows, period) : null,
        sectionTrend: sectionTrendRows
          ? pivotLongToWide(sectionTrendRows)
          : null,
      };
    },
  } as const;
}

export function useIcDrilldownBatch(
  opts: IcDrilldownBatchOpts,
): UseQueryResult<DrilldownBatchData> {
  return useQuery(icDrilldownBatchQueryOptions(opts));
}

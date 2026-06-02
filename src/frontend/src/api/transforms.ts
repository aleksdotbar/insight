/**
 * Design rules:
 * - Explicit field mapping only (no generic snake->camel converter).
 * - Every function is stateless and side-effect free.
 * - All metric metadata (labels / sublabels / thresholds) comes from the
 *   wire catalog (`useCatalog()`). No compile-in metric defaults — when
 *   the catalog is unavailable the transforms emit empty arrays and
 *   consumers render skeletons or empty states (Refs #82).
 */

import type {
  PeriodValue,
  ExecTeamRow,
  ExecViewConfig,
  IcKpi,
  BulletMetric,
  CrmFlowPoint,
  CrmKpis,
  LocDataPoint,
  DeliveryDataPoint,
  TeamMember,
  TimeOffNotice,
  DrillData,
} from '@/types/insight';
import type {
  RawExecSummaryRow,
  RawIcAggregateRow,
  RawBulletAggregateRow,
  RawCrmFlowRow,
  RawCrmKpisRow,
  RawLocTrendRow,
  RawDeliveryTrendRow,
  RawTeamMemberRow,
  RawTimeOffRow,
  RawDrillRow,
} from './raw-types';
import {
  type CatalogMetric,
  type CatalogResponse,
  prefixForBulletSection,
} from './catalog-client';
import { evaluateStatus } from './metric-semantics';

/** Format strings the IC KPI catalog rows surface. */
type IcKpiFormat = 'integer' | 'decimal1' | 'percent' | 'hours';

function asIcKpiFormat(v: string | undefined): IcKpiFormat {
  if (v === 'integer' || v === 'decimal1' || v === 'percent' || v === 'hours') {
    return v;
  }
  // Wire row predates the `format` field — keep IC KPI rendering safe by
  // defaulting to whole-number display.
  return 'integer';
}

function keyBy<T, K extends keyof T>(items: T[], key: K): Record<string, T> {
  const out: Record<string, T> = {};
  for (const item of items) {
    out[String(item[key])] = item;
  }
  return out;
}

function clamp(v: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, v));
}

function pctInRange(value: number, rangeMin: number, rangeMax: number): number {
  const span = rangeMax - rangeMin;
  if (span <= 0) return 0;
  return Math.round(clamp(((value - rangeMin) / span) * 100, 0, 100));
}

function formatValue(raw: number, fmt: IcKpiFormat): string {
  // Whole-number display across the board. Fractional precision was noisy on
  // a dashboard that re-aggregates per period — "78.4%" jitters to "78.6%"
  // when the period nudges by a day. Integers stay readable and stable.
  switch (fmt) {
    case 'integer': return String(Math.round(raw));
    case 'decimal1': return String(Math.round(raw));
    case 'percent': return String(Math.round(raw));
    case 'hours': return `${Math.round(raw)}h`;
  }
}

function formatDelta(delta: number, fmt: IcKpiFormat): string {
  const sign = delta > 0 ? '+' : '';
  switch (fmt) {
    case 'integer': return `${sign}${Math.round(delta)}`;
    case 'decimal1': return `${sign}${Math.round(delta)}`;
    case 'percent': return `${sign}${Math.round(delta)}%`;
    case 'hours': return `${sign}${Math.round(delta)}h`;
  }
}

function deltaType(
  delta: number,
  higherIsBetter: boolean,
): 'good' | 'bad' | 'neutral' {
  const threshold = 0.001;
  if (Math.abs(delta) < threshold) return 'neutral';
  if (higherIsBetter) return delta > 0 ? 'good' : 'bad';
  return delta < 0 ? 'good' : 'bad';
}

// Cache Intl formatters — constructing them per-call is surprisingly
// expensive, and the labels only change when the locale changes (never
// during a normal session).
const WEEKDAY_FMT = new Intl.DateTimeFormat(undefined, { weekday: 'short' });
const MONTH_FMT   = new Intl.DateTimeFormat(undefined, { month: 'short' });

function formatDateLabel(isoDate: string, period: PeriodValue): string {
  // Parse as local-midnight so label reflects the user's timezone, not UTC.
  const [y, m, day] = isoDate.split('-').map(Number);
  const d = new Date(y, (m ?? 1) - 1, day ?? 1);
  switch (period) {
    case 'week':
      // Short weekday label in the user's locale (e.g. "Mon", "Пн", "月").
      return WEEKDAY_FMT.format(d);
    case 'month': {
      // Week number within the month: W1, W2, ...
      const weekNum = Math.ceil(d.getDate() / 7);
      return `W${weekNum}`;
    }
    case 'quarter':
      // Short month label in the user's locale.
      return MONTH_FMT.format(d);
    case 'year': {
      const q = Math.floor(d.getMonth() / 3) + 1;
      return `Q${q}`;
    }
  }
}

function formatRangeStr(value: number, unit: string): string {
  // Range edges (min/max for the bullet scale) come from CH as raw floats \u2014
  // round here so '783637.2667' doesn't leak to the screen. Matches the
  // whole-number policy applied to the value/median in formatBulletValue.
  const v = Math.round(value);
  if (unit === '%') return `${v}%`;
  if (unit === 'h' || unit === 'h/mo') return `${v}h`;
  if (unit === 'd') return `${v}d`;
  if (unit === '\u00d7') return `${v}\u00d7`;
  return String(v);
}

function formatBulletValue(raw: number | null | undefined, _unit: string): string {
  if (raw === null || raw === undefined || !Number.isFinite(raw)) return '—';
  // Whole-number display — see formatValue() rationale.
  return String(Math.round(raw));
}

/**
 * Auto-convert hour-scale bullets to days when the chart would otherwise
 * display 3-digit hour values that read as "long opaque numbers". Industry
 * dashboards (Jellyfish, LinearB, Atlassian) flip to days at the same
 * threshold. The whole bullet (value/median/min/max) shares a unit, so we
 * decide once per row using the upper edge of the range.
 *
 * Returns null when no scaling needed; otherwise scaled numbers + new unit.
 */
function scaleHoursToDays(
  unit: string,
  value: number | null | undefined,
  median: number | null | undefined,
  rangeMin: number | null | undefined,
  rangeMax: number | null | undefined,
): null | {
  unit: string;
  value: number | null | undefined;
  median: number | null | undefined;
  rangeMin: number | null | undefined;
  rangeMax: number | null | undefined;
} {
  if (unit !== 'h') return null;
  if (rangeMax == null || !Number.isFinite(rangeMax) || rangeMax < 48) return null;
  const toDays = (n: number | null | undefined): number | null | undefined =>
    n == null || !Number.isFinite(n) ? n : Math.round(n / 24);
  return {
    unit: 'd',
    value:    toDays(value),
    median:   toDays(median),
    rangeMin: toDays(rangeMin),
    rangeMax: toDays(rangeMax),
  };
}

export function transformExecRows(
  rows: RawExecSummaryRow[],
  thresholds?: ExecViewConfig['column_thresholds'],
): ExecTeamRow[] {
  // Round so UInt64/Float64 from backend render cleanly. Preserve null —
  // previously `toInt(null) === 0` silently turned "source absent" into "0".
  const roundOrNull = (v: number | null | undefined): number | null =>
    v == null || !Number.isFinite(v) ? null : Math.round(v);

  return rows.map((r) => {
    let status: 'good' | 'warn' | 'bad' = 'good';

    if (thresholds) {
      const warnings = thresholds.filter((t) => {
        const val = r[t.metric_key as keyof RawExecSummaryRow] as number | null;
        return val !== null && val < t.threshold;
      });
      if (warnings.length >= 2) status = 'bad';
      else if (warnings.length === 1) status = 'warn';
    }

    return {
      team_id: r.org_unit_id,
      team_name: r.org_unit_name,
      headcount: r.headcount,
      tasks_closed: roundOrNull(r.tasks_closed),
      bugs_fixed: roundOrNull(r.bugs_fixed),
      build_success_pct: r.build_success_pct,
      focus_time_pct: r.focus_time_pct,
      ai_adoption_pct: r.ai_adoption_pct,
      ai_loc_share_pct: r.ai_loc_share_pct,
      pr_cycle_time_h: r.pr_cycle_time_h,
      status,
    };
  });
}

const IC_KPI_PREFIX = 'ic_kpis.';

export function transformIcKpis(
  current: RawIcAggregateRow | null,
  previous: RawIcAggregateRow | null,
  period: PeriodValue,
  catalog: CatalogResponse | undefined,
): IcKpi[] {
  // No catalog → no labels → consumers render skeletons / empty states.
  if (!catalog) return [];
  // No current row → backend has nothing for this person in the period.
  if (current === null) return [];

  // The bare `metric_key` (post-`ic_kpis.` prefix) is the column name on
  // `RawIcAggregateRow` — wire seed migration `m20260527_000001` keeps
  // the catalog key in sync with the gold table column. Catalog rows
  // whose bare key isn't an own property of the raw aggregate are
  // silently omitted (the FE doesn't know how to source their value).
  // `hasOwnProperty` (not `in`) so a malformed wire key like `__proto__`
  // or `toString` can't reach `Object.prototype` members.
  type IcRow = RawIcAggregateRow;

  const out: IcKpi[] = [];
  for (const m of catalog.metrics) {
    if (!m.metric_key || !m.metric_key.startsWith(IC_KPI_PREFIX)) continue;
    const bareKey = m.metric_key.slice(IC_KPI_PREFIX.length);
    if (!Object.prototype.hasOwnProperty.call(current, bareKey)) continue;
    const rawField = bareKey as keyof IcRow;

    // Distinguish "raw value missing" (NULL from backend → source not
    // ingested) from "raw value is zero" (real measurement).
    const rawCur = current[rawField];
    const rawPrev = previous?.[rawField];
    const curVal =
      rawCur == null || typeof rawCur !== 'number' ? null : rawCur;
    const prevVal =
      rawPrev == null || typeof rawPrev !== 'number' ? null : rawPrev;

    const format = asIcKpiFormat(m.format);
    const value = curVal === null ? null : formatValue(curVal, format);
    let delta = '';
    let dt: 'good' | 'warn' | 'bad' | 'neutral' = 'neutral';

    if (curVal !== null && prevVal !== null) {
      const diff = curVal - prevVal;
      delta = formatDelta(diff, format);
      dt = deltaType(diff, m.higher_is_better);
    }

    out.push({
      period,
      metric_key: bareKey,
      label: m.label,
      value,
      raw_value: curVal,
      unit: m.unit ?? '',
      sublabel: m.sublabel ?? '',
      description: m.description,
      delta,
      delta_type: dt,
    });
  }
  return out;
}

/**
 * @param teamSize  Headcount of the team that context applies to. Used to
 *                  rewrite unit/range_max for member-scale metrics
 *                  (`active_ai_members` etc.) so bullets show "N / 113"
 *                  instead of the old hardcoded "N / 12". When unknown
 *                  (e.g. IC Dashboard context), member-scale metrics render
 *                  as 'unavailable' — we don't invent a denominator.
 */
export function transformBulletMetrics(
  rows: RawBulletAggregateRow[],
  section: string,
  period: PeriodValue,
  teamSize: number | undefined,
  _viewKind: 'ic' | 'team',
  catalog: CatalogResponse | undefined,
): BulletMetric[] {
  // No catalog → no labels → consumers render skeletons. The screen
  // surfaces `isError` / `isLoading` directly from `useCatalog()` so
  // returning empty here is the right "no data" shape.
  if (!catalog) return [];

  const wirePrefixDot = prefixForBulletSection(section) + '.';

  // Section catalog rows, keyed by bare metric_key (the form raw aggregate
  // rows carry, e.g. `tasks_completed`). Catalog metric_keys are
  // wire-prefixed (e.g. `task_delivery_bullet_rows.tasks_completed`).
  const catalogByBareKey = new Map<string, CatalogMetric>();
  for (const m of catalog.metrics) {
    if (!m.metric_key || !m.metric_key.startsWith(wirePrefixDot)) continue;
    const bare = m.metric_key.slice(wirePrefixDot.length);
    if (!catalogByBareKey.has(bare)) catalogByBareKey.set(bare, m);
  }

  // Synthesize honest-zero rows for every metric_key the catalog knows about
  // for this section but the backend didn't return. Without this, an
  // unanswered metric makes the whole bullet disappear from the screen.
  const seenKeys = new Set(rows.map((r) => r.metric_key));
  const synthetic: RawBulletAggregateRow[] = [];
  for (const bareKey of catalogByBareKey.keys()) {
    if (!seenKeys.has(bareKey)) {
      synthetic.push({
        metric_key: bareKey,
        value: 0,
        median: null,
        range_min: null,
        range_max: null,
      });
    }
  }
  const allRows = [...rows, ...synthetic];

  const out: BulletMetric[] = [];
  for (const r of allRows) {
    const catalogRow = catalogByBareKey.get(r.metric_key);
    // Missing-id (catalog row absent for this section) → silently omit per
    // DESIGN §3.3 Catalog Consumer Contract.
    if (!catalogRow) continue;

    const label = catalogRow.label;
    const sublabel = catalogRow.sublabel ?? '';
    const baseUnit = catalogRow.unit ?? '';
    const isMemberScale = catalogRow.is_member_scale;
    const higherIsBetter = catalogRow.higher_is_better;
    const goodThr = catalogRow.thresholds.good;
    const warnThr = catalogRow.thresholds.warn;
    const isSchemaError = catalogRow.schema_status === 'error';

    // Member-scale metrics use team headcount as the denominator. Unit
    // becomes "/ N" at the team view; IC view keeps them unavailable.
    const effectiveUnit = isMemberScale
      ? teamSize != null
        ? `/ ${teamSize}`
        : ''
      : baseUnit;

    const valueUnavailable =
      r.value === null || r.value === undefined || !Number.isFinite(r.value);
    const rangeAvailable = r.range_min != null && r.range_max != null;
    const rangeMax =
      isMemberScale && teamSize != null ? teamSize : r.range_max;
    const rangeMin = isMemberScale && teamSize != null ? 0 : r.range_min;
    const memberScaleMissingSize = isMemberScale && teamSize == null;

    if (
      valueUnavailable ||
      !rangeAvailable ||
      rangeMin == null ||
      rangeMax == null ||
      memberScaleMissingSize
    ) {
      out.push({
        period,
        section,
        metric_key: r.metric_key,
        label,
        sublabel,
        value: formatBulletValue(r.value, effectiveUnit),
        unit: effectiveUnit,
        range_min: '—',
        range_max: '—',
        median: '—',
        median_label: '',
        bar_left_pct: 0,
        bar_width_pct: 0,
        median_left_pct: 0,
        status: 'unavailable',
        drill_id: '',
        ...(isSchemaError ? { schema_error: true } : {}),
      });
      continue;
    }

    // Auto-scale hour-bullets to days when the upper range crosses a few
    // days (industry default: 48h). Keeps display readable without changing
    // the underlying metric semantics or thresholds.
    const scaled = scaleHoursToDays(
      effectiveUnit,
      r.value,
      r.median,
      rangeMin,
      rangeMax,
    );
    const dispUnit = scaled?.unit ?? effectiveUnit;
    const dispVal = scaled?.value ?? r.value;
    const dispMin = scaled?.rangeMin ?? rangeMin;
    const dispMax = scaled?.rangeMax ?? rangeMax;
    const median = scaled?.median ?? r.median;
    const medianFormatted =
      median != null ? formatRangeStr(median, dispUnit) : '—';

    out.push({
      period,
      section,
      metric_key: r.metric_key,
      label,
      sublabel,
      value: formatBulletValue(dispVal, dispUnit),
      unit: dispUnit,
      range_min: formatRangeStr(dispMin, dispUnit),
      range_max: formatRangeStr(dispMax, dispUnit),
      median: median != null ? formatBulletValue(median, dispUnit) : '—',
      median_label: median != null ? `Median: ${medianFormatted}` : '',
      bar_left_pct: 0,
      bar_width_pct: pctInRange(dispVal, dispMin, dispMax),
      median_left_pct:
        median != null ? pctInRange(median, dispMin, dispMax) : 0,
      // schema_status='error' suppresses threshold-based coloring per DESIGN
      // §3.3: bar dimensions render normally but the row is flagged so
      // consumers can show the "Metric source unavailable" indicator.
      status: isSchemaError
        ? 'unavailable'
        : evaluateStatus(
            r.value,
            { good: goodThr, warn: warnThr },
            higherIsBetter,
          ),
      drill_id: '',
      ...(isSchemaError ? { schema_error: true } : {}),
    });
  }
  return out;
}


export function transformLocTrend(
  rows: RawLocTrendRow[],
  period: PeriodValue,
): LocDataPoint[] {
  return rows.map((r) => ({
    label: formatDateLabel(r.date_bucket, period),
    aiLoc: r.ai_loc,
    codeLoc: r.code_loc,
    specLines: r.spec_lines,
  }));
}

export function transformDeliveryTrend(
  rows: RawDeliveryTrendRow[],
  period: PeriodValue,
): DeliveryDataPoint[] {
  return rows.map((r) => ({
    label: formatDateLabel(r.date_bucket, period),
    commits: r.commits,
    prsMerged: r.prs_merged,
    tasksDone: r.tasks_done,
  }));
}

export function transformTeamMembers(
  rows: RawTeamMemberRow[],
  period: PeriodValue,
): TeamMember[] {
  // tasks_closed / bugs_fixed are non-nullable from the seed (sum over
  // jira_closed_tasks with ifNull(x, 0)), so rounding is fine. prs_merged is
  // nullable — preserve null so MembersTable renders em-dash instead of "0".
  const roundOrNull = (v: number | null | undefined): number | null =>
    v == null || !Number.isFinite(v) ? null : Math.round(v);

  return rows.map((r) => ({
    person_id: r.person_id,
    period,
    name: r.display_name,
    seniority: r.seniority,
    supervisor_email: r.supervisor_email,
    tasks_closed: Math.round(r.tasks_closed),
    bugs_fixed: Math.round(r.bugs_fixed),
    dev_time_h: r.dev_time_h,
    prs_merged: roundOrNull(r.prs_merged),
    build_success_pct: r.build_success_pct,
    focus_time_pct: r.focus_time_pct,
    ai_tools: r.ai_tools,
    ai_loc_share_pct: r.ai_loc_share_pct,
  }));
}

export function transformTimeOff(row: RawTimeOffRow): TimeOffNotice {
  return {
    days: row.days,
    dateRange: row.date_range,
    bambooHrUrl: row.bamboo_hr_url,
  };
}

export function transformDrill(row: RawDrillRow): DrillData {
  return {
    title: row.title,
    source: row.source,
    srcClass: row.src_class,
    value: row.value,
    filter: row.filter,
    columns: row.columns,
    rows: row.rows,
  };
}

// ---------------------------------------------------------------------------
// CRM (sales-rep dashboard)
// ---------------------------------------------------------------------------

function numCoerce(v: number | string | null | undefined): number {
  if (v === null || v === undefined) return 0;
  if (typeof v === 'number') return v;
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

export function transformCrmKpis(row: RawCrmKpisRow | null): CrmKpis | null {
  if (row === null) return null;
  return {
    dealsOpened: numCoerce(row.deals_opened),
    dealsClosed: numCoerce(row.deals_closed),
    dealsWon: numCoerce(row.deals_won),
    dealsValueClosed: numCoerce(row.deals_value_closed),
    commsCount: numCoerce(row.comms_count),
    pipelineCount: numCoerce(row.pipeline_count),
    pipelineValue: numCoerce(row.pipeline_value),
  };
}

export function transformCrmFlow(rows: RawCrmFlowRow[]): CrmFlowPoint[] {
  return [...rows]
    .sort((a, b) => a.metric_date.localeCompare(b.metric_date))
    .map((r) => ({
      label: r.date_bucket,
      opened: numCoerce(r.opened),
      closed: numCoerce(r.closed),
      won: numCoerce(r.won),
    }));
}

export interface CrmBulletDef {
  metric_key: string;
  label: string;
  sublabel: string;
  unit: string;
  higher_is_better: boolean;
}

export const CRM_QUALITY_BULLETS: CrmBulletDef[] = [
  { metric_key: 'win_rate',      label: 'Win Rate',       sublabel: 'Won ÷ closed in period',                       unit: '%', higher_is_better: true  },
  { metric_key: 'avg_deal_size', label: 'Avg Deal Size',  sublabel: 'Won deals · mean of properties_amount',        unit: '$', higher_is_better: true  },
  { metric_key: 'cycle_days',    label: 'Avg Cycle Time', sublabel: 'Created → won · mean days · lower = better',   unit: 'd', higher_is_better: false },
  { metric_key: 'deals_opened',  label: 'Deals Opened',   sublabel: 'Volume · deals created in period',             unit: '',  higher_is_better: true  },
];

export const CRM_ACTIVITY_BULLETS: CrmBulletDef[] = [
  { metric_key: 'calls',         label: 'Calls',            sublabel: 'HubSpot · engagements_calls in period',                  unit: '',  higher_is_better: true  },
  { metric_key: 'emails',        label: 'Emails',           sublabel: 'HubSpot · engagements_emails in period',                 unit: '',  higher_is_better: true  },
  { metric_key: 'meetings',      label: 'Meetings',         sublabel: 'HubSpot · engagements_meetings in period',               unit: '',  higher_is_better: true  },
  { metric_key: 'comms_per_won', label: 'Comms / Won Deal', sublabel: 'Total comms ÷ deals won · efficiency · lower = better', unit: '',  higher_is_better: false },
];

function fmtCrm(v: number | null, unit: string): string {
  if (v === null || !Number.isFinite(v)) return '—';
  if (unit === '%') return `${v.toFixed(1)}%`;
  if (unit === 'd') return `${v.toFixed(1)}d`;
  if (unit === '$') {
    const a = Math.abs(v);
    if (a >= 1_000_000) return `$${(v / 1_000_000).toFixed(2)}M`;
    if (a >= 1_000) return `$${(v / 1_000).toFixed(0)}k`;
    return `$${v.toFixed(0)}`;
  }
  return new Intl.NumberFormat('en-US').format(Math.round(v));
}

export function transformCrmBullets(
  rows: RawBulletAggregateRow[],
  period: PeriodValue,
  section: string,
  defs: CrmBulletDef[],
): BulletMetric[] {
  const byKey = keyBy(rows, 'metric_key');
  return defs.map((def) => {
    const r = byKey[def.metric_key] as RawBulletAggregateRow | undefined;
    const value = r?.value ?? null;
    const median = r?.median ?? null;
    const rMin = r?.range_min ?? null;
    const rMax = r?.range_max ?? null;

    const distAvailable =
      value !== null && rMin !== null && rMax !== null && rMin !== rMax;
    if (!distAvailable) {
      return {
        period,
        section,
        metric_key: def.metric_key,
        label: def.label,
        sublabel: def.sublabel,
        value: fmtCrm(value, def.unit),
        unit: def.unit,
        range_min: '—',
        range_max: '—',
        median: '—',
        median_label: '',
        bar_left_pct: 0,
        bar_width_pct: 0,
        median_left_pct: 0,
        status: 'unavailable',
        drill_id: '',
      };
    }

    const safeValue = value as number;
    const safeMedian = median as number | null;
    const safeMin = rMin as number;
    const safeMax = rMax as number;
    const barWidthPct = pctInRange(safeValue, safeMin, safeMax);
    const medianLeftPct =
      safeMedian !== null ? pctInRange(safeMedian, safeMin, safeMax) : 0;

    let status: 'good' | 'warn' | 'bad' = 'warn';
    if (safeMedian !== null) {
      const diff = safeValue - safeMedian;
      const tolerance = Math.abs(safeMedian) * 0.1;
      if (Math.abs(diff) <= tolerance) status = 'warn';
      else if ((diff > 0) === def.higher_is_better) status = 'good';
      else status = 'bad';
    }

    return {
      period,
      section,
      metric_key: def.metric_key,
      label: def.label,
      sublabel: def.sublabel,
      value: fmtCrm(safeValue, def.unit),
      unit: def.unit,
      range_min: fmtCrm(safeMin, def.unit),
      range_max: fmtCrm(safeMax, def.unit),
      median: fmtCrm(safeMedian, def.unit),
      median_label:
        safeMedian !== null ? `Median: ${fmtCrm(safeMedian, def.unit)}` : '',
      bar_left_pct: 0,
      bar_width_pct: barWidthPct,
      median_left_pct: medianLeftPct,
      status,
      drill_id: '',
    };
  });
}

/**
 * Transform layer
 *
 * Pure functions that map raw ClickHouse aggregates (snake_case) into the
 * pre-formatted UI types consumed by components.
 *
 * Design rules:
 * - Explicit field mapping only (no generic snake->camel converter).
 * - Every function is stateless and side-effect free.
 * - Threshold defaults come from thresholdConfig.ts; backend-provided
 *   values override them when present.
 */

import { keyBy } from 'lodash';
import type {
  PeriodValue,
  ExecTeamRow,
  ExecViewConfig,
  IcKpi,
  BulletMetric,
  LocDataPoint,
  DeliveryDataPoint,
  TeamMember,
  TimeOffNotice,
  DrillData,
} from '../types';
import type {
  RawExecSummaryRow,
  RawIcAggregateRow,
  RawBulletAggregateRow,
  RawLocTrendRow,
  RawDeliveryTrendRow,
  RawTeamMemberRow,
  RawTimeOffRow,
  RawDrillRow,
} from './rawTypes';
import { BULLET_DEFS, IC_KPI_DEFS } from './thresholdConfig';
import type { IcKpiDef } from './thresholdConfig';
import { evaluateStatus } from './metricSemantics';

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

function clamp(v: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, v));
}

function pctInRange(value: number, rangeMin: number, rangeMax: number): number {
  const span = rangeMax - rangeMin;
  if (span <= 0) return 0;
  return Math.round(clamp(((value - rangeMin) / span) * 100, 0, 100));
}

function formatValue(raw: number, fmt: IcKpiDef['format']): string {
  switch (fmt) {
    case 'integer': return String(Math.round(raw));
    case 'decimal1': return raw.toFixed(1);
    case 'percent': return String(Math.round(raw));
    case 'hours': return `${raw.toFixed(1)}h`;
  }
}

function formatDelta(
  delta: number,
  def: IcKpiDef,
): string {
  const sign = delta > 0 ? '+' : '';
  switch (def.format) {
    case 'integer': return `${sign}${Math.round(delta)}`;
    case 'decimal1': return `${sign}${delta.toFixed(1)}`;
    case 'percent': return `${sign}${Math.round(delta)}%`;
    case 'hours': return `${sign}${delta.toFixed(1)}h`;
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
  if (unit === '%') return `${value}%`;
  if (unit === 'h' || unit === 'h/mo') return `${value}h`;
  if (unit === 'd') return `${value}d`;
  if (unit === '\u00d7') return `${value}\u00d7`;
  return String(value);
}

/**
 * Units where the displayed value should be an integer (counts, members, messages).
 * Fractional-by-nature units (%, hours, ratios, "avg" of small values) are excluded.
 */
function isCountUnit(unit: string): boolean {
  const u = unit.toLowerCase().trim();
  return (
    u === '' ||
    u === 'tasks' ||
    u === 'count' ||
    u === 'lines' ||
    u === 'replies' ||
    u === 'days' ||
    u === '/mo' ||
    u === '/day' ||
    u.startsWith('/ ')
  );
}

function formatBulletValue(raw: number | null | undefined, unit: string): string {
  if (raw === null || raw === undefined || !Number.isFinite(raw)) return '—';
  if (isCountUnit(unit)) return String(Math.round(raw));
  return String(Math.round(raw * 100) / 100); // up to 2 decimals for %, ratios, hours
}

// ---------------------------------------------------------------------------
// 1. Executive summary
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// 2. IC KPIs
// ---------------------------------------------------------------------------

export function transformIcKpis(
  current: RawIcAggregateRow | null,
  previous: RawIcAggregateRow | null,
  period: PeriodValue,
): IcKpi[] {
  // Data-driven: no current row means the backend has nothing for this person
  // in the selected period — return empty so composites render ComingSoon
  // uniformly instead of forcing zeros onto the UI.
  if (current === null) return [];

  return IC_KPI_DEFS.map((def) => {
    // Distinguish "raw value missing" (NULL from backend → source not ingested)
    // from "raw value is zero" (real measurement). Missing → value:null so the
    // KpiStrip cell can render ComingSoon. Zero → formatted '0' like any number.
    const rawCur = current[def.raw_field];
    const rawPrev = previous?.[def.raw_field];
    const curVal = rawCur == null ? null : (rawCur as number);
    const prevVal = rawPrev == null ? null : (rawPrev as number);

    const value = curVal === null ? null : formatValue(curVal, def.format);
    let delta = '';
    let dt: 'good' | 'warn' | 'bad' | 'neutral' = 'neutral';

    if (curVal !== null && prevVal !== null) {
      const diff = curVal - prevVal;
      delta = formatDelta(diff, def);
      dt = deltaType(diff, def.higher_is_better);
    }

    return {
      period,
      metric_key: def.metric_key,
      label: def.label,
      value,
      unit: def.unit,
      sublabel: def.sublabel,
      description: def.description,
      delta,
      delta_type: dt,
    };
  });
}

// ---------------------------------------------------------------------------
// 3. Bullet metrics
// ---------------------------------------------------------------------------

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
  teamSize?: number,
  viewKind: 'ic' | 'team' = 'ic',
): BulletMetric[] {
  const defsByKey = keyBy(
    BULLET_DEFS.filter((d) => d.section === section),
    'metric_key',
  );

  const pickSublabel = (d: { sublabel: string; teamSublabel?: string }) =>
    viewKind === 'team' && d.teamSublabel ? d.teamSublabel : d.sublabel;

  return rows.map((r) => {
      const def = defsByKey[r.metric_key];

      if (!def) {
        // Unknown metric_key — backend surfaced something the FE doesn't know.
        // Render what we can and mark unavailable so the bar doesn't pretend
        // to reflect a distribution we can't describe.
        const unitStr = '';
        return {
          period,
          section,
          metric_key: r.metric_key,
          label: r.metric_key,
          sublabel: '',
          value: formatBulletValue(r.value, unitStr),
          unit: unitStr,
          range_min: '\u2014',
          range_max: '\u2014',
          median: '\u2014',
          median_label: '',
          bar_left_pct: 0,
          bar_width_pct: 0,
          median_left_pct: 0,
          status: 'unavailable',
          drill_id: '',
        };
      }

      // Member-scale metrics use team headcount as the denominator. Unit
      // becomes "/ N" at the team view; IC view keeps them unavailable.
      const effectiveUnit = def.isMemberScale
        ? teamSize != null
          ? `/ ${teamSize}`
          : ''
        : def.unit;

      const valueUnavailable = r.value === null || r.value === undefined || !Number.isFinite(r.value);
      const rangeAvailable = r.range_min != null && r.range_max != null;
      // For member-scale metrics, override range_max with team size when
      // known (the backend emits min/max across team members, but the chart
      // scale should run 0..teamSize so "out of N" reads correctly).
      const rangeMax = def.isMemberScale && teamSize != null
        ? teamSize
        : r.range_max;
      const rangeMin = def.isMemberScale && teamSize != null
        ? 0
        : r.range_min;
      // Member-scale metrics rendered without a known team size have no
      // meaningful denominator — fall through to unavailable instead of
      // showing a bare "N" with an implicit scale (IC dashboard doesn't
      // know the viewer's team size today).
      const memberScaleMissingSize = def.isMemberScale && teamSize == null;

      // Distribution not provided by the backend → can't draw a meaningful
      // bullet. Show value + label; render ComingSoon in the bar slot.
      if (
        valueUnavailable ||
        !rangeAvailable ||
        rangeMin == null ||
        rangeMax == null ||
        memberScaleMissingSize
      ) {
        return {
          period,
          section,
          metric_key: r.metric_key,
          label: def.label,
          sublabel: pickSublabel(def),
          value: formatBulletValue(r.value, effectiveUnit),
          unit: effectiveUnit,
          range_min: '\u2014',
          range_max: '\u2014',
          median: '\u2014',
          median_label: '',
          bar_left_pct: 0,
          bar_width_pct: 0,
          median_left_pct: 0,
          status: 'unavailable',
          drill_id: def.drill_id,
        };
      }

      const median = r.median;
      // Use formatRangeStr for median too so units stay consistent with the
      // min/max labels rendered under the bar — previously the median-only
      // formatter handled `%`/`h` but missed `×`, `h/mo`, `d`, producing
      // labels like "0× … Median: 1.1 … 3×".
      const medianFormatted = median != null ? formatRangeStr(median, def.unit) : '\u2014';
      return {
        period,
        section,
        metric_key: r.metric_key,
        label: def.label,
        sublabel: pickSublabel(def),
        // Format counters as integers (round), ratios/percents/hours as 2-decimal.
        value: formatBulletValue(r.value, effectiveUnit),
        unit: effectiveUnit,
        range_min: formatRangeStr(rangeMin, def.unit),
        range_max: formatRangeStr(rangeMax, def.unit),
        median: median != null ? formatBulletValue(median, def.unit) : '\u2014',
        median_label: median != null ? `Median: ${medianFormatted}` : '',
        bar_left_pct: 0,
        bar_width_pct: pctInRange(r.value, rangeMin, rangeMax),
        median_left_pct: median != null ? pctInRange(median, rangeMin, rangeMax) : 0,
        status: evaluateStatus(r.value, def),
        drill_id: def.drill_id,
      };
    });
}

// ---------------------------------------------------------------------------
// 4. Chart transforms
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// 5. Team members
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// 6. Time-off notice
// ---------------------------------------------------------------------------

export function transformTimeOff(row: RawTimeOffRow): TimeOffNotice {
  return {
    days: row.days,
    dateRange: row.date_range,
    bambooHrUrl: row.bamboo_hr_url,
  };
}

// ---------------------------------------------------------------------------
// 7. Drill data
// ---------------------------------------------------------------------------

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

/**
 * Team View Actions
 *
 * Queries the Analytics API for team member rows and bullet section metrics,
 * then assembles TeamViewData from the responses.
 * Also fetches data_availability via ConnectorManagerService.
 *
 * Each section runs its own pipeline and emits its own
 * TeamViewSectionLoading / TeamViewSectionLoaded / TeamViewSectionFailed
 * events. A slow section never blocks the rendering of the others.
 *
 * Spec: analytics-views-api.md §4.2
 */

import { eventBus, apiRegistry } from '@hai3/react';
import { TeamViewEvents, type TeamViewSectionData } from '../events/teamViewEvents';
import { InsightApiService } from '../api/insightApiService';
import { ConnectorManagerService } from '../api/connectorManagerService';
import { METRIC_REGISTRY } from '../api/metricRegistry';
import { odataDateFilter, odataEscapeValue, type DateRange } from '../utils/periodToDateRange';
import { transformTeamMembers, transformBulletMetrics, transformDrill } from '../api/transforms';
import type {
  PeriodValue,
  TeamMember,
  DrillData,
  ODataResponse,
} from '../types';
import type { RawTeamMemberRow, RawBulletAggregateRow, RawDrillRow } from '../api/rawTypes';
import {
  TEAM_KPIS_BY_PERIOD,
  TEAM_VIEW_CONFIG,
} from '../api/viewConfigs';
import { teamHealthStatus } from '../api/metricSemantics';

// ---------------------------------------------------------------------------
// Team KPI derivation (§4.2 — frontend-computed from member rows)
// ---------------------------------------------------------------------------

function median(values: number[]): number | null {
  const nums = values.filter((v) => Number.isFinite(v));
  if (nums.length === 0) return null;
  const sorted = [...nums].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? (sorted[mid - 1]! + sorted[mid]!) / 2
    : sorted[mid]!;
}

export function deriveTeamKpis(members: TeamMember[], period: PeriodValue) {
  // Data-driven: no members means the backend has nothing for this team in
  // the selected period — return empty so TeamHeroStrip renders ComingSoon
  // uniformly instead of forcing zero counters onto the UI.
  if (members.length === 0) return [];

  const total      = members.length;
  const focusTrigger = TEAM_VIEW_CONFIG.alert_thresholds
    .find((t) => t.metric_key === 'focus_time_pct')?.trigger ?? 60;

  // Skip null metrics — missing connector data reads as `null`, and
  // `null < trigger` coerces to `true`, inflating atRisk with phantom members.
  const atRisk = members.filter((m) =>
    TEAM_VIEW_CONFIG.alert_thresholds.some((t) => {
      const v = m[t.metric_key as keyof TeamMember];
      return typeof v === 'number' && Number.isFinite(v) && v < t.trigger;
    }),
  ).length;
  // `focus_time_pct` is nullable when the person has no upstream focus row.
  // Skip nulls so they don't count against belowFocus (otherwise every member
  // without a focus source looks "below target").
  const membersWithFocus = members.filter((m): m is TeamMember & { focus_time_pct: number } => m.focus_time_pct !== null);
  const focusCount  = membersWithFocus.filter((m) => m.focus_time_pct >= focusTrigger).length;
  const belowFocus  = membersWithFocus.length - focusCount;
  const noAiCount   = members.filter((m) => m.ai_tools.length === 0).length;

  // Median dev_time_h across members — skip nulls (missing focus source).
  const devTimeMedian = median(
    members.map((m) => m.dev_time_h).filter((v): v is number => v !== null),
  );

  // Statuses scale with team size (TEAM_HEALTH_THRESHOLDS) — "2 problematic"
  // is a crisis in a 5-person team and a rounding error in a 100-person one.
  const atRiskStatus = teamHealthStatus(atRisk, total);
  const focusStatus  = teamHealthStatus(belowFocus, total);
  const noAiStatus   = teamHealthStatus(noAiCount, total);

  return (TEAM_KPIS_BY_PERIOD[period] ?? TEAM_KPIS_BY_PERIOD['month']).map((k) => {
    if (k.metric_key === 'at_risk_count') return { ...k, value: String(atRisk),  status: atRiskStatus };
    if (k.metric_key === 'focus_gte_60')  return { ...k, value: `${focusCount} / ${total}`, sublabel: `${belowFocus} member${belowFocus !== 1 ? 's' : ''} below target`, status: focusStatus };
    if (k.metric_key === 'not_using_ai')  return { ...k, value: String(noAiCount), status: noAiStatus };
    if (k.metric_key === 'team_dev_time') {
      const value = devTimeMedian === null ? '—' : `${devTimeMedian.toFixed(1)}h`;
      // chipLabel: undefined — TeamHeroStrip uses `kpi.chipLabel ?? kpi.status`
      // for the badge, so empty string would render an empty badge; undefined
      // correctly falls back to status.
      return { ...k, value, sublabel: `Team median · ${total} member${total !== 1 ? 's' : ''}`, chipLabel: undefined };
    }
    return k;
  });
}

// ---------------------------------------------------------------------------
// Action
// ---------------------------------------------------------------------------

/**
 * Roster entry passed in from the screen — derived from the IR subtree so the
 * team-view shows the same people the sidebar menu does. When `null`, the
 * action falls back to the legacy org_unit_id flow (executive viewing a
 * department-string `org_unit_name` for which no IR subtree is loaded).
 */
export interface TeamRosterEntry {
  email: string;
  display_name: string;
  supervisor_email: string | null;
}

const buildSyntheticMember = (entry: TeamRosterEntry, period: PeriodValue): TeamMember => ({
  person_id: entry.email,
  period,
  name: entry.display_name,
  seniority: '',
  supervisor_email: entry.supervisor_email,
  tasks_closed: 0,
  bugs_fixed: 0,
  dev_time_h: null,
  prs_merged: null,
  build_success_pct: null,
  focus_time_pct: null,
  ai_tools: [],
  ai_loc_share_pct: null,
});

// Per-section runner: emits Loading, then Loaded on success / Failed on
// rejection. Sections are independent — one slow query never blocks others.
function runSection(
  sectionId: string,
  pipeline: () => Promise<TeamViewSectionData>,
): void {
  eventBus.emit(TeamViewEvents.TeamViewSectionLoading, { sectionId });
  void pipeline()
    .then((data) => {
      eventBus.emit(TeamViewEvents.TeamViewSectionLoaded, { sectionId, data });
    })
    .catch((err: unknown) => {
      eventBus.emit(TeamViewEvents.TeamViewSectionFailed, {
        sectionId,
        error: err instanceof Error ? err.message : String(err),
      });
    });
}

export const loadTeamView = (
  teamId: string,
  period: PeriodValue,
  range: DateRange,
  roster: TeamRosterEntry[] | null,
  pivotDisplayName: string | null,
): void => {
  eventBus.emit(TeamViewEvents.TeamViewLoadStarted);

  const api        = apiRegistry.getService(InsightApiService);
  const connectors = apiRegistry.getService(ConnectorManagerService);

  // Bullet metrics still aggregate at org_unit level — known residual: when
  // pivot is an email (executive drilldown), these will return empty until
  // bullets are migrated to the IR-roster path.
  const dateFilter = odataDateFilter(range);
  const bulletScope = teamId.includes('@') ? teamId.toLowerCase() : teamId;
  const bulletFilter = `org_unit_id eq '${odataEscapeValue(bulletScope)}' and ${dateFilter}`;

  // ---- Section: team_summary (synchronous — name + static config) ---------
  // Rendered immediately so the screen header is never empty waiting for
  // members. teamKpis come from the members section once it lands.
  const teamName = pivotDisplayName ?? (teamId.charAt(0).toUpperCase() + teamId.slice(1));
  runSection('team_summary', () => Promise.resolve({
    kind: 'team_summary',
    teamName,
    teamKpis: [],
    config: TEAM_VIEW_CONFIG,
  }));

  // ---- Availability (best-effort) -----------------------------------------
  void connectors.getDataAvailability()
    .then((availability) => {
      eventBus.emit(TeamViewEvents.TeamViewAvailabilityLoaded, availability);
    })
    .catch(() => {
      // swallow — availability is informational
    });

  // ---- Section: members (per-person or bulk) ------------------------------
  // Roster mode: fetch metrics per person from the IR-derived list. Missing
  // analytics rows render as synthetic empties so headcount stays accurate.
  // Fallback (roster=null): legacy org_unit_id query — only path remaining is
  // an executive viewing a department-string org_unit.
  const memberQueries: Promise<ODataResponse<RawTeamMemberRow>>[] = roster
    ? roster.map((r) =>
        api.queryMetric<RawTeamMemberRow>(METRIC_REGISTRY.TEAM_MEMBER, {
          $filter: `person_id eq '${odataEscapeValue(r.email.toLowerCase())}' and ${dateFilter}`,
          $top:    1,
        }),
      )
    : [
        api.queryMetric<RawTeamMemberRow>(METRIC_REGISTRY.TEAM_MEMBER, {
          $filter:  bulletFilter,
          $orderby: 'display_name asc',
          $top:     200,
        }),
      ];

  runSection('members', () =>
    // allSettled inside the section: one missing person row shouldn't fail
    // the whole roster — synthetic entries cover the gaps. The section only
    // fails (and shows Retry) when *every* per-person query rejects, which
    // is unlikely outside a total backend outage.
    Promise.allSettled(memberQueries).then((settledResults) => {
      const fulfilled = settledResults.filter(
        (r): r is PromiseFulfilledResult<ODataResponse<RawTeamMemberRow>> => r.status === 'fulfilled',
      );
      if (settledResults.length > 0 && fulfilled.length === 0) {
        // Total failure — surface as section error so the table renders Retry.
        throw new Error('TEAM_MEMBER queries all rejected');
      }

      const rowByEmail = new Map<string, RawTeamMemberRow>();
      for (const resp of fulfilled) {
        for (const row of resp.value.items) {
          rowByEmail.set(row.person_id.toLowerCase(), row);
        }
      }

      let members: TeamMember[];
      if (roster) {
        // Preserve roster order (IR DFS) so the table matches the sidebar
        // tree the viewer just navigated through.
        members = roster.map((entry) => {
          const row = rowByEmail.get(entry.email.toLowerCase());
          if (row) return transformTeamMembers([row], period)[0]!;
          return buildSyntheticMember(entry, period);
        });
      } else {
        members = transformTeamMembers(Array.from(rowByEmail.values()), period);
        members.sort((a, b) => a.name.localeCompare(b.name));
      }

      return { kind: 'members', members };
    }),
  );

  // ---- Bullet sections (independent) --------------------------------------
  const bulletSection = (sectionId: string, registryKey: keyof typeof METRIC_REGISTRY): void => {
    runSection(sectionId, () =>
      api.queryMetric<RawBulletAggregateRow>(METRIC_REGISTRY[registryKey], { $filter: bulletFilter })
        // teamSize is unknown at this point (members section may not have
        // landed yet); the bullet transform handles undefined teamSize by
        // marking member-scale rows as `unavailable` rather than inventing a
        // denominator. AI bullets reconcile their denominator when the
        // members section lands (see slice.setTeamSize re-derivation).
        .then((resp) => ({
          kind: 'bullet',
          sectionId,
          metrics: transformBulletMetrics(resp.items, sectionId, period, undefined, 'team'),
        })),
    );
  };

  bulletSection('task_delivery', 'TEAM_BULLET_DELIVERY');
  bulletSection('code_quality',  'TEAM_BULLET_QUALITY');
  bulletSection('collaboration', 'TEAM_BULLET_COLLAB');
  bulletSection('ai_adoption',   'TEAM_BULLET_AI');
};

// ---------------------------------------------------------------------------
// Drill — redux-managed (team / member / cell)
// ---------------------------------------------------------------------------

type TeamDrillFilter =
  | { kind: 'team'; teamId: string; drillId: string }
  | { kind: 'cell'; personId: string; drillId: string };

/**
 * Open a drill modal for the team view. The filter shape determines which
 * OData filter is sent to the backend. Emits DrillOpened on success.
 */
export const openTeamDrill = (filter: TeamDrillFilter): void => {
  const $filter =
    filter.kind === 'team'
      ? `org_unit_id eq '${odataEscapeValue(filter.teamId)}' and drill_id eq '${odataEscapeValue(filter.drillId)}'`
      : `person_id eq '${odataEscapeValue(filter.personId.toLowerCase())}' and drill_id eq '${odataEscapeValue(filter.drillId)}'`;

  void apiRegistry
    .getService(InsightApiService)
    .queryMetric<RawDrillRow>(METRIC_REGISTRY.IC_DRILL, { $filter })
    .then((resp) => {
      const row = resp.items[0];
      if (!row) return;
      const drillData: DrillData = transformDrill(row);
      eventBus.emit(TeamViewEvents.DrillOpened, { drillId: filter.drillId, drillData });
    })
    .catch(() => {
      // Drill fetch failure: keep modal closed; data-driven UI will show
      // ComingSoon if opened without rows. Explicit error surfacing happens
      // in Phase 3 (identity / API error-state work).
    });
};

export const closeTeamDrill = (): void => {
  eventBus.emit(TeamViewEvents.DrillClosed);
};

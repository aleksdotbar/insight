/**
 * Team View Actions
 *
 * Queries the Analytics API for team member rows and bullet section metrics,
 * then assembles TeamViewData from the responses.
 * Also fetches data_availability via ConnectorManagerService.
 *
 * Spec: analytics-views-api.md §4.2
 */

import { eventBus, apiRegistry } from '@hai3/react';
import { TeamViewEvents } from '../events/teamViewEvents';
import { InsightApiService } from '../api/insightApiService';
import { ConnectorManagerService } from '../api/connectorManagerService';
import { METRIC_REGISTRY } from '../api/metricRegistry';
import { odataDateFilter, odataEscapeValue } from '../utils/periodToDateRange';
import { transformTeamMembers, transformBulletMetrics } from '../api/transforms';
import type {
  PeriodValue,
  TeamMember,
  TeamViewData,
} from '../types';
import type { RawTeamMemberRow, RawBulletAggregateRow } from '../api/rawTypes';
import {
  TEAM_KPIS_BY_PERIOD,
  TEAM_VIEW_CONFIG,
} from '../api/viewConfigs';
import { settled, emptyOdata } from '../utils/settledResult';

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
  const total      = members.length;
  const focusTrigger = TEAM_VIEW_CONFIG.alert_thresholds
    .find((t) => t.metric_key === 'focus_time_pct')?.trigger ?? 60;

  const atRisk = members.filter((m) =>
    TEAM_VIEW_CONFIG.alert_thresholds.some(
      (t) => (m[t.metric_key as keyof TeamMember] as number) < t.trigger,
    ),
  ).length;
  const focusCount  = members.filter((m) => m.focus_time_pct >= focusTrigger).length;
  const belowFocus  = total - focusCount;
  const noAiCount   = members.filter((m) => m.ai_tools.length === 0).length;

  // Median dev_time_h across members — honest replacement for the hardcoded "13h" chip.
  const devTimeMedian = median(members.map((m) => m.dev_time_h));

  const atRiskStatus:  'good' | 'warn' | 'bad' = atRisk === 0 ? 'good' : atRisk <= 2 ? 'warn' : 'bad';
  const focusStatus:   'good' | 'warn' | 'bad' = belowFocus === 0 ? 'good' : belowFocus <= 2 ? 'warn' : 'bad';
  const noAiStatus:    'good' | 'warn' | 'bad' = noAiCount === 0 ? 'good' : noAiCount <= 2 ? 'warn' : 'bad';

  return (TEAM_KPIS_BY_PERIOD[period] ?? TEAM_KPIS_BY_PERIOD['month']).map((k) => {
    if (k.metric_key === 'at_risk_count') return { ...k, value: String(atRisk),  status: atRiskStatus };
    if (k.metric_key === 'focus_gte_60')  return { ...k, value: `${focusCount} / ${total}`, sublabel: `${belowFocus} member${belowFocus !== 1 ? 's' : ''} below target`, status: focusStatus };
    if (k.metric_key === 'not_using_ai')  return { ...k, value: String(noAiCount), status: noAiStatus };
    if (k.metric_key === 'team_dev_time') {
      const value = devTimeMedian === null ? '—' : `${devTimeMedian.toFixed(1)}h`;
      return { ...k, value, sublabel: `Team median \u00b7 ${total} member${total !== 1 ? 's' : ''}`, chipLabel: '' };
    }
    return k;
  });
}

// ---------------------------------------------------------------------------
// Action
// ---------------------------------------------------------------------------

export const loadTeamView = (teamId: string, period: PeriodValue): void => {
  eventBus.emit(TeamViewEvents.TeamViewLoadStarted);

  const api        = apiRegistry.getService(InsightApiService);
  const connectors = apiRegistry.getService(ConnectorManagerService);

  const teamFilter = `org_unit_id eq '${odataEscapeValue(teamId)}' and ${odataDateFilter(period)}`;
  const teamBulletFilter = teamFilter;

  void Promise.allSettled([
    api.queryMetric<RawTeamMemberRow>(METRIC_REGISTRY.TEAM_MEMBER, {
      $filter:  teamFilter,
      $orderby: 'display_name asc',
      $top:     200,
    }),
    api.queryMetric<RawBulletAggregateRow>(METRIC_REGISTRY.TEAM_BULLET_DELIVERY, { $filter: teamBulletFilter }),
    api.queryMetric<RawBulletAggregateRow>(METRIC_REGISTRY.TEAM_BULLET_QUALITY,  { $filter: teamBulletFilter }),
    api.queryMetric<RawBulletAggregateRow>(METRIC_REGISTRY.TEAM_BULLET_COLLAB,   { $filter: teamBulletFilter }),
    api.queryMetric<RawBulletAggregateRow>(METRIC_REGISTRY.TEAM_BULLET_AI,       { $filter: teamBulletFilter }),
    connectors.getDataAvailability(),
  ])
    .then(([membersResult, deliveryResult, qualityResult, collabResult, aiResult, availResult]) => {
      const membersResp  = settled(membersResult,  emptyOdata<RawTeamMemberRow>(),        'TEAM_MEMBER');
      const deliveryResp = settled(deliveryResult,  emptyOdata<RawBulletAggregateRow>(),   'TEAM_BULLET_DELIVERY');
      const qualityResp  = settled(qualityResult,   emptyOdata<RawBulletAggregateRow>(),   'TEAM_BULLET_QUALITY');
      const collabResp   = settled(collabResult,    emptyOdata<RawBulletAggregateRow>(),   'TEAM_BULLET_COLLAB');
      const aiResp       = settled(aiResult,        emptyOdata<RawBulletAggregateRow>(),   'TEAM_BULLET_AI');

      const members = transformTeamMembers(membersResp.items, period);

      const bulletSections = [
        { id: 'task_delivery',  title: 'Task Delivery',  metrics: transformBulletMetrics(deliveryResp.items, 'task_delivery', period) },
        { id: 'code_quality',   title: 'Code & Quality', metrics: transformBulletMetrics(qualityResp.items,  'code_quality',  period) },
        { id: 'collaboration',  title: 'Collaboration',  metrics: transformBulletMetrics(collabResp.items,   'collaboration', period) },
        { id: 'ai_adoption',    title: 'AI Adoption',    metrics: transformBulletMetrics(aiResp.items,       'ai_adoption',   period) },
      ].filter((s) => s.metrics.length > 0);

      const teamName = teamId.charAt(0).toUpperCase() + teamId.slice(1);

      const data: TeamViewData = {
        teamName,
        teamKpis:      deriveTeamKpis(members, period),
        members,
        bulletSections,
        config:        TEAM_VIEW_CONFIG,
      };

      eventBus.emit(TeamViewEvents.TeamViewLoaded, data);

      const availability = settled(
        availResult,
        { git: 'no-connector', tasks: 'no-connector', ci: 'no-connector', comms: 'no-connector', hr: 'no-connector', ai: 'no-connector' } as const,
        'CONNECTORS',
      );
      eventBus.emit(TeamViewEvents.TeamViewAvailabilityLoaded, availability);
    })
    .catch((err: unknown) => {
      // Promise.allSettled never rejects, but guard against unexpected
      // errors in the transform/emit logic above.
      eventBus.emit(TeamViewEvents.TeamViewLoadFailed, String(err));
    });
};

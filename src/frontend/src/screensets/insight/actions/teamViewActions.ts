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
} from '../api/mocks/fixtures/team-view-base';

// ---------------------------------------------------------------------------
// Team KPI derivation (§4.2 — frontend-computed from member rows)
// ---------------------------------------------------------------------------

function deriveTeamKpis(members: TeamMember[], period: PeriodValue) {
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

  const atRiskStatus:  'good' | 'warn' | 'bad' = atRisk === 0 ? 'good' : atRisk <= 2 ? 'warn' : 'bad';
  const focusStatus:   'good' | 'warn' | 'bad' = belowFocus === 0 ? 'good' : belowFocus <= 2 ? 'warn' : 'bad';
  const noAiStatus:    'good' | 'warn' | 'bad' = noAiCount === 0 ? 'good' : noAiCount <= 2 ? 'warn' : 'bad';

  return (TEAM_KPIS_BY_PERIOD[period] ?? TEAM_KPIS_BY_PERIOD['month']).map((k) => {
    if (k.metric_key === 'at_risk_count') return { ...k, value: String(atRisk),  status: atRiskStatus };
    if (k.metric_key === 'focus_gte_60')  return { ...k, value: `${focusCount} / ${total}`, sublabel: `${belowFocus} member${belowFocus !== 1 ? 's' : ''} below target`, status: focusStatus };
    if (k.metric_key === 'not_using_ai')  return { ...k, value: String(noAiCount), status: noAiStatus };
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

  // Critical path — metric data only
  void Promise.all([
    api.queryMetric<RawTeamMemberRow>(METRIC_REGISTRY.TEAM_MEMBER, {
      $filter:  teamFilter,
      $orderby: 'display_name asc',
      $top:     200,
    }),
    api.queryMetric<RawBulletAggregateRow>(METRIC_REGISTRY.TEAM_BULLET_DELIVERY, { $filter: teamFilter }),
    api.queryMetric<RawBulletAggregateRow>(METRIC_REGISTRY.TEAM_BULLET_QUALITY,  { $filter: teamFilter }),
    api.queryMetric<RawBulletAggregateRow>(METRIC_REGISTRY.TEAM_BULLET_COLLAB,   { $filter: teamFilter }),
    api.queryMetric<RawBulletAggregateRow>(METRIC_REGISTRY.TEAM_BULLET_AI,       { $filter: teamFilter }),
  ])
    .then(([membersResp, delivery, quality, collab, ai]) => {
      const members = transformTeamMembers(membersResp.items, period);

      const bulletSections = [
        { id: 'task_delivery',  title: 'Task Delivery',  metrics: transformBulletMetrics(delivery.items, 'task_delivery', period) },
        { id: 'code_quality',   title: 'Code & Quality', metrics: transformBulletMetrics(quality.items,  'code_quality',  period) },
        { id: 'collaboration',  title: 'Collaboration',  metrics: transformBulletMetrics(collab.items,   'collaboration', period) },
        { id: 'ai_adoption',    title: 'AI Adoption',    metrics: transformBulletMetrics(ai.items,       'ai_adoption',   period) },
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
    })
    .catch((err: unknown) => {
      eventBus.emit(TeamViewEvents.TeamViewLoadFailed, String(err));
    });

  // Best-effort — availability does not block the view
  void connectors.getDataAvailability()
    .then((availability) => {
      eventBus.emit(TeamViewEvents.TeamViewAvailabilityLoaded, availability);
    })
    .catch(() => { /* availability is optional */ });
};

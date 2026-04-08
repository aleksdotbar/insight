/**
 * AttentionNeeded — computed alert list from team members metrics.
 * No state imports.
 */

import React from 'react';
import { Badge, Button, Card, CardContent } from '@hai3/uikit';
import type { TeamMember } from '../../../types';

export interface AttentionNeededProps {
  members: TeamMember[];
  onNavigate: (personId: string) => void;
}

type Severity = 'bad' | 'warn';

interface AlertItem {
  member: TeamMember;
  title: string;
  description: string;
  severity: Severity;
  declineBadge: string;
}

const SEVERITY_ICON: Record<Severity, string> = { bad: '🔴', warn: '🟡' };
const SEVERITY_ICON_BG: Record<Severity, string> = { bad: 'bg-red-100', warn: 'bg-amber-100' };
const SEVERITY_BADGE_CLASS: Record<Severity, string> = {
  bad: 'bg-red-100 text-red-600',
  warn: 'bg-amber-100 text-amber-600',
};

function computeAlerts(members: TeamMember[]): AlertItem[] {
  const alerts: AlertItem[] = [];
  for (const m of members) {
    if (m.focus_time_pct < 60 || m.dev_time_h > 20) {
      const severity: Severity = m.focus_time_pct < 48 || m.dev_time_h > 25 ? 'bad' : 'warn';
      const parts: string[] = [];
      if (m.focus_time_pct < 60) parts.push(`Focus Time ${m.focus_time_pct}%`);
      if (m.dev_time_h > 20) parts.push(`Dev Time ${m.dev_time_h}h`);
      alerts.push({
        member: m,
        title: `${m.name} — ${parts.join(', ')}`,
        description: m.dev_time_h > 20
          ? `Dev time is ${m.dev_time_h}h vs team median 14h. Only ${m.tasks_closed} tasks completed. Suggest 1:1 to understand blockers.`
          : `Focus Time below 60% target. ${m.tasks_closed} tasks completed this month.`,
        severity,
        declineBadge: severity === 'bad' ? '3 months declining' : '2 months declining',
      });
    } else if (m.ai_loc_share_pct === 0 && m.ai_tools.length === 0) {
      alerts.push({
        member: m,
        title: `${m.name} — Not using AI tools`,
        description: `No AI activity logged this month. ${m.tasks_closed} tasks completed.`,
        severity: 'warn',
        declineBadge: '2 months declining',
      });
    }
  }
  return alerts;
}

export const AttentionNeeded: React.FC<AttentionNeededProps> = ({ members, onNavigate }) => {
  const alerts = computeAlerts(members);
  if (alerts.length === 0) return null;

  return (
    <Card className="shadow-sm rounded-[10px]">
      <div className="px-4 pt-3.5 pb-0">
        <span className="text-[13px] font-bold text-gray-900">Attention Needed</span>
      </div>
      <CardContent className="px-4 py-3">
        {alerts.map((alert, i) => (
          <div
            key={`${alert.member.person_id}-${i}`}
            className={`flex items-start gap-3 py-3 ${i < alerts.length - 1 ? 'border-b border-gray-200' : ''}`}
          >
            <div className={`w-8 h-8 rounded-lg flex items-center justify-center text-base flex-shrink-0 ${SEVERITY_ICON_BG[alert.severity]}`}>
              {SEVERITY_ICON[alert.severity]}
            </div>
            <div className="flex-1 min-w-0">
              <div className="text-[12px] font-bold text-gray-900 mb-0.5">{alert.title}</div>
              <div className="text-[11px] text-gray-500">{alert.description}</div>
              <Button
                variant="ghost"
                size="sm"
                onClick={() => onNavigate(alert.member.person_id)}
                className="mt-1 h-auto p-0 text-[11px] text-blue-600 font-semibold"
              >
                → Open IC dashboard
              </Button>
            </div>
            <Badge className={`text-[10px] font-bold flex-shrink-0 ${SEVERITY_BADGE_CLASS[alert.severity]}`}>
              {alert.declineBadge}
            </Badge>
          </div>
        ))}
      </CardContent>
    </Card>
  );
};

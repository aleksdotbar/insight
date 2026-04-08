/**
 * TeamBulletSections — bullet metric sections for team view.
 * task_delivery + code_quality + estimation: always-visible cards with Legend.
 * ai_adoption + collaboration: collapsible with custom sub-group layouts.
 * No state imports.
 */

import React from 'react';
import { Card, CardContent } from '@hai3/uikit';
import CollapsibleSection from '../../../uikit/composite/CollapsibleSection';
import BulletChart from '../../../uikit/composite/BulletChart';
import type { BulletSection, BulletMetric, ViewMode } from '../../../types';

export interface TeamBulletSectionsProps {
  bulletSections: BulletSection[];
  viewMode: ViewMode;
  onDrillClick?: (drillId: string) => void;
}

// Company-median legend used in all team sections
const Legend: React.FC = () => (
  <div className="flex items-center gap-3 text-[9px] text-gray-400 mb-2.5">
    <span className="flex items-center gap-1">
      <span className="w-0.5 h-[11px] bg-blue-600/50 rounded inline-block" />
      Company median
    </span>
    <span className="flex items-center gap-1">
      <span className="w-4 h-[5px] rounded bg-blue-600 inline-block" />
      Team
    </span>
  </div>
);

// Standard 2-column bullet card (Task Delivery, Code Quality)
const TwoColCard: React.FC<{ title: string; subtitle: string; metrics: BulletMetric[]; onDrillClick?: (id: string) => void }> = ({
  title, subtitle, metrics, onDrillClick
}) => {
  const left = metrics.filter((_, i) => i % 2 === 0);
  const right = metrics.filter((_, i) => i % 2 !== 0);
  return (
    <Card className="shadow-sm rounded-[10px]">
      <CardContent className="px-4 py-3.5">
        <div className="flex items-center justify-between mb-1">
          <span className="text-[13px] font-bold text-gray-900">{title}</span>
          <span className="text-[10px] text-gray-400">{subtitle}</span>
        </div>
        <Legend />
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-3.5">
          <div className="flex flex-col gap-4">{left.map((m) => <BulletChart key={m.metric_key} metric={m} onDrillClick={onDrillClick} mode="chart" />)}</div>
          <div className="flex flex-col gap-4">{right.map((m) => <BulletChart key={m.metric_key} metric={m} onDrillClick={onDrillClick} mode="chart" />)}</div>
        </div>
      </CardContent>
    </Card>
  );
};

// Estimation card — 3 sub-groups
const ESTIMATION_GROUPS = [
  { label: '1 · Time estimate accuracy', keys: ['estimation_accuracy', 'overrun_ratio'] },
  { label: '2 · Sprint scope',           keys: ['scope_completion', 'scope_creep'] },
  { label: '3 · Deadline (date-driven)', keys: ['on_time_delivery', 'avg_slip'] },
];

const EstimationCard: React.FC<{ metrics: BulletMetric[]; onDrillClick?: (id: string) => void }> = ({ metrics, onDrillClick }) => (
  <Card className="shadow-sm rounded-[10px]">
    <CardContent className="px-4 py-3.5">
      <div className="flex items-center justify-between mb-1">
        <span className="text-[13px] font-bold text-gray-900">Estimation</span>
        <span className="text-[10px] text-gray-400">Team median vs company median · Source: Jira</span>
      </div>
      <Legend />
      <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-3.5">
        {ESTIMATION_GROUPS.map(({ label, keys }) => {
          const groupMetrics = metrics.filter((m) => keys.includes(m.metric_key));
          return (
            <div key={label}>
              <div className="text-[10px] font-semibold text-gray-400 mb-1.5">{label}</div>
              <div className="flex flex-col gap-4">
                {groupMetrics.map((m) => <BulletChart key={m.metric_key} metric={m} onDrillClick={onDrillClick} mode="chart" />)}
              </div>
            </div>
          );
        })}
      </div>
    </CardContent>
  </Card>
);

// AI Adoption — collapsible, 2-column (left: member counts, right: rates)
const AI_ADOPTION_LEFT_KEYS = ['active_ai_members', 'cursor_active', 'cc_active', 'codex_active'];
const AI_ADOPTION_RIGHT_KEYS = ['team_ai_loc', 'cursor_acceptance', 'cc_tool_acceptance'];

const AiAdoptionSection: React.FC<{ metrics: BulletMetric[]; onDrillClick?: (id: string) => void }> = ({ metrics, onDrillClick }) => (
  <CollapsibleSection title="AI Adoption" defaultOpen={false}>
    <div className="px-4 py-3">
      <div className="text-[11px] font-bold text-gray-500 uppercase tracking-wide mb-2.5">Cursor · Claude Code · Codex</div>
      <Legend />
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3.5">
        <div className="flex flex-col gap-4">
          {metrics.filter((m) => AI_ADOPTION_LEFT_KEYS.includes(m.metric_key)).map((m) => (
            <BulletChart key={m.metric_key} metric={m} onDrillClick={onDrillClick} mode="chart" />
          ))}
        </div>
        <div className="flex flex-col gap-4">
          {metrics.filter((m) => AI_ADOPTION_RIGHT_KEYS.includes(m.metric_key)).map((m) => (
            <BulletChart key={m.metric_key} metric={m} onDrillClick={onDrillClick} mode="chart" />
          ))}
        </div>
      </div>
    </div>
  </CollapsibleSection>
);

// Collaboration — collapsible, 3 columns with sub-headings
const COLLAB_COLUMNS = [
  { title: 'Slack',                  keys: ['slack_thread_participation', 'slack_message_engagement', 'slack_dm_ratio'] },
  { title: 'M365',                   keys: ['m365_teams_messages', 'm365_emails_sent', 'm365_files_shared'] },
  { title: 'Meetings · M365 · Zoom', keys: ['meeting_hours', 'zoom_calls', 'meeting_free'] },
];

const CollaborationSection: React.FC<{ metrics: BulletMetric[]; onDrillClick?: (id: string) => void }> = ({ metrics, onDrillClick }) => (
  <CollapsibleSection title="Collaboration" defaultOpen={false}>
    <div className="px-4 py-3">
      <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-3.5">
        {COLLAB_COLUMNS.map(({ title, keys }) => (
          <div key={title}>
            <div className="text-[11px] font-bold text-gray-500 uppercase tracking-wide mb-2.5">{title}</div>
            <Legend />
            <div className="flex flex-col gap-4">
              {metrics.filter((m) => keys.includes(m.metric_key)).map((m) => (
                <BulletChart key={m.metric_key} metric={m} onDrillClick={onDrillClick} mode="chart" />
              ))}
            </div>
          </div>
        ))}
      </div>
    </div>
  </CollapsibleSection>
);

export const TeamBulletSections: React.FC<TeamBulletSectionsProps> = ({ bulletSections, onDrillClick }) => {
  const byId = Object.fromEntries(bulletSections.map((s) => [s.id, s]));

  const taskDelivery = byId['task_delivery'];
  const codeQuality  = byId['code_quality'];
  const estimation   = byId['estimation'];
  const aiAdoption   = byId['ai_adoption'];
  const collab       = byId['collaboration'];

  return (
    <div className="flex flex-col gap-3.5">
      {/* Task Delivery + Code & Quality — side by side */}
      {(taskDelivery || codeQuality) && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-3.5">
          {taskDelivery && (
            <TwoColCard
              title="Task Delivery"
              subtitle="Team median vs company median"
              metrics={taskDelivery.metrics}
              onDrillClick={onDrillClick}
            />
          )}
          {codeQuality && (
            <TwoColCard
              title="Code & Quality"
              subtitle="Team median vs company median"
              metrics={codeQuality.metrics}
              onDrillClick={onDrillClick}
            />
          )}
        </div>
      )}

      {/* Estimation */}
      {estimation && <EstimationCard metrics={estimation.metrics} onDrillClick={onDrillClick} />}

      {/* AI Adoption — collapsible */}
      {aiAdoption && <AiAdoptionSection metrics={aiAdoption.metrics} onDrillClick={onDrillClick} />}

      {/* Collaboration — collapsible */}
      {collab && <CollaborationSection metrics={collab.metrics} onDrillClick={onDrillClick} />}
    </div>
  );
};

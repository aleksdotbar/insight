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
import { filterBulletsByLayoutGroup, BULLET_LAYOUT_GROUPS } from '../../../api/thresholdConfig';

export interface TeamBulletSectionsProps {
  bulletSections: BulletSection[];
  viewMode: ViewMode;
  onDrillClick?: (drillId: string) => void;
}

// Company-median legend used in all team sections
const Legend: React.FC = () => (
  <div className="flex items-center gap-3 text-2xs text-gray-400 mb-2.5">
    <span className="flex items-center gap-1">
      <span className="w-[2px] h-3 bg-gray-800/60 rounded inline-block" />
      Company median
    </span>
    <span className="flex items-center gap-1">
      <span className="w-4 h-1.5 rounded bg-blue-600 inline-block" />
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
    <Card className="shadow-sm rounded-xl">
      <CardContent className="px-4 py-3.5">
        <div className="flex items-center justify-between mb-1">
          <span className="text-sm font-bold text-gray-900">{title}</span>
          <span className="text-xs text-gray-400">{subtitle}</span>
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

// Estimation card — 3 sub-groups driven by BULLET_LAYOUT_GROUPS so adding a
// new estimation metric automatically slots into the right column.
const ESTIMATION_GROUPS = [
  { label: '1 · Time estimate accuracy', group: 'estimate_accuracy' },
  { label: '2 · Sprint scope',           group: 'sprint_scope' },
  { label: '3 · Deadline (date-driven)', group: 'deadline' },
];

const EstimationCard: React.FC<{ metrics: BulletMetric[]; onDrillClick?: (id: string) => void }> = ({ metrics, onDrillClick }) => (
  <Card className="shadow-sm rounded-xl">
    <CardContent className="px-4 py-3.5">
      <div className="flex items-center justify-between mb-1">
        <span className="text-sm font-bold text-gray-900">Estimation</span>
        <span className="text-xs text-gray-400">Team median vs company median · Source: Jira</span>
      </div>
      <Legend />
      <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-3.5">
        {ESTIMATION_GROUPS.map(({ label, group }) => {
          const groupMetrics = filterBulletsByLayoutGroup(metrics, group);
          return (
            <div key={label}>
              <div className="text-xs font-semibold text-gray-400 mb-1.5">{label}</div>
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

// AI Adoption — collapsible, 2-column (left: member counts, right: team output + acceptance rates)
const AI_RIGHT_GROUPS = ['ai_team_output', 'ai_acceptance'];

const AiAdoptionSection: React.FC<{ metrics: BulletMetric[]; onDrillClick?: (id: string) => void }> = ({ metrics, onDrillClick }) => {
  const leftMetrics = filterBulletsByLayoutGroup(metrics, 'ai_members');
  const rightMetrics = metrics.filter((m) => {
    const grp = BULLET_LAYOUT_GROUPS[m.metric_key];
    return !!grp && AI_RIGHT_GROUPS.includes(grp);
  });
  return (
    <CollapsibleSection title="AI Adoption" defaultOpen={false}>
      <div className="px-4 py-3">
        <div className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-2.5">Cursor · Claude Code · Codex</div>
        <Legend />
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-3.5">
          <div className="flex flex-col gap-4">
            {leftMetrics.map((m) => (
              <BulletChart key={m.metric_key} metric={m} onDrillClick={onDrillClick} mode="chart" />
            ))}
          </div>
          <div className="flex flex-col gap-4">
            {rightMetrics.map((m) => (
              <BulletChart key={m.metric_key} metric={m} onDrillClick={onDrillClick} mode="chart" />
            ))}
          </div>
        </div>
      </div>
    </CollapsibleSection>
  );
};

// Collaboration — collapsible, 3 columns driven by BULLET_LAYOUT_GROUPS.
const COLLAB_COLUMNS = [
  { title: 'Slack',                  group: 'slack' },
  { title: 'M365',                   group: 'm365' },
  { title: 'Meetings · M365 · Zoom', group: 'meetings' },
];

const CollaborationSection: React.FC<{ metrics: BulletMetric[]; onDrillClick?: (id: string) => void }> = ({ metrics, onDrillClick }) => (
  <CollapsibleSection title="Collaboration" defaultOpen={false}>
    <div className="px-4 py-3">
      <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-3.5">
        {COLLAB_COLUMNS.map(({ title, group }) => (
          <div key={title}>
            <div className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-2.5">{title}</div>
            <Legend />
            <div className="flex flex-col gap-4">
              {filterBulletsByLayoutGroup(metrics, group).map((m) => (
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

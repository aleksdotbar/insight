/**
 * CollaborationSection — Slack, M365, and meeting collaboration metrics.
 * Supports chart mode (3-column grouped) and tile mode (flat grid).
 * No state imports.
 */

import React from 'react';
import type { BulletMetric, ViewMode } from '../../../types';
import BulletChart from '../../../uikit/composite/BulletChart';
import ComingSoon from '../../../uikit/composite/ComingSoon';
import { filterBulletsByLayoutGroup } from '../../../api/thresholdConfig';

export interface CollaborationSectionProps {
  metrics: BulletMetric[];
  viewMode: ViewMode;
  onDrillClick: (drillId: string) => void;
  personName?: string;
}

const ColumnHeading: React.FC<{ children: React.ReactNode }> = ({ children }) => (
  <div className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-2.5">
    {children}
  </div>
);

const CollaborationSection: React.FC<CollaborationSectionProps> = ({
  metrics,
  viewMode,
  onDrillClick,
  personName,
}) => {
  if (metrics.length === 0) {
    return (
      <div className="p-4">
        <ComingSoon variant="card" />
      </div>
    );
  }

  if (viewMode === 'tile') {
    return (
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 p-4">
        {metrics.map((metric) => (
          <BulletChart
            key={metric.metric_key}
            metric={metric}
            onDrillClick={onDrillClick}
            mode="tile"
            personName={personName}
          />
        ))}
      </div>
    );
  }

  // Chart mode — 3 columns driven by BULLET_LAYOUT_GROUPS.
  const slackMetrics   = filterBulletsByLayoutGroup(metrics, 'slack');
  const m365Metrics    = filterBulletsByLayoutGroup(metrics, 'm365');
  const meetingMetrics = filterBulletsByLayoutGroup(metrics, 'meetings');

  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-4 p-4">
      {/* Slack */}
      <div>
        <ColumnHeading>Slack</ColumnHeading>
        <div className="flex flex-col gap-4">
          {slackMetrics.map((metric) => (
            <BulletChart key={metric.metric_key} metric={metric} onDrillClick={onDrillClick} mode="chart" personName={personName} />
          ))}
        </div>
      </div>

      {/* M365 */}
      <div>
        <ColumnHeading>M365</ColumnHeading>
        <div className="flex flex-col gap-4">
          {m365Metrics.map((metric) => (
            <BulletChart key={metric.metric_key} metric={metric} onDrillClick={onDrillClick} mode="chart" personName={personName} />
          ))}
        </div>
      </div>

      {/* Meetings */}
      <div>
        <ColumnHeading>Meetings · M365 · Zoom</ColumnHeading>
        <div className="flex flex-col gap-4">
          {meetingMetrics.map((metric) => (
            <BulletChart key={metric.metric_key} metric={metric} onDrillClick={onDrillClick} mode="chart" personName={personName} />
          ))}
        </div>
      </div>
    </div>
  );
};

export default React.memo(CollaborationSection);

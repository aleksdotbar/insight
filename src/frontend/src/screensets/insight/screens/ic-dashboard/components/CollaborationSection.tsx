/**
 * CollaborationSection — Slack, Microsoft 365, and meeting collaboration metrics.
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

const ChartLegend: React.FC = () => (
  <div className="flex gap-4 items-center mt-2">
    <div className="flex items-center gap-1">
      <div className="w-0.5 h-3 bg-gray-800/60 rounded" />
      <span className="text-2xs text-gray-400">Team median</span>
    </div>
    <div className="flex items-center gap-1">
      <div className="w-4 h-1.5 rounded bg-gradient-to-r from-green-600 via-amber-600 to-red-600" />
      <span className="text-2xs text-gray-400">Result · color = vs target</span>
    </div>
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

  // Chart mode — 4 columns by activity type (chat / email / meetings /
  // files) instead of by vendor. Source label (Slack / Microsoft 365 /
  // Zoom) stays visible inside each metric's own sublabel, so users can
  // still see "this number came from Slack" — they just don't have to
  // mentally re-aggregate Slack-chat + Teams-chat themselves any more.
  const chatMetrics     = filterBulletsByLayoutGroup(metrics, 'chat');
  const emailMetrics    = filterBulletsByLayoutGroup(metrics, 'email');
  const meetingsMetrics = filterBulletsByLayoutGroup(metrics, 'meetings');
  const filesMetrics    = filterBulletsByLayoutGroup(metrics, 'files');

  function renderColumn(heading: string, items: BulletMetric[]): React.ReactElement {
    return (
      <div className="flex flex-col gap-4">
        <ColumnHeading>{heading}</ColumnHeading>
        {items.map((m) => (
          <BulletChart
            key={m.metric_key}
            metric={m}
            onDrillClick={onDrillClick}
            mode="chart"
            personName={personName}
          />
        ))}
      </div>
    );
  }

  return (
    <div className="p-4">
      <ChartLegend />
      <div className="mt-2 grid grid-cols-1 sm:grid-cols-2 md:grid-cols-4 gap-x-4 gap-y-4">
        {renderColumn('Chat', chatMetrics)}
        {renderColumn('Email', emailMetrics)}
        {renderColumn('Meetings', meetingsMetrics)}
        {renderColumn('Files', filesMetrics)}
      </div>
    </div>
  );
};

export default React.memo(CollaborationSection);

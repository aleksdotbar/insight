/**
 * CollaborationSection — Slack, Microsoft 365, and meeting collaboration metrics.
 * Supports chart mode (3-column grouped) and tile mode (flat grid).
 * No state imports.
 */

import React from 'react';
import type { BulletMetric, ViewMode } from '../../../types';
import BulletChart from '../../../uikit/composite/BulletChart';
import ComingSoon from '../../../uikit/composite/ComingSoon';

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

  // Chart mode — 3-column grid with rows aligned by metric category so
  // analogous metrics across Slack / Microsoft 365 / Meetings sit at the
  // same level. Empty slots render an empty cell to preserve row height.
  const byKey = new Map(metrics.map((m) => [m.metric_key, m]));

  // Each row is one category; null slot = no analog in that channel.
  const ALIGNED_ROWS: Array<{
    slack: string | null;
    m365: string | null;
    meetings: string | null;
  }> = [
    { slack: 'slack_messages_sent',       m365: 'm365_emails_sent',         meetings: 'meetings_count' },
    { slack: 'slack_channel_posts',       m365: 'm365_emails_received',     meetings: 'meeting_hours' },
    { slack: 'slack_msgs_per_active_day', m365: 'm365_emails_read',         meetings: 'teams_meetings' },
    { slack: 'slack_dm_ratio',            m365: 'm365_teams_chats',         meetings: 'teams_meeting_hours' },
    { slack: 'slack_active_days',         m365: 'm365_files_engaged',       meetings: 'zoom_meetings' },
    { slack: null,                        m365: 'm365_files_shared_internal', meetings: 'zoom_meeting_hours' },
    { slack: null,                        m365: 'm365_files_shared_external', meetings: 'meeting_free' },
    { slack: null,                        m365: 'm365_active_days',         meetings: null },
  ];

  const renderCell = (key: string | null) => {
    if (!key) return <div />;
    const metric = byKey.get(key);
    if (!metric) return <div />;
    return (
      <BulletChart metric={metric} onDrillClick={onDrillClick} mode="chart" personName={personName} />
    );
  };

  return (
    <div className="p-4">
      <ChartLegend />
      <div className="mt-2 grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-x-4 gap-y-4">
        <ColumnHeading>Slack</ColumnHeading>
        <ColumnHeading>Microsoft 365</ColumnHeading>
        <ColumnHeading>Meetings · Microsoft 365 · Zoom</ColumnHeading>
        {ALIGNED_ROWS.flatMap((row, i) => [
          <React.Fragment key={`s-${i}`}>{renderCell(row.slack)}</React.Fragment>,
          <React.Fragment key={`m-${i}`}>{renderCell(row.m365)}</React.Fragment>,
          <React.Fragment key={`v-${i}`}>{renderCell(row.meetings)}</React.Fragment>,
        ])}
      </div>
    </div>
  );
};

export default React.memo(CollaborationSection);

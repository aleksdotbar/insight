/**
 * AiToolsSection — AI dev tools & AI chat metrics section.
 * Supports chart mode (grouped columns) and tile mode (flat grid).
 * No state imports.
 */

import React from 'react';
import type { BulletMetric, ViewMode } from '../../../types';
import BulletChart from '../../../uikit/composite/BulletChart';
import ComingSoon from '../../../uikit/composite/ComingSoon';
import { filterBulletsByLayoutGroup } from '../../../api/thresholdConfig';

export interface AiToolsSectionProps {
  metrics: BulletMetric[];
  viewMode: ViewMode;
  onDrillClick: (drillId: string) => void;
  personName?: string;
}

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

const SectionHeading: React.FC<{ children: React.ReactNode }> = ({ children }) => (
  <div className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-2">
    {children}
  </div>
);

const AiToolsSection: React.FC<AiToolsSectionProps> = ({
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

  // Chart mode — group into left/right columns via BULLET_LAYOUT_GROUPS
  // (single source of truth lives in thresholdConfig).
  const cursorMetrics = filterBulletsByLayoutGroup(metrics, 'ai_cursor_detail');
  const ccMetrics     = filterBulletsByLayoutGroup(metrics, 'ai_cc_detail');
  const aiLocMetrics  = filterBulletsByLayoutGroup(metrics, 'ai_loc_share');
  const chatMetrics   = filterBulletsByLayoutGroup(metrics, 'ai_web');

  return (
    <div className="p-4">
      {/* Legend above columns */}
      <ChartLegend />
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mt-3">
        {/* Left column */}
        <div className="flex flex-col gap-5">
          <div>
            <SectionHeading>CURSOR · api-dev</SectionHeading>
            <div className="flex flex-col gap-4">
              {cursorMetrics.map((metric) => (
                <BulletChart key={metric.metric_key} metric={metric} onDrillClick={onDrillClick} mode="chart" personName={personName} />
              ))}
            </div>
          </div>
          <div>
            <SectionHeading>CLAUDE CODE · Enterprise Admin API</SectionHeading>
            <div className="flex flex-col gap-4">
              {ccMetrics.map((metric) => (
                <BulletChart key={metric.metric_key} metric={metric} onDrillClick={onDrillClick} mode="chart" personName={personName} />
              ))}
            </div>
          </div>
        </div>

        {/* Right column */}
        <div className="flex flex-col gap-5">
          <div>
            <SectionHeading>AI CODE ACCEPTANCE · Cursor + Claude Code</SectionHeading>
            <div className="flex flex-col gap-4">
              {aiLocMetrics.map((metric) => (
                <BulletChart key={metric.metric_key} metric={metric} onDrillClick={onDrillClick} mode="chart" personName={personName} />
              ))}
            </div>
          </div>
          <div>
            <SectionHeading>AI CHAT · Claude Web · ChatGPT</SectionHeading>
            <div className="flex flex-col gap-4">
              {chatMetrics.map((metric) => (
                <BulletChart key={metric.metric_key} metric={metric} onDrillClick={onDrillClick} mode="chart" personName={personName} />
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default React.memo(AiToolsSection);

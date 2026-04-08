/**
 * AiToolsSection — AI dev tools & AI chat metrics section.
 * Supports chart mode (grouped columns) and tile mode (flat grid).
 * No state imports.
 */

import React from 'react';
import type { BulletMetric, ViewMode } from '../../../types';
import BulletChart from '../../../uikit/composite/BulletChart';

export interface AiToolsSectionProps {
  metrics: BulletMetric[];
  viewMode: ViewMode;
  onDrillClick: (drillId: string) => void;
  personName?: string;
}

const CURSOR_KEYS = ['cursor_completions', 'cursor_agents', 'cursor_lines'];
const CC_KEYS = ['cc_sessions', 'cc_lines', 'cc_tool_accept'];

const ChartLegend: React.FC = () => (
  <div className="flex gap-4 items-center mt-2">
    <div className="flex items-center gap-1">
      <div className="w-0.5 h-[11px] bg-blue-600/50 rounded" />
      <span className="text-[9px] text-gray-400">Team median</span>
    </div>
    <div className="flex items-center gap-1">
      <div className="w-[18px] h-[5px] rounded bg-gradient-to-r from-green-600 via-amber-600 to-red-600" />
      <span className="text-[9px] text-gray-400">Your result · color = vs target</span>
    </div>
  </div>
);

const SectionHeading: React.FC<{ children: React.ReactNode }> = ({ children }) => (
  <div className="text-[10px] font-bold text-gray-500 uppercase tracking-wide mb-2">
    {children}
  </div>
);

const AiToolsSection: React.FC<AiToolsSectionProps> = ({
  metrics,
  viewMode,
  onDrillClick,
  personName,
}) => {
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

  // Chart mode — group into left/right columns
  const cursorMetrics = metrics.filter((m) => CURSOR_KEYS.includes(m.metric_key));
  const ccMetrics = metrics.filter((m) => CC_KEYS.includes(m.metric_key));
  const aiLocMetrics = metrics.filter((m) => m.metric_key === 'ai_loc_share2');
  const chatMetrics = metrics.filter((m) => ['claude_web', 'chatgpt'].includes(m.metric_key));

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
            <SectionHeading>AI LOC SHARE · Cursor + Claude Code</SectionHeading>
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

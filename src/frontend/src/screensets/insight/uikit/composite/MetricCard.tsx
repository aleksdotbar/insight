/**
 * MetricCard — card wrapping a group of BulletChart metrics.
 * Chart mode: header with legend, multi-column round-robin layout.
 * Tile mode: fixed 3-col grid, no legend.
 * No state imports.
 */

import React from 'react';
import { Card, CardContent } from '@hai3/uikit';
import BulletChart from './BulletChart';
import ComingSoon from './ComingSoon';
import type { BulletMetric } from '../../types';

export interface MetricCardProps {
  title: string;
  metrics: Array<BulletMetric & { period?: string }>;
  columns?: 1 | 2 | 3;
  onDrillClick?: (drillId: string) => void;
  mode?: 'chart' | 'tile';
  personName?: string;
  /** True when the backend query for this section failed (vs returned empty). */
  errored?: boolean;
  /** True while the backend query is still in flight — shows a loading placeholder. */
  loading?: boolean;
  onRetry?: () => void;
}

const GRID_COLS_CLASS = {
  1: 'grid-cols-1',
  2: 'grid-cols-1 sm:grid-cols-2',
  3: 'grid-cols-1 sm:grid-cols-2 md:grid-cols-3',
} as const;

// Chart mode legend
const ChartLegend: React.FC = () => (
  <div className="flex gap-4 items-center mt-2">
    {/* Team median swatch */}
    <div className="flex items-center gap-1">
      <div className="w-[2px] h-3 bg-gray-800/60 rounded" />
      <span className="text-2xs text-gray-400">Team median</span>
    </div>

    {/* Gradient swatch */}
    <div className="flex items-center gap-1">
      <div className="w-4 h-1.5 rounded bg-gradient-to-r from-green-600 via-amber-600 to-red-600" />
      <span className="text-2xs text-gray-400">Result · color = vs target</span>
    </div>
  </div>
);

const MetricCard: React.FC<MetricCardProps> = ({
  title,
  metrics,
  columns = 1,
  onDrillClick,
  mode = 'chart',
  personName,
  errored = false,
  loading = false,
  onRetry,
}) => {
  // Loading takes precedence: while in flight we don't know yet whether the
  // section will end up empty or with data, so we render the loading skeleton
  // instead of "no data".
  const placeholderState = loading ? 'loading' : errored ? 'error' : 'empty';
  const isEmpty = metrics.length === 0;

  if (mode === 'tile') {
    return (
      <Card>
        <CardContent className="px-3.5 py-3">
          <div className="text-xs font-bold uppercase tracking-wide text-gray-500 mb-2">
            {title}
          </div>
          {isEmpty ? (
            <ComingSoon variant="card" state={placeholderState} onRetry={onRetry} />
          ) : (
            <div className="grid grid-cols-2 sm:grid-cols-3 gap-2">
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
          )}
        </CardContent>
      </Card>
    );
  }

  // Chart mode: distribute metrics round-robin across columns
  const cols = columns ?? 1;
  const columnGroups: Array<Array<BulletMetric & { period?: string }>> = Array.from(
    { length: cols },
    () => []
  );
  metrics.forEach((m, i) => {
    columnGroups[i % cols].push(m);
  });

  return (
    <Card>
      <CardContent className="px-3.5 py-3">
        {/* Header */}
        <div className="flex justify-between items-start">
          <span className="text-xs font-bold uppercase tracking-wide text-gray-500">
            {title}
          </span>
          <span className="text-xs text-gray-400">vs team range</span>
        </div>

        {/* Legend */}
        {!isEmpty && <ChartLegend />}

        {/* Columns or empty/error placeholder */}
        {isEmpty ? (
          <div className="mt-3">
            <ComingSoon variant="card" state={placeholderState} onRetry={onRetry} />
          </div>
        ) : (
          <div className={`grid ${GRID_COLS_CLASS[cols]} gap-3.5 mt-3`}>
            {columnGroups.map((colMetrics, colIdx) => (
              <div key={colIdx} className="flex flex-col gap-4">
                {colMetrics.map((metric) => (
                  <BulletChart
                    key={metric.metric_key}
                    metric={metric}
                    onDrillClick={onDrillClick}
                    mode="chart"
                    personName={personName}
                  />
                ))}
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  );
};

export default React.memo(MetricCard);

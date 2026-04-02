/**
 * insight API Mocks
 * Mock data for development and testing
 *
 * Keys use full URL patterns (including baseURL path).
 * Register mocks via MockPlugin in main.tsx or screenset.
 */

import type { MockMap } from '@hai3/react';
import type { DashboardData, SpeedData } from '../types';

const mockDashboardData: DashboardData = {
  stats: [
    { key: 'total_commits', value: '4.2K', sub: '825.5K lines added' },
    { key: 'lines_of_code', value: '1.2M', sub: '+928.5K - 313.8K' },
    { key: 'ai_involvement', value: '86%', sub: 'ai_involvement_sub' },
    { key: 'active_total', value: '402 / 582', sub: 'active_total_sub' },
  ],
  chartData: [
    { date: '03-19', aiLoc: 170, commitLoc: 300 },
    { date: '03-20', aiLoc: 150, commitLoc: 155 },
    { date: '03-21', aiLoc: 155, commitLoc: 160 },
    { date: '03-22', aiLoc: 165, commitLoc: 200 },
    { date: '03-23', aiLoc: 280, commitLoc: 290 },
    { date: '03-24', aiLoc: 200, commitLoc: 270 },
    { date: '03-25', aiLoc: 150, commitLoc: 175 },
    { date: '03-26', aiLoc: 140, commitLoc: 160 },
    { date: '03-27', aiLoc: 130, commitLoc: 150 },
    { date: '03-28', aiLoc: 120, commitLoc: 140 },
    { date: '03-29', aiLoc: 110, commitLoc: 130 },
    { date: '03-30', aiLoc: 100, commitLoc: 120 },
    { date: '03-31', aiLoc: 130, commitLoc: 380 },
  ],
  bottomMetrics: [
    { key: 'avg_ai_suggested', value: '235' },
    { key: 'avg_ai_accepted', value: '261' },
    { key: 'avg_ai_deleted', value: '31' },
    { key: 'avg_ai_nloc', value: '261' },
    { key: 'avg_ai_loc', value: '273' },
    { key: 'avg_commit_added', value: '96' },
    { key: 'avg_commit_deleted', value: '22' },
    { key: 'avg_commit_nloc', value: '108' },
    { key: 'avg_commit_loc', value: '137' },
  ],
  periodSplits: [
    { label: '2026-03-19 → 2026-03-25', value: 156 },
    { label: '2026-03-26 → 2026-03-31', value: 123 },
  ],
  periodDelta: -33,
};


const mockSpeedData: SpeedData = {
  value: 42,
  min: 0,
  max: 70,
  unit: 'km/h',
  label: 'current_speed',
};

/**
 * Mock map for insight API service
 */
export const insightMockMap = {
  'GET /api/insight/dashboard': (): DashboardData => mockDashboardData,
  'GET /api/insight/speed': (): SpeedData => mockSpeedData,
} satisfies MockMap;

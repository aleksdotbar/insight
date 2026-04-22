/**
 * Metric UUID Registry
 *
 * Maps semantic metric keys to stable UUIDs seeded into the Analytics API catalog.
 * UUIDs are assigned by the backend at seed time and must match the values in
 * the MariaDB `metrics` table.
 *
 * TODO: Replace placeholder UUIDs once backend catalog seed is deployed.
 * See: docs/components/backend/specs/analytics-views-api.md §4
 */

export const METRIC_REGISTRY = {
  // Executive screen (§4.1)
  EXEC_SUMMARY:         '00000000-0000-0000-0001-000000000001',

  // Team screen (§4.2)
  TEAM_MEMBER:          '00000000-0000-0000-0001-000000000002',
  TEAM_BULLET_DELIVERY: '00000000-0000-0000-0001-000000000003',
  TEAM_BULLET_QUALITY:  '00000000-0000-0000-0001-000000000004',
  TEAM_BULLET_COLLAB:   '00000000-0000-0000-0001-000000000005',
  TEAM_BULLET_AI:       '00000000-0000-0000-0001-000000000006',

  // IC Dashboard (§4.3)
  IC_KPIS:              '00000000-0000-0000-0001-000000000010',
  IC_BULLET_DELIVERY:   '00000000-0000-0000-0001-000000000011',
  IC_BULLET_COLLAB:     '00000000-0000-0000-0001-000000000012',
  IC_BULLET_AI:         '00000000-0000-0000-0001-000000000013',
  IC_CHART_LOC:         '00000000-0000-0000-0001-000000000014',
  IC_CHART_DELIVERY:    '00000000-0000-0000-0001-000000000015',
  IC_DRILL:             '00000000-0000-0000-0001-000000000016',
  IC_TIMEOFF:           '00000000-0000-0000-0001-000000000017',
  IC_BULLET_GIT:        '00000000-0000-0000-0001-000000000018',
} as const satisfies Record<string, string>;

export type MetricRegistryKey = keyof typeof METRIC_REGISTRY;

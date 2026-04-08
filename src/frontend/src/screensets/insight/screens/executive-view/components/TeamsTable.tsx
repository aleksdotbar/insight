/**
 * TeamsTable — full data table of all teams with status badges and colored metrics.
 * No state imports.
 */

import React from 'react';
import { Card, ScrollArea, Table, TableHeader, TableBody, TableHead, TableRow, TableCell, Badge, Skeleton } from '@hai3/uikit';
import type { ExecTeamRow, ExecColumnThreshold } from '../../../types';
import MetricInfo from '../../../uikit/base/MetricInfo';

export interface TeamsTableProps {
  teams: ExecTeamRow[];
  columnThresholds: ExecColumnThreshold[];
  loading: boolean;
}

const statusBadgeClass = (status: 'good' | 'warn' | 'bad'): string => {
  if (status === 'good') return 'bg-insight-green-bg text-insight-green border-insight-green/20';
  if (status === 'warn') return 'bg-insight-amber-bg text-insight-amber border-insight-amber/20';
  return 'bg-insight-red-bg text-insight-red border-insight-red/20';
};

function thresholdClass(pct: number, metricKey: string, thresholds: ExecColumnThreshold[]): string {
  const t = thresholds.find((x) => x.metric_key === metricKey);
  if (!t) return 'font-semibold';
  return pct >= t.threshold ? 'text-insight-green font-semibold' : 'text-insight-amber font-semibold';
}

const SkeletonRow: React.FC = () => (
  <TableRow>
    {Array.from({ length: 10 }).map((_, i) => (
      <TableCell key={i}>
        <Skeleton className="h-3.5 w-full" />
      </TableCell>
    ))}
  </TableRow>
);

export const TeamsTable: React.FC<TeamsTableProps> = ({ teams, columnThresholds, loading }) => {
  const buildT = columnThresholds.find((t) => t.metric_key === 'build_success_pct')?.threshold ?? 90;
  const focusT = columnThresholds.find((t) => t.metric_key === 'focus_time_pct')?.threshold ?? 60;
  const aiT    = columnThresholds.find((t) => t.metric_key === 'ai_adoption_pct')?.threshold ?? 60;
  return (
  <Card>
    <ScrollArea className="w-full">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Team</TableHead>
            <TableHead>Headcount</TableHead>
            <TableHead>Tasks Closed</TableHead>
            <TableHead>Bugs Fixed</TableHead>
            <TableHead>Build %<MetricInfo description={`CI/CD builds passing. Target ≥${buildT}%.`} side="bottom" /></TableHead>
            <TableHead>Focus %<MetricInfo description={`Work time in uninterrupted 60-min+ blocks. Target ≥${focusT}%.`} side="bottom" /></TableHead>
            <TableHead>AI Adoption %<MetricInfo description={`Share of members actively using any AI tool this period. Target ≥${aiT}%.`} side="bottom" /></TableHead>
            <TableHead>AI LOC %<MetricInfo description="Share of authored code lines accepted from AI suggestions (Cursor + Claude Code)." side="bottom" /></TableHead>
            <TableHead>PR Cycle<MetricInfo description="Average time from PR opened to merged, in hours. Lower is better." side="bottom" /></TableHead>
            <TableHead>Status</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {loading ? (
            <>
              <SkeletonRow />
              <SkeletonRow />
              <SkeletonRow />
            </>
          ) : (
            teams.map((team) => (
              <TableRow key={team.team_id}>
                <TableCell className="font-semibold">{team.team_name}</TableCell>
                <TableCell>{team.headcount}</TableCell>
                <TableCell>{team.tasks_closed}</TableCell>
                <TableCell>{team.bugs_fixed}</TableCell>
                <TableCell className={thresholdClass(team.build_success_pct, 'build_success_pct', columnThresholds)}>
                  {team.build_success_pct}%
                </TableCell>
                <TableCell className={thresholdClass(team.focus_time_pct, 'focus_time_pct', columnThresholds)}>
                  {team.focus_time_pct}%
                </TableCell>
                <TableCell className={thresholdClass(team.ai_adoption_pct, 'ai_adoption_pct', columnThresholds)}>
                  {team.ai_adoption_pct}%
                </TableCell>
                <TableCell>{team.ai_loc_share_pct}%</TableCell>
                <TableCell>{team.pr_cycle_time_h}h</TableCell>
                <TableCell>
                  <Badge variant="outline" className={statusBadgeClass(team.status)}>
                    {team.status}
                  </Badge>
                </TableCell>
              </TableRow>
            ))
          )}
        </TableBody>
      </Table>
    </ScrollArea>
  </Card>
  );
};

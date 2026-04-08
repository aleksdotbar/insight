/**
 * TeamsTable — full data table of all teams with status badges and colored metrics.
 * No state imports.
 */

import React from 'react';
import { Card, ScrollArea, Table, TableHeader, TableBody, TableHead, TableRow, TableCell, Badge, Skeleton } from '@hai3/uikit';
import type { ExecTeamRow } from '../../../types';
import MetricInfo from '../../../uikit/base/MetricInfo';

export interface TeamsTableProps {
  teams: ExecTeamRow[];
  loading: boolean;
}

const statusBadgeClass = (status: 'good' | 'warn' | 'bad'): string => {
  if (status === 'good') return 'text-green-600 border-green-600';
  if (status === 'warn') return 'text-amber-600 border-amber-600';
  return 'text-red-600 border-red-600';
};

const thresholdClass = (pct: number, threshold: number): string =>
  pct >= threshold ? 'text-green-600 font-semibold' : 'text-amber-600 font-semibold';

const SkeletonRow: React.FC = () => (
  <TableRow>
    {Array.from({ length: 10 }).map((_, i) => (
      <TableCell key={i}>
        <Skeleton className="h-3.5 w-full" />
      </TableCell>
    ))}
  </TableRow>
);

export const TeamsTable: React.FC<TeamsTableProps> = ({ teams, loading }) => (
  <Card>
    <ScrollArea className="w-full">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Team</TableHead>
            <TableHead>Headcount</TableHead>
            <TableHead>Tasks Closed</TableHead>
            <TableHead>Bugs Fixed</TableHead>
            <TableHead>Build %<MetricInfo description="CI/CD builds passing. Target ≥90%." side="bottom" /></TableHead>
            <TableHead>Focus %<MetricInfo description="Work time in uninterrupted 60-min+ blocks. Target ≥60%." side="bottom" /></TableHead>
            <TableHead>AI Adoption %<MetricInfo description="Share of members actively using any AI tool this period." side="bottom" /></TableHead>
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
                <TableCell className={thresholdClass(team.build_success_pct, 90)}>
                  {team.build_success_pct}%
                </TableCell>
                <TableCell className={thresholdClass(team.focus_time_pct, 60)}>
                  {team.focus_time_pct}%
                </TableCell>
                <TableCell className={thresholdClass(team.ai_adoption_pct, 60)}>
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

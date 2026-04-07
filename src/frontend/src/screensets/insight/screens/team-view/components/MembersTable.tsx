/**
 * MembersTable — clickable table of individual team members.
 * No state imports.
 */

import React from 'react';
import { Card, CardContent, Table, TableHeader, TableBody, TableHead, TableRow, TableCell, Badge } from '@hai3/uikit';
import { DynamicWidthBar } from '../../../uikit/base/DynamicWidthBar';
import type { TeamMember } from '../../../types';

export interface MembersTableProps {
  members: TeamMember[];
  loading: boolean;
  onRowClick: (personId: string) => void;
  onDetailsDrill?: () => void;
  onCellDrill?: (personId: string, drillId: string) => void;
}

// higher = better
const hi = (v: number, good: number, warn: number): string => {
  if (v >= good) return 'text-green-600';
  if (v >= warn) return 'text-amber-600';
  return 'text-red-600';
};

// lower = better
const lo = (v: number, good: number, warn: number): string => {
  if (v <= good) return 'text-green-600';
  if (v <= warn) return 'text-amber-600';
  return 'text-red-600';
};

// Bar color: higher = better
const hiBar = (v: number, good: number, warn: number): string => {
  if (v >= good) return 'bg-green-600';
  if (v >= warn) return 'bg-amber-600';
  return 'bg-red-600';
};

const DrillCell: React.FC<{
  children: React.ReactNode;
  className?: string;
  onClick: (e: React.MouseEvent) => void;
}> = ({ children, className = '', onClick }) => (
  <button
    type="button"
    onClick={onClick}
    className={`bg-transparent border-none p-0 cursor-pointer text-left underline decoration-dotted underline-offset-2 hover:text-blue-600 transition-colors ${className}`}
  >
    {children}
  </button>
);

const FocusBar: React.FC<{ pct: number }> = ({ pct }) => (
  <div className="flex items-center gap-1.5">
    <div className="w-20 h-1.5 rounded-full bg-slate-100 overflow-hidden flex-shrink-0">
      <DynamicWidthBar pct={pct} colorClass={hiBar(pct, 60, 50)} />
    </div>
    <span className={`text-[12px] font-bold ${hi(pct, 60, 50)}`}>{pct}%</span>
  </div>
);

const COL_HEADERS = [
  { label: 'Name',          sub: '' },
  { label: 'Tasks',         sub: 'closed · Jira' },
  { label: 'Bugs Fixed',    sub: 'bug-type tasks · Jira' },
  { label: 'Dev Time',      sub: 'time in dev per task · lower = better' },
  { label: 'Pull Requests', sub: 'merged to main · Bitbucket' },
  { label: 'Build Success', sub: 'CI builds passing · target ≥90%' },
  { label: 'Focus Time',    sub: 'uninterrupted work · target ≥60%' },
  { label: 'AI Tools',      sub: 'active this month' },
  { label: 'AI LOC Share',  sub: 'Cursor + Claude Code' },
];

const SkeletonRow: React.FC = () => (
  <TableRow>
    {COL_HEADERS.map((_, i) => (
      <TableCell key={i}>
        <div className="h-3.5 bg-gray-200 rounded animate-pulse" />
      </TableCell>
    ))}
  </TableRow>
);

export const MembersTable: React.FC<MembersTableProps> = ({ members, loading, onRowClick, onDetailsDrill, onCellDrill }) => {
  const drill = (personId: string, drillId: string) => (e: React.MouseEvent) => {
    e.stopPropagation();
    onCellDrill?.(personId, drillId);
  };
  return (
  <Card>
    <div className="px-4 pt-3.5 pb-3 border-b border-gray-200 flex items-center justify-between">
      <span className="text-[13px] font-bold text-gray-900">Team Members</span>
      <div className="flex items-center gap-3">
        {onDetailsDrill && (
          <button
            type="button"
            onClick={onDetailsDrill}
            className="text-[11px] font-medium text-blue-600 hover:text-blue-700 bg-transparent border-none cursor-pointer px-0"
          >
            View team stats ↗
          </button>
        )}
        <span className="text-[10px] text-gray-400">Click member to open IC dashboard</span>
      </div>
    </div>
    <CardContent className="p-0">
      <div className="overflow-x-auto">
        <Table>
          <TableHeader>
            <TableRow className="bg-gray-50 hover:bg-gray-50 border-b border-gray-200">
              {COL_HEADERS.map((col) => (
                <TableHead key={col.label} className="text-[10px] font-bold uppercase tracking-[0.4px] text-gray-400 h-9 px-3 bg-gray-50">
                  {col.label}
                  {col.sub && (
                    <>
                      <br />
                      <span className="font-normal text-gray-300 normal-case tracking-normal text-[9px]">{col.sub}</span>
                    </>
                  )}
                </TableHead>
              ))}
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
              members.map((m) => (
                <TableRow
                  key={m.person_id}
                  className="cursor-pointer border-b border-gray-200 hover:bg-blue-50/40"
                  onClick={() => onRowClick(m.person_id)}
                >
                  {/* Name + Seniority stacked */}
                  <TableCell className="px-3 py-2.5">
                    <div className="text-[12px] font-bold text-gray-900">{m.name}</div>
                    <div className="text-[10px] text-gray-400">{m.seniority}</div>
                  </TableCell>
                  <TableCell className="px-3 py-2.5 text-[12px]">
                    {onCellDrill ? (
                      <DrillCell onClick={drill(m.person_id, 'tasks-completed')}>{m.tasks_closed}</DrillCell>
                    ) : m.tasks_closed}
                  </TableCell>
                  <TableCell className={`px-3 py-2.5 text-[12px] font-bold ${hi(m.bugs_fixed, 15, 8)}`}>
                    {onCellDrill ? (
                      <DrillCell onClick={drill(m.person_id, 'bugs-fixed')} className={hi(m.bugs_fixed, 15, 8)}>{m.bugs_fixed}</DrillCell>
                    ) : m.bugs_fixed}
                  </TableCell>
                  <TableCell className={`px-3 py-2.5 text-[12px] font-bold ${lo(m.dev_time_h, 14, 20)}`}>
                    {onCellDrill ? (
                      <DrillCell onClick={drill(m.person_id, 'cycle-time')} className={lo(m.dev_time_h, 14, 20)}>{m.dev_time_h}h</DrillCell>
                    ) : `${m.dev_time_h}h`}
                  </TableCell>
                  <TableCell className="px-3 py-2.5 text-[12px]">
                    {onCellDrill ? (
                      <DrillCell onClick={drill(m.person_id, 'pull-requests')}>{m.prs_merged}</DrillCell>
                    ) : m.prs_merged}
                  </TableCell>
                  <TableCell className={`px-3 py-2.5 text-[12px] font-bold ${hi(m.build_success_pct, 90, 80)}`}>
                    {onCellDrill ? (
                      <DrillCell onClick={drill(m.person_id, 'builds')} className={hi(m.build_success_pct, 90, 80)}>{m.build_success_pct}%</DrillCell>
                    ) : `${m.build_success_pct}%`}
                  </TableCell>
                  <TableCell className="px-3 py-2.5">
                    <FocusBar pct={m.focus_time_pct} />
                  </TableCell>
                  <TableCell className="px-3 py-2.5">
                    {m.ai_tools.length === 0 ? (
                      <span className="text-gray-400">—</span>
                    ) : (
                      <div className="flex flex-wrap gap-1">
                        {m.ai_tools.map((tool) => (
                          <Badge key={tool} variant="outline" className="text-[9px] font-bold px-1.5 py-0 h-auto rounded bg-gray-50 border-gray-200 text-gray-400">
                            {tool}
                          </Badge>
                        ))}
                      </div>
                    )}
                  </TableCell>
                  <TableCell className={`px-3 py-2.5 text-[12px] font-bold ${hi(m.ai_loc_share_pct, 20, 10)}`}>
                    {m.ai_loc_share_pct > 0 ? `${m.ai_loc_share_pct}%` : <span className="text-gray-400">0%</span>}
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>
    </CardContent>
  </Card>
  );
};

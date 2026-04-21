/**
 * DrillModal — modal overlay for metric drill-down detail view.
 * Uses @hai3/uikit Dialog (Radix UI) for focus trap, escape key and portal.
 * Uses @hai3/uikit Table for the data grid.
 * No state imports.
 */

import React from 'react';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  ScrollArea,
  Table,
  TableHeader,
  TableBody,
  TableHead,
  TableRow,
  TableCell,
} from '@hai3/uikit';
import ComingSoon from './ComingSoon';

export interface DrillModalDrill {
  title: string;
  source: string;
  /** Tailwind bg class for the source badge, e.g. 'bg-blue-600' */
  srcClass: string;
  value: string;
  filter: string;
  columns: string[];
  rows: Record<string, string | number>[];
}

export interface DrillModalProps {
  drill: DrillModalDrill | null;
  open: boolean;
  onClose: () => void;
}

const DrillModal: React.FC<DrillModalProps> = ({ drill, open, onClose }) => (
  <Dialog open={open && !!drill} onOpenChange={(o) => { if (!o) onClose(); }}>
    <DialogContent className="w-full max-w-full sm:max-w-2xl max-h-[80vh] flex flex-col p-0 gap-0">
      {drill && (
        <>
          {/* Header */}
          <DialogHeader className="flex-row items-center gap-2.5 px-4 pr-12 py-3.5 border-b border-gray-200 space-y-0">
            <DialogTitle className="flex-1 text-base font-bold text-gray-900">
              {drill.title}
            </DialogTitle>
            <span className={`text-xs font-semibold text-white rounded px-2 py-0.5 ${drill.srcClass}`}>
              {drill.source}
            </span>
            <span className="text-sm font-bold text-gray-900">{drill.value}</span>
          </DialogHeader>

          {/* Filter bar */}
          <div className="bg-slate-100 px-4 py-2 text-xs text-gray-500 flex-shrink-0">
            {drill.filter}
          </div>

          {/* Table or empty-state */}
          <ScrollArea className="flex-1">
            {drill.rows.length === 0 ? (
              <div className="p-4">
                <ComingSoon variant="card" />
              </div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow className="bg-gray-50">
                    {drill.columns.map((col) => (
                      <TableHead
                        key={col}
                        className="text-xs font-semibold text-gray-500 whitespace-nowrap"
                      >
                        {col}
                      </TableHead>
                    ))}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {drill.rows.map((row, rowIdx) => (
                    <TableRow key={rowIdx}>
                      {drill.columns.map((col) => (
                        <TableCell key={col} className="text-gray-700 text-xs">
                          {row[col] ?? '—'}
                        </TableCell>
                      ))}
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </ScrollArea>

          {/* Footer */}
          <div className="flex justify-between items-center px-4 py-2.5 border-t border-gray-200 text-xs flex-shrink-0">
            <a
              href="#"
              className="text-blue-600 no-underline font-medium"
              onClick={(e) => e.preventDefault()}
            >
              Open all in {drill.source} ↗
            </a>
            <span className="text-gray-400">{drill.rows.length} records</span>
          </div>
        </>
      )}
    </DialogContent>
  </Dialog>
);

export default DrillModal;

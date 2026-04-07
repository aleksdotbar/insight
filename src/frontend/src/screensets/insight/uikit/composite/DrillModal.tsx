/**
 * DrillModal — modal overlay for metric drill-down detail view.
 * Shows title, source badge, value, filter string, data table, footer link.
 * Renders null when !open || !drill.
 * No state imports.
 */

import React from 'react';

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

const DrillModal: React.FC<DrillModalProps> = ({ drill, open, onClose }) => {
  if (!open || !drill) return null;

  const stopPropagation = (e: React.MouseEvent) => e.stopPropagation();

  return (
    /* Backdrop */
    <div
      className="fixed inset-0 bg-black/40 backdrop-blur-sm z-50 flex items-center justify-center"
      onClick={onClose}
    >
      {/* Modal panel */}
      <div
        className="bg-white rounded-xl shadow-2xl max-w-2xl w-full mx-4 max-h-[80vh] overflow-y-auto"
        onClick={stopPropagation}
      >
        {/* Header */}
        <div className="flex items-center gap-2.5 px-4 py-3.5 border-b border-gray-200">
          <span className="text-[15px] font-bold text-gray-900 flex-1">{drill.title}</span>
          <span className={`text-[10px] font-semibold text-white rounded px-2 py-0.5 ${drill.srcClass}`}>
            {drill.source}
          </span>
          <span className="text-[13px] font-bold text-gray-900">{drill.value}</span>
          <button
            type="button"
            onClick={onClose}
            className="bg-transparent border-none cursor-pointer text-base text-gray-500 px-1 leading-none"
          >
            ✕
          </button>
        </div>

        {/* Filter bar */}
        <div className="bg-slate-100 px-4 py-2 text-[11px] text-gray-500">
          {drill.filter}
        </div>

        {/* Table */}
        <div className="overflow-x-auto">
          <table className="w-full border-collapse text-xs">
            <thead>
              <tr className="bg-gray-50">
                {drill.columns.map((col) => (
                  <th
                    key={col}
                    className="px-3 py-2 text-left font-semibold text-gray-500 text-[11px] border-b border-gray-200 whitespace-nowrap"
                  >
                    {col}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {drill.rows.map((row, rowIdx) => (
                <tr key={rowIdx} className="border-b border-gray-100 hover:bg-gray-50">
                  {drill.columns.map((col) => (
                    <td key={col} className="px-3 py-2 text-gray-700">
                      {row[col] ?? '—'}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Footer */}
        <div className="flex justify-between items-center px-4 py-2.5 border-t border-gray-200 text-xs">
          <a
            href="#"
            className="text-blue-600 no-underline font-medium"
            onClick={(e) => e.preventDefault()}
          >
            Open all in {drill.source} ↗
          </a>
          <span className="text-gray-400">{drill.rows.length} records</span>
        </div>
      </div>
    </div>
  );
};

export default DrillModal;

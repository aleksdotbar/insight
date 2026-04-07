/**
 * CollapsibleSection — expandable/collapsible section with a trigger row.
 * Uses local useState for open state (initialised from defaultOpen).
 * No @hai3/state or state hook imports from @hai3/react.
 */

import React, { useState } from 'react';

export interface CollapsibleSectionProps {
  title: string;
  subtitle?: string;
  defaultOpen?: boolean;
  children: React.ReactNode;
}

const CollapsibleSection: React.FC<CollapsibleSectionProps> = ({
  title,
  subtitle,
  defaultOpen = false,
  children,
}) => {
  const [open, setOpen] = useState(defaultOpen);

  return (
    <div className="border border-gray-200 rounded-lg overflow-hidden">
      {/* Trigger row */}
      <button
        type="button"
        onClick={() => setOpen((prev) => !prev)}
        className="flex items-start justify-between w-full px-4 py-3 bg-white border-none cursor-pointer text-left hover:bg-black/[0.03] transition-colors"
      >
        <div>
          <span className="text-[13px] font-semibold text-gray-900">{title}</span>
          {subtitle && (
            <div className="text-[10px] text-gray-400 mt-0.5">{subtitle}</div>
          )}
        </div>
        <div className="flex items-center gap-1.5 flex-shrink-0">
          {/* Status badge */}
          <span className="text-[10px] text-gray-500 bg-slate-100 rounded px-1.5 py-px">
            {open ? 'Expanded' : 'Collapsed'}
          </span>
          {/* Arrow */}
          <span className="text-[10px] text-gray-500">{open ? '▴' : '▾'}</span>
        </div>
      </button>

      {/* Content */}
      {open && (
        <div className="border-t border-gray-200 bg-white">
          {children}
        </div>
      )}
    </div>
  );
};

export default CollapsibleSection;

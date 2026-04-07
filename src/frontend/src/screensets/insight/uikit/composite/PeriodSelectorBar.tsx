/**
 * PeriodSelectorBar — W/M/Q/Y period tabs + custom date range picker.
 * Desktop: full labels (Week / Month / Quarter / Year).
 * Segmented-control style (gray bg, white active pill, shadow) matching prototype NavBar.
 * Purely presentational: receives state as props, emits changes via callbacks.
 * No inline styles (Tailwind only).
 */

import React, { useState } from 'react';
import type { DateRange } from 'react-day-picker';
import { Popover, PopoverContent, PopoverTrigger, Calendar } from '@hai3/uikit';
import type { PeriodValue, CustomRange } from '../../types';

const TABS: { value: PeriodValue; label: string }[] = [
  { value: 'week', label: 'Week' },
  { value: 'month', label: 'Month' },
  { value: 'quarter', label: 'Quarter' },
  { value: 'year', label: 'Year' },
];

export interface PeriodSelectorBarProps {
  period: PeriodValue;
  customRange: CustomRange | null;
  onPeriodChange: (period: PeriodValue) => void;
  onRangeChange: (range: CustomRange | null) => void;
}

export const PeriodSelectorBar: React.FC<PeriodSelectorBarProps> = ({
  period,
  customRange,
  onPeriodChange,
  onRangeChange,
}) => {
  const [calOpen, setCalOpen] = useState(false);
  const [tempRange, setTempRange] = useState<DateRange | undefined>();

  const handleRangeSelect = (selected: DateRange | undefined) => {
    setTempRange(selected);
    if (selected?.from && selected?.to && selected.to.getTime() > selected.from.getTime()) {
      onRangeChange({
        from: selected.from.toISOString().slice(0, 10),
        to: selected.to.toISOString().slice(0, 10),
      });
      setCalOpen(false);
    }
  };

  const calLabel = customRange
    ? `${customRange.from.slice(5)} – ${customRange.to.slice(5)}`
    : null;

  return (
    <div className="flex items-center gap-2">
      {/* Period tabs — segmented control */}
      <div className="flex bg-[#F0F2F7] rounded-lg p-[3px] gap-px">
        {TABS.map(({ value, label }) => (
          <button
            key={value}
            type="button"
            onClick={() => onPeriodChange(value)}
            className={`px-3 py-1 rounded-md text-[12px] font-medium transition-all border-0 cursor-pointer ${
              period === value && !customRange
                ? 'bg-white text-gray-900 [box-shadow:0_1px_3px_rgba(0,0,0,0.1)]'
                : 'bg-transparent text-gray-500 hover:text-gray-700'
            }`}
          >
            {label}
          </button>
        ))}
      </div>

      {/* Custom date range button */}
      <Popover open={calOpen} onOpenChange={setCalOpen}>
        <PopoverTrigger asChild>
          <button
            type="button"
            className={`flex items-center gap-1.5 px-3 py-1 rounded-md text-[11px] font-medium border transition-colors ${
              customRange
                ? 'border-blue-300 bg-blue-50 text-blue-700'
                : 'border-gray-200 bg-white text-gray-500 hover:text-gray-700'
            }`}
          >
            <span>📅</span>
            {calLabel ?? <span className="hidden sm:inline">Custom</span>}
          </button>
        </PopoverTrigger>
        <PopoverContent className="p-0 w-auto" align="end">
          <div className="px-4 py-3 border-b border-gray-100">
            <div className="flex items-center gap-2">
              <span className="text-[12px] font-bold text-gray-900">Custom date range</span>
              <span className="text-[10px] font-semibold text-blue-600 bg-blue-50 px-2 py-px rounded-full">Custom</span>
            </div>
            <p className="text-[11px] text-gray-500 mt-0.5">Select start date, then end date.</p>
            {tempRange?.from && (
              <p className="text-[11px] font-semibold text-gray-900 mt-1.5">
                {tempRange.from.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' })}
                {tempRange.to
                  ? ` – ${tempRange.to.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' })}`
                  : <span className="text-gray-400"> → pick end date</span>}
              </p>
            )}
          </div>
          <Calendar
            mode="range"
            selected={tempRange}
            onSelect={handleRangeSelect}
            numberOfMonths={2}
          />
          <div className="px-4 py-2 border-t border-gray-100 flex gap-3">
            {customRange && (
              <button
                type="button"
                onClick={() => { setTempRange(undefined); onRangeChange(null); onPeriodChange('month'); setCalOpen(false); }}
                className="text-[11px] text-gray-500 hover:text-gray-700"
              >
                Clear
              </button>
            )}
            <button
              type="button"
              onClick={() => setCalOpen(false)}
              className="text-[11px] text-gray-500 hover:text-gray-700"
            >
              Cancel
            </button>
          </div>
        </PopoverContent>
      </Popover>
    </div>
  );
};

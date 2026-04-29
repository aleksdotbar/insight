/**
 * PeriodSelectorBar — W/M/Q/Y period tabs + custom date range picker.
 * Desktop: full labels (Week / Month / Quarter / Year).
 * Segmented-control style (gray bg, white active pill, shadow) matching prototype NavBar.
 * Purely presentational: receives state as props, emits changes via callbacks.
 * No inline styles (Tailwind only).
 */

import React, { useState } from 'react';
import type { DateRange } from 'react-day-picker';
import { Popover, PopoverContent, PopoverTrigger, Calendar, ToggleGroup, ToggleGroupItem, Button } from '@hai3/uikit';
import type { PeriodValue, CustomRange } from '../../types';
import { toISODate, resolveDateRange } from '../../utils/periodToDateRange';

/** Format an ISO date string (YYYY-MM-DD) to "DD MMM" using local naming. */
function formatShortDate(iso: string): string {
  const [y, m, d] = iso.split('-').map(Number);
  // Parse as local-midnight so the displayed day matches the picker calendar.
  const date = new Date(y ?? 1970, (m ?? 1) - 1, d ?? 1);
  return date.toLocaleDateString('en-GB', { day: 'numeric', month: 'short' });
}

const TABS: { value: PeriodValue; label: string; short: string }[] = [
  { value: 'week',    label: 'Week',    short: 'W' },
  { value: 'month',   label: 'Month',   short: 'M' },
  { value: 'quarter', label: 'Quarter', short: 'Q' },
  { value: 'year',    label: 'Year',    short: 'Y' },
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
    // Only update the temporary preview; commit happens on Apply.
    setTempRange(selected);
  };

  const calLabel = customRange
    ? `${customRange.from.slice(5)} – ${customRange.to.slice(5)}`
    : null;

  // Active resolved range (preset or custom) — shown next to the bar so the
  // user always sees which window they're querying. For presets it answers
  // "what does Week / Month / Quarter / Year currently mean".
  const activeRange = resolveDateRange(period, customRange);
  const activeRangeLabel = `${formatShortDate(activeRange.from)} – ${formatShortDate(activeRange.to)}`;

  return (
    <div className="flex items-center gap-3">
    <div className="flex items-center gap-0 bg-slate-100 rounded-lg p-[3px]">
      {/* Period tabs — segmented control */}
      <ToggleGroup
        type="single"
        value={customRange ? '' : period}
        onValueChange={(v) => { if (v) onPeriodChange(v as PeriodValue); }}
        className="gap-px"
      >
        {TABS.map(({ value, label, short }) => (
          <ToggleGroupItem
            key={value}
            value={value}
            className="px-3 py-1 rounded-md text-sm font-medium data-[state=on]:bg-white data-[state=on]:text-gray-900 data-[state=on]:[box-shadow:0_1px_3px_rgba(0,0,0,0.1)] data-[state=off]:text-gray-500"
          >
            <span className="hidden sm:inline">{label}</span>
            <span className="sm:hidden">{short}</span>
          </ToggleGroupItem>
        ))}
      </ToggleGroup>

      {/* Divider between period tabs and custom */}
      <div className="w-px h-4 bg-gray-300/60 mx-1 flex-shrink-0" />

      {/* Custom date range — same pill style as tabs */}
      <Popover open={calOpen} onOpenChange={setCalOpen}>
        <PopoverTrigger asChild>
          <button
            type="button"
            className={`px-3 py-1 rounded-md text-sm font-medium transition-colors flex items-center gap-1 ${
              customRange
                ? 'bg-white text-blue-600 [box-shadow:0_1px_3px_rgba(0,0,0,0.1)]'
                : 'text-gray-500 hover:text-gray-700'
            }`}
          >
            <span>📅</span>
            <span className="hidden sm:inline">{calLabel ?? 'Custom'}</span>
            <span className="sm:hidden">{calLabel ?? '…'}</span>
          </button>
        </PopoverTrigger>
        <PopoverContent className="p-0 w-auto" align="end">
          <div className="px-4 py-3 border-b border-gray-100">
            <div className="flex items-center gap-2">
              <span className="text-sm font-bold text-gray-900">Custom date range</span>
              <span className="text-xs font-semibold text-blue-600 bg-blue-50 px-2 py-px rounded-full">Custom</span>
            </div>
            <p className="text-xs text-gray-500 mt-0.5">Select start date, then end date.</p>
            {tempRange?.from && (
              <p className="text-xs font-semibold text-gray-900 mt-1.5">
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
            numberOfMonths={typeof window !== 'undefined' && window.innerWidth < 640 ? 1 : 2}
          />
          <div className="px-4 py-2 border-t border-gray-100 flex items-center gap-3">
            {customRange && (
              <Button variant="ghost" size="sm" className="h-auto p-0 text-xs text-gray-500"
                onClick={() => { setTempRange(undefined); onRangeChange(null); onPeriodChange('month'); setCalOpen(false); }}>
                Clear
              </Button>
            )}
            <Button variant="ghost" size="sm" className="h-auto p-0 text-xs text-gray-500" onClick={() => setCalOpen(false)}>
              Cancel
            </Button>
            <Button
              variant="default"
              size="sm"
              className="h-auto px-3 py-1 text-xs ml-auto"
              disabled={!tempRange?.from}
              onClick={() => {
                if (!tempRange?.from) return;
                const toDate = tempRange.to ?? tempRange.from;
                onRangeChange({ from: toISODate(tempRange.from), to: toISODate(toDate) });
                setCalOpen(false);
              }}
            >
              Apply
            </Button>
          </div>
        </PopoverContent>
      </Popover>
    </div>
      <span className="text-xs text-gray-500 whitespace-nowrap" aria-label="Active date range">
        {activeRangeLabel}
      </span>
    </div>
  );

};

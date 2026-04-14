/**
 * ViewModeToggle — Charts / Tiles mode segmented control.
 * Uses @hai3/uikit ToggleGroup (Radix UI) for accessibility and keyboard navigation.
 * No inline styles (Tailwind only).
 */

import React from 'react';
import { ToggleGroup, ToggleGroupItem } from '@hai3/uikit';
import type { ViewMode } from '../../types';

export interface ViewModeToggleProps {
  mode: ViewMode;
  onChange: (mode: ViewMode) => void;
}

export const ViewModeToggle: React.FC<ViewModeToggleProps> = ({ mode, onChange }) => (
  <ToggleGroup
    type="single"
    value={mode}
    onValueChange={(v) => { if (v) onChange(v as ViewMode); }}
    className="bg-gray-100 rounded-lg p-[3px] gap-px"
  >
    <ToggleGroupItem
      value="chart"
      className="px-3 py-1 rounded-md text-xs font-semibold data-[state=on]:bg-white data-[state=on]:text-gray-900 data-[state=on]:shadow-sm data-[state=off]:text-gray-500"
    >
      ▬ Charts
    </ToggleGroupItem>
    <ToggleGroupItem
      value="tile"
      className="px-3 py-1 rounded-md text-xs font-semibold data-[state=on]:bg-white data-[state=on]:text-gray-900 data-[state=on]:shadow-sm data-[state=off]:text-gray-500"
    >
      ⊞ Tiles
    </ToggleGroupItem>
  </ToggleGroup>
);

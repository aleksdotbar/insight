/**
 * ViewModeToggle — Charts / Tiles mode segmented control.
 * Matches prototype NavBar style: gray bg, white active pill, shadow.
 * No inline styles (Tailwind only).
 */

import React from 'react';
import type { ViewMode } from '../../types';

export interface ViewModeToggleProps {
  mode: ViewMode;
  onChange: (mode: ViewMode) => void;
}

export const ViewModeToggle: React.FC<ViewModeToggleProps> = ({ mode, onChange }) => (
  <div className="flex bg-gray-100 rounded-lg p-[3px] gap-px">
    <button
      type="button"
      onClick={() => onChange('chart')}
      className={`px-3 py-1 rounded-md text-[11px] font-semibold transition-all ${
        mode === 'chart' ? 'bg-white text-gray-900 shadow-sm' : 'text-gray-500 hover:text-gray-700'
      }`}
    >
      ▬ Charts
    </button>
    <button
      type="button"
      onClick={() => onChange('tile')}
      className={`px-3 py-1 rounded-md text-[11px] font-semibold transition-all ${
        mode === 'tile' ? 'bg-white text-gray-900 shadow-sm' : 'text-gray-500 hover:text-gray-700'
      }`}
    >
      ⊞ Tiles
    </button>
  </div>
);

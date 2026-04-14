/**
 * ProgressTrack — renders a bullet chart track with dynamic bar and median line positions.
 * Lives in uikit/base/ so inline style is allowed for dynamic percentage values.
 */

import React from 'react';

export interface ProgressTrackProps {
  /** Left offset of the value bar as a percentage (0–100) */
  barLeftPct: number;
  /** Width of the value bar as a percentage (0–100) */
  barWidthPct: number;
  /** Left position of the median line as a percentage (0–100) */
  medianLeftPct: number;
  /** Tailwind background-color class for the value bar, e.g. 'bg-insight-green' */
  barColorClass: string;
}

export const ProgressTrack: React.FC<ProgressTrackProps> = ({
  barLeftPct,
  barWidthPct,
  medianLeftPct,
  barColorClass,
}) => {
  const clampPct = (v: number) => Math.max(0, Math.min(100, v));
  const safeLeft   = clampPct(barLeftPct);
  const safeWidth  = Math.min(clampPct(barWidthPct), 100 - safeLeft);
  const safeMedian = clampPct(medianLeftPct);

  return (
    <div className="relative h-5 bg-slate-200 rounded mt-1.5">
      {/* Value bar */}
      <div
        style={{ left: `${safeLeft}%`, width: `${safeWidth}%` }}
        className={`absolute top-[3px] bottom-[3px] rounded-sm transition-[width] duration-500 ease-in-out ${barColorClass}`}
      />
      {/* Median line */}
      <div
        style={{ left: `${safeMedian}%` }}
        className="absolute w-[2px] -top-0.5 -bottom-0.5 -translate-x-1/2 bg-gray-800/60 rounded"
      />
    </div>
  );
};

/**
 * DynamicWidthBar — a div with dynamic percentage width.
 * Lives in uikit/base/ where inline style is allowed for the dynamic value.
 */

import React from 'react';

export interface DynamicWidthBarProps {
  /** Width as a percentage (0–100), capped at 100 */
  pct: number;
  /** Tailwind background-color class, e.g. 'bg-green-600' */
  colorClass: string;
}

export const DynamicWidthBar: React.FC<DynamicWidthBarProps> = ({ pct, colorClass }) => (
  <div
    style={{ width: `${Math.min(pct, 100)}%` }}
    className={`h-full rounded-full transition-[width] duration-[400ms] ease-in-out ${colorClass}`}
  />
);

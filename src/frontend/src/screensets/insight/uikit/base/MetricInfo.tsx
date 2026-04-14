/**
 * MetricInfo — small ⓘ icon that shows a description tooltip on hover.
 * Uses Radix UI tooltip (via @hai3/uikit) so it renders via a portal
 * and is never clipped by parent overflow or stacking contexts.
 */

import React from 'react';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@hai3/uikit';

interface MetricInfoProps {
  description: string;
  side?: 'top' | 'bottom' | 'left' | 'right';
}

const MetricInfo: React.FC<MetricInfoProps> = ({ description, side = 'top' }) => (
  <TooltipProvider delayDuration={200}>
    <Tooltip>
      <TooltipTrigger asChild>
        <span tabIndex={0} role="button" className="inline text-xs text-gray-400 cursor-help leading-none select-none ml-1 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-blue-500 rounded-sm">ⓘ</span>
      </TooltipTrigger>
      <TooltipContent side={side} className="max-w-56 text-xs leading-snug">
        {description}
      </TooltipContent>
    </Tooltip>
  </TooltipProvider>
);

export default MetricInfo;

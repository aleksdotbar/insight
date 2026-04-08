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
        <span className="inline text-[11px] text-gray-400 cursor-help leading-none select-none ml-1">ⓘ</span>
      </TooltipTrigger>
      <TooltipContent side={side} className="max-w-[220px] text-[11px] leading-snug">
        {description}
      </TooltipContent>
    </Tooltip>
  </TooltipProvider>
);

export default MetricInfo;

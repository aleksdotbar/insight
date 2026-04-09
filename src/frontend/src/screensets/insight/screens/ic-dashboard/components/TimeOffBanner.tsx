/**
 * TimeOffBanner — compact single-line time-off notice.
 * Renders null when notice is null.
 * No state imports.
 */

import React from 'react';
import type { TimeOffNotice } from '../../../types';

export interface TimeOffBannerProps {
  notice: TimeOffNotice | null;
}

const TimeOffBanner: React.FC<TimeOffBannerProps> = ({ notice }) => {
  if (!notice) return null;

  return (
    <div className="flex flex-wrap items-center gap-2 px-4 py-1.5 bg-amber-50 border-b border-amber-200 text-xs text-amber-800">
      <span className="text-base leading-none">📅</span>
      <span>
        <strong>{notice.days} days off</strong> this month ({notice.dateRange})
      </span>
      <span className="text-amber-400">·</span>
      <span className="text-amber-600">metrics reflect working days</span>
      <span className="text-amber-400">·</span>
      <a
        href={notice.bambooHrUrl}
        className="text-amber-700 underline underline-offset-2 hover:text-amber-900"
        target="_blank"
        rel="noopener noreferrer"
      >
        BambooHR ↗
      </a>
    </div>
  );
};

export default React.memo(TimeOffBanner);

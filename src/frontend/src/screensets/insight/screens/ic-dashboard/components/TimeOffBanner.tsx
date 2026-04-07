/**
 * TimeOffBanner — shows time-off notice for the current period.
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
    <div className="bg-amber-50 border-t border-amber-200 px-4 py-2 flex items-center gap-2 text-xs text-amber-900">
      📅{' '}
      <span>
        <strong>{notice.days} days off</strong> this month ({notice.dateRange}) · metrics reflect
        working days ·{' '}
        <a
          href={notice.bambooHrUrl}
          className="text-amber-700 underline"
          target="_blank"
          rel="noopener noreferrer"
        >
          BambooHR
        </a>
      </span>
    </div>
  );
};

export default React.memo(TimeOffBanner);

import { CalendarIcon, ExternalLink } from "lucide-react";

import type { TimeOffNotice } from "@/types/insight";

export interface TimeOffBannerProps {
  notice: TimeOffNotice | null;
}

export function TimeOffBanner({ notice }: TimeOffBannerProps) {
  if (!notice) return null;

  return (
    <div className="bg-warning/10 border-warning/30 text-warning flex flex-wrap items-center gap-2 border-b px-4 py-1.5 text-xs">
      <CalendarIcon className="size-3.5" />
      <span>
        <strong>{notice.days} days off</strong> this month ({notice.dateRange})
      </span>
      <span className="opacity-60">·</span>
      <span className="opacity-80">metrics reflect working days</span>
      <span className="opacity-60">·</span>
      <a
        href={notice.bambooHrUrl}
        className="inline-flex items-center gap-0.5 underline underline-offset-2 hover:opacity-80"
        target="_blank"
        rel="noopener noreferrer"
      >
        BambooHR
        <ExternalLink className="size-3" />
      </a>
    </div>
  );
}

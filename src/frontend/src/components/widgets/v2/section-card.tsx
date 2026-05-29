import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { useSettings } from "@/hooks/use-settings";
import { hasBulletValue, rowStatus } from "@/lib/insight/v2/peer-status";
import { type PeerStats } from "@/lib/peers";
import {
  aggregateSectionStatus,
  pickSectionHeadline,
  SECTION_STRIPE,
  sectionCounts,
  type ScoredMetric,
} from "@/lib/scoring";
import {
  STATUS_BG,
  STATUS_TEXT,
  applyFocusStatus,
} from "@/lib/status";
import { cn } from "@/lib/utils";
import type { BulletMetric } from "@/types/insight";

const COLLAB_PREVIEW_KEYS = [
  "meeting_hours",
  "slack_messages_sent",
  "m365_emails_sent",
];

function pickPreviewRows(
  sectionId: string | undefined,
  rows: BulletMetric[],
): BulletMetric[] {
  if (sectionId === "collaboration") {
    const byKey = new Map(rows.map((r) => [r.metric_key, r]));
    return COLLAB_PREVIEW_KEYS.map((k) => byKey.get(k)).filter(
      (r): r is BulletMetric => r != null && hasBulletValue(r),
    );
  }
  return rows.filter(hasBulletValue).slice(0, 3);
}

export interface SectionCardProps {
  title: string;
  rows: BulletMetric[];
  onOpen: () => void;
  onHover?: () => void;
  sectionId?: string;
  subtitle?: string;
  cohortStats?: Map<string, PeerStats>;
}

export function SectionCard({
  title,
  rows,
  onOpen,
  onHover,
  sectionId,
  subtitle,
  cohortStats,
}: SectionCardProps) {
  const { focusMode } = useSettings();
  const scored: ScoredMetric<BulletMetric>[] = rows.map((r) => ({
    row: r,
    status: rowStatus(r, cohortStats),
  }));
  const rawStatus = aggregateSectionStatus(scored);
  const status = applyFocusStatus(rawStatus, focusMode);
  const counts = sectionCounts(scored);
  const evaluated = counts.good + counts.warn + counts.bad;
  const topCount = counts.good;
  const badgeText =
    evaluated === 0
      ? "no peer data"
      : `${topCount} of ${evaluated} in top`;

  const headline = pickSectionHeadline(scored);
  const unit = headline?.row.unit ?? "";
  const summary = headline
    ? `${headline.row.label}: ${headline.row.value}${unit ? ` ${unit}` : ""}`
    : "No data for this period.";

  const preview = pickPreviewRows(sectionId, rows);
  const isEmpty = evaluated === 0 && preview.length === 0;
  const stripeClass =
    status === "neutral" ? "border-l-border" : SECTION_STRIPE[status];

  return (
    <Card
      render={
        <button
          type="button"
          onClick={onOpen}
          onMouseEnter={onHover}
          onFocus={onHover}
          aria-label={`Open ${title} details`}
        />
      }
      className={cn(
        "border-l-2 text-left transition-colors hover:bg-accent",
        stripeClass,
      )}
    >
      <CardHeader className="pb-2">
        <CardTitle className="text-base font-semibold">{title}</CardTitle>
        <CardDescription className="flex flex-col gap-1 text-xs">
          {subtitle ? (
            <span className="text-muted-foreground">{subtitle}</span>
          ) : null}
          <span className="flex items-center gap-1.5">
            <span
              className={cn("size-1.5 shrink-0 rounded-full", STATUS_BG[status])}
              aria-hidden
            />
            <span className="tabular-nums">{badgeText}</span>
          </span>
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-3 pt-0">
        {isEmpty ? (
          <p className="text-sm text-muted-foreground">
            No metrics with data for this period.
          </p>
        ) : (
          <>
            <p className="text-sm text-foreground/80">{summary}</p>
            {preview.length > 0 ? (
              <ul className="flex flex-col gap-1.5">
                {preview.map((r) => {
                  const previewStatus = applyFocusStatus(
                    rowStatus(r, cohortStats),
                    focusMode,
                  );
                  return (
                    <li
                      key={r.metric_key}
                      className="flex items-center gap-2 text-sm"
                    >
                      <span
                        className={cn(
                          "size-2 shrink-0 rounded-full",
                          STATUS_BG[previewStatus],
                        )}
                        aria-hidden
                      />
                      <span className="min-w-0 flex-1 truncate text-muted-foreground">
                        {r.label}
                      </span>
                      <span
                        className={cn(
                          "shrink-0 font-medium tabular-nums",
                          STATUS_TEXT[previewStatus],
                        )}
                      >
                        {r.value}
                        {r.unit ? (
                          <span className="ml-0.5 text-xs text-muted-foreground">
                            {r.unit}
                          </span>
                        ) : null}
                      </span>
                    </li>
                  );
                })}
              </ul>
            ) : null}
          </>
        )}
      </CardContent>
    </Card>
  );
}

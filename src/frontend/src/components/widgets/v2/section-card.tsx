import { Sparkles } from "lucide-react";

import { useCatalog } from "@/api/use-catalog";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { GroupCardEmpty } from "@/components/widgets/group-card-empty";
import { useSettings } from "@/hooks/use-settings";
import {
  hasBulletValue,
  peerStatusForRow,
  peerStatusToStatus,
} from "@/lib/insight/v2/peer-status";
import type { PeerStatusWithNeutral } from "@/lib/peers";
import {
  gradeSectionStanding,
  pickSectionHeadline,
  rankCounts,
  rankableCount,
  sectionStandingPhrase,
  type RankedMetric,
} from "@/lib/scoring";
import {
  STATUS_BG_CLASS,
  STATUS_STRIPE_LEFT,
  STATUS_TEXT_CLASS,
  applyFocusStatus,
} from "@/lib/status";
import { cn } from "@/lib/utils";
import type { BulletMetric } from "@/types/insight";

function pickPreviewRows(
  _sectionId: string | undefined,
  rows: BulletMetric[],
): BulletMetric[] {
  return rows.filter(hasBulletValue).slice(0, 3);
}

export interface SectionCardProps {
  title: string;
  rows: BulletMetric[];
  onOpen: () => void;
  onHover?: () => void;
  sectionId?: string;
  subtitle?: string;
  unavailable?: boolean;
  /**
   * Per-metric rank override. When supplied (team view), it replaces the
   * single-row quartile scoring — the team card rolls up each metric from
   * per-member-vs-own-department standings instead of comparing a team
   * aggregate against an individual band. Absent (IC view) → the row's own
   * cohort rank.
   */
  rankByMetricKey?: Map<string, PeerStatusWithNeutral>;
}

export function SectionCard({
  title,
  rows,
  onOpen,
  onHover,
  sectionId,
  subtitle,
  unavailable,
  rankByMetricKey,
}: SectionCardProps) {
  const { focusMode } = useSettings();
  const { byMetricKey } = useCatalog();
  const metricRank = (r: BulletMetric): PeerStatusWithNeutral =>
    rankByMetricKey?.get(r.metric_key) ?? peerStatusForRow(r, byMetricKey);

  if (unavailable) {
    return (
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-base font-semibold">{title}</CardTitle>
          <CardDescription className="flex items-center gap-1.5 text-xs">
            <Sparkles className="size-3.5 shrink-0" aria-hidden />
            Coming soon
          </CardDescription>
        </CardHeader>
        <CardContent className="pt-0">
          <p className="text-sm text-muted-foreground">Not tracked yet.</p>
        </CardContent>
      </Card>
    );
  }

  const ranked: RankedMetric<BulletMetric>[] = rows.map((r) => ({
    row: r,
    rank: metricRank(r),
  }));
  const counts = rankCounts(ranked);
  const evaluated = rankableCount(counts);
  const status = applyFocusStatus(gradeSectionStanding(counts), focusMode);
  const badgeText = sectionStandingPhrase(counts);

  const headline = pickSectionHeadline(ranked);
  const unit = headline?.row.unit ?? "";
  const summary = headline
    ? `${headline.row.label}: ${headline.row.value}${unit ? ` ${unit}` : ""}`
    : "No data for this period.";

  const preview = pickPreviewRows(sectionId, rows);
  const isEmpty = evaluated === 0 && preview.length === 0;
  const stripeClass = STATUS_STRIPE_LEFT[status];

  return (
    <Card
      // An empty card has nothing to drill into: render a plain, unfocusable
      // div instead of a button, and drop the interactive affordances.
      render={
        isEmpty ? undefined : (
          <button
            type="button"
            onClick={onOpen}
            onMouseEnter={onHover}
            onFocus={onHover}
            aria-label={`Open ${title} details`}
          />
        )
      }
      className={cn(
        !isEmpty && "text-left transition-colors hover:bg-accent/50",
        stripeClass,
      )}
    >
      <CardHeader className="pb-2">
        <CardTitle className="text-base font-semibold">{title}</CardTitle>
        {subtitle || !isEmpty ? (
          <CardDescription className="flex flex-col gap-1 text-xs">
            {subtitle ? (
              <span className="text-muted-foreground">{subtitle}</span>
            ) : null}
            {/* An empty card carries no standing — the badge would only
                restate the empty state below. */}
            {!isEmpty ? (
              <span className="flex items-center gap-1.5">
                <span
                  className={cn(
                    "size-1.5 shrink-0 rounded-full",
                    STATUS_BG_CLASS[status],
                  )}
                  aria-hidden
                />
                <span className="tabular-nums">{badgeText}</span>
              </span>
            ) : null}
          </CardDescription>
        ) : null}
      </CardHeader>
      <CardContent className="flex flex-col gap-3 pt-0">
        {isEmpty ? (
          <GroupCardEmpty />
        ) : (
          <>
            <p className="text-sm text-foreground/80">{summary}</p>
            {preview.length > 0 ? (
              <ul className="flex flex-col gap-1.5">
                {preview.map((r) => {
                  const previewStatus = applyFocusStatus(
                    peerStatusToStatus(metricRank(r)),
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
                          STATUS_BG_CLASS[previewStatus],
                        )}
                        aria-hidden
                      />
                      <span className="min-w-0 flex-1 truncate text-muted-foreground">
                        {r.label}
                      </span>
                      <span
                        className={cn(
                          "shrink-0 font-medium tabular-nums",
                          STATUS_TEXT_CLASS[previewStatus],
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

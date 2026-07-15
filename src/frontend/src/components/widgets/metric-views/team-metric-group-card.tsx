import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Spinner } from "@/components/ui/spinner";
import { ComingSoon } from "@/components/widgets/coming-soon";
import { useSettings } from "@/hooks/use-settings";
import type { MetricGroup } from "@/lib/insight/groups";
import { peerStatusToStatus } from "@/lib/insight/v2/peer-status";
import {
  teamMetricStandings,
  type TeamMetricStanding,
} from "@/lib/insight/team-metrics";
import {
  gradeSectionStanding,
  rankCounts,
  sectionStandingPhrase,
} from "@/lib/scoring";
import {
  STATUS_BG_CLASS,
  STATUS_STRIPE_LEFT,
  applyFocusStatus,
} from "@/lib/status";
import type { MetricCollectionResult } from "@/queries/metric-results";
import { cn } from "@/lib/utils";

export interface TeamMetricGroupCardProps {
  def: MetricGroup;
  data: MetricCollectionResult;
  memberIds: string[];
  onOpen: () => void;
  subtitle?: string;
}

/**
 * Team card for a metrics-backed group. No team aggregates — a ratio can't
 * be summed from per-member values — so each preview row reports roster
 * standings against members' own cohorts ("3 of 8 in top").
 */
export function TeamMetricGroupCard({
  def,
  data,
  memberIds,
  onOpen,
  subtitle,
}: TeamMetricGroupCardProps) {
  const { focusMode } = useSettings();

  if (data.isPending) {
    // Keep the card's identity while it loads: the name in the header, a
    // spinner in the body. Not interactive — nothing to open yet.
    return (
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-base font-semibold">{def.title}</CardTitle>
          {subtitle ? (
            <CardDescription className="text-xs text-muted-foreground">
              {subtitle}
            </CardDescription>
          ) : null}
        </CardHeader>
        <CardContent className="flex items-center justify-center py-6">
          <Spinner
            className="size-5 text-muted-foreground"
            aria-label={`Loading ${def.title}`}
          />
        </CardContent>
      </Card>
    );
  }
  if (data.isError) {
    return (
      <ComingSoon
        variant="card"
        state="error"
        label={def.title}
        onRetry={data.refetch}
      />
    );
  }

  const standings = teamMetricStandings(def, data.byKey, memberIds);
  const scored = standings.filter((s) => s.scored > 0);
  const counts = rankCounts(
    standings.map((standing) => ({ row: standing, rank: standing.verdict })),
  );
  const status = applyFocusStatus(gradeSectionStanding(counts), focusMode);
  const badgeText = sectionStandingPhrase(counts);

  const preview: TeamMetricStanding[] = def.card.preview
    .map((key) => scored.find((s) => s.metric.metric_key === key))
    .filter((s): s is TeamMetricStanding => s != null);
  const stripeClass = STATUS_STRIPE_LEFT[status];

  return (
    <Card
      render={
        <button
          type="button"
          onClick={onOpen}
          aria-label={`Open ${def.title} details`}
        />
      }
      className={cn(
        "text-left transition-colors hover:bg-accent/50",
        stripeClass,
      )}
    >
      <CardHeader className="pb-2">
        <CardTitle className="text-base font-semibold">{def.title}</CardTitle>
        <CardDescription className="flex flex-col gap-1 text-xs">
          {subtitle ? (
            <span className="text-muted-foreground">{subtitle}</span>
          ) : null}
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
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-3 pt-0">
        {scored.length === 0 ? (
          <p className="text-sm text-muted-foreground">
            No metrics with peer data for this period.
          </p>
        ) : (
          <ul className="flex flex-col gap-1.5">
            {(preview.length > 0 ? preview : scored.slice(0, 3)).map(
              (standing) => {
                const rowStatus = applyFocusStatus(
                  peerStatusToStatus(standing.verdict),
                  focusMode,
                );
                return (
                  <li
                    key={standing.metric.metric_key}
                    className="flex items-center gap-2 text-sm"
                  >
                    <span
                      className={cn(
                        "size-2 shrink-0 rounded-full",
                        STATUS_BG_CLASS[rowStatus],
                      )}
                      aria-hidden
                    />
                    <span className="min-w-0 flex-1 truncate text-muted-foreground">
                      {standing.metric.label}
                    </span>
                    <span className="shrink-0 font-medium tabular-nums">
                      {standing.top} of {standing.scored} in top
                    </span>
                  </li>
                );
              },
            )}
          </ul>
        )}
      </CardContent>
    </Card>
  );
}

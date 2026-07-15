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
import { formatMetricValue } from "@/lib/format";
import type { MetricGroup } from "@/lib/insight/groups";
import { peerStatusToStatus } from "@/lib/insight/v2/peer-status";
import { forEntity, type NormalizedMetricResult } from "@/lib/metrics/collection";
import { derivePeerStanding } from "@/lib/metrics/peer-standing";
import {
  aggregateSectionStatus,
  pickSectionHeadline,
  sectionCounts,
  type ScoredMetric,
} from "@/lib/scoring";
import {
  STATUS_BG_CLASS,
  STATUS_STRIPE_LEFT,
  STATUS_TEXT_CLASS,
  applyFocusStatus,
  type Status,
} from "@/lib/status";
import type { MetricCollectionResult } from "@/queries/metric-results";
import { cn } from "@/lib/utils";

export interface MetricGroupCardProps {
  def: MetricGroup;
  data: MetricCollectionResult;
  entityId: string;
  onOpen: () => void;
  onHover?: () => void;
  subtitle?: string;
  /**
   * Per-metric status override (team view): replaces the single-entity
   * quartile scoring with a roster rollup computed by the caller.
   */
  statusByMetricKey?: Map<string, Status>;
}

interface CardRow {
  metric: NormalizedMetricResult;
  value: number | null;
  status: Status;
}

export function MetricGroupCard({
  def,
  data,
  entityId,
  onOpen,
  onHover,
  subtitle,
  statusByMetricKey,
}: MetricGroupCardProps) {
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

  const rows: CardRow[] = def.collection.metrics.flatMap((metricConfig) => {
    const metric = data.byKey.get(metricConfig.key);
    if (!metric) return [];
    const entityData = forEntity(metric, entityId);
    const status =
      statusByMetricKey?.get(metric.metric_key) ??
      rowStatusFromPeer(metric, entityData.value, entityId);
    return [{ metric, value: entityData.value, status }];
  });

  const scored: ScoredMetric<CardRow>[] = rows.map((row) => ({
    row,
    status: row.status,
  }));
  const status = applyFocusStatus(aggregateSectionStatus(scored), focusMode);
  const counts = sectionCounts(scored);
  const evaluated = counts.good + counts.warn + counts.bad;
  const badgeText =
    evaluated === 0 ? "No peer data" : `${counts.good} of ${evaluated} in top`;

  const headline = pickSectionHeadline(scored);
  const summary =
    headline && headline.row.value != null
      ? `${headline.row.metric.label}: ${formatMetricValue(
          headline.row.value,
          headline.row.metric.format,
          headline.row.metric.unit,
        )}`
      : "No data for this period.";

  const preview = def.card.preview
    .map((key) => rows.find((row) => row.metric.metric_key === key))
    .filter((row): row is CardRow => row != null && row.value != null);
  const isEmpty = evaluated === 0 && preview.length === 0;
  const stripeClass = STATUS_STRIPE_LEFT[status];

  return (
    <Card
      render={
        <button
          type="button"
          onClick={onOpen}
          onMouseEnter={onHover}
          onFocus={onHover}
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
        {isEmpty ? (
          <p className="text-sm text-muted-foreground">
            No metrics with data for this period.
          </p>
        ) : (
          <>
            <p className="text-sm text-foreground/80">{summary}</p>
            {preview.length > 0 ? (
              <ul className="flex flex-col gap-1.5">
                {preview.map((row) => {
                  const previewStatus = applyFocusStatus(row.status, focusMode);
                  return (
                    <li
                      key={row.metric.metric_key}
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
                        {row.metric.label}
                      </span>
                      <span
                        className={cn(
                          "shrink-0 font-medium tabular-nums",
                          STATUS_TEXT_CLASS[previewStatus],
                        )}
                      >
                        {row.value != null
                          ? formatMetricValue(
                              row.value,
                              row.metric.format,
                              row.metric.unit,
                            )
                          : "—"}
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

function rowStatusFromPeer(
  metric: NormalizedMetricResult,
  value: number | null,
  entityId: string,
): Status {
  const peerRow =
    metric.peer?.values.find((v) => v.entity_id === entityId) ?? null;
  // All eligibility gates (observed / suppressed / flat pool / neutral
  // direction) live in the shared standing derivation.
  const standing = derivePeerStanding(metric.direction, {
    value,
    peer: peerRow,
  });
  return peerStatusToStatus(standing.rank);
}

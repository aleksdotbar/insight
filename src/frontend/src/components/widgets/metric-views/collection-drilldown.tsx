import { ComingSoon } from "@/components/widgets/coming-soon";
import { Spinner } from "@/components/ui/spinner";
import { MetricBreakdown } from "@/components/widgets/metric-views/metric-breakdown";
import { MetricTrend } from "@/components/widgets/metric-views/metric-trend";
import { PeerStory } from "@/components/widgets/metric-views/peer-story";
import type { DrilldownBlock, MetricGroup } from "@/lib/insight/groups";
import type { NormalizedMetricResult } from "@/lib/metrics/collection";
import { buildPeerStoryEntries } from "@/lib/metrics/peer-story";
import type { PeerCohortLabel } from "@/lib/peers";
import type { MetricCollectionResult } from "@/queries/metric-results";
import { cn } from "@/lib/utils";

export interface CollectionDrilldownProps {
  def: MetricGroup;
  data: MetricCollectionResult;
  entityId: string;
  cohortLabel?: PeerCohortLabel;
  className?: string;
}

function blockMetrics(
  block: DrilldownBlock,
  byKey: Map<string, NormalizedMetricResult>,
): NormalizedMetricResult[] {
  return block.metrics.flatMap((key) => {
    const metric = byKey.get(key);
    return metric ? [metric] : [];
  });
}

function Block({
  block,
  byKey,
  entityId,
}: {
  block: DrilldownBlock;
  byKey: Map<string, NormalizedMetricResult>;
  entityId: string;
}) {
  const metrics = blockMetrics(block, byKey);
  if (metrics.length === 0) return null;
  if (block.view === "timeseries") {
    return <MetricTrend metrics={metrics} entityId={entityId} chart={block.chart} />;
  }
  return (
    <>
      {metrics.map((metric) => (
        <MetricBreakdown
          key={metric.metric_key}
          metric={metric}
          entityId={entityId}
        />
      ))}
    </>
  );
}

/**
 * Drilldown body for a metrics-backed group: the def's chart blocks, then
 * the peer story over every collection metric (hero outlier, side cards,
 * chips, supporting fold).
 */
export function CollectionDrilldown({
  def,
  data,
  entityId,
  cohortLabel = "department",
  className,
}: CollectionDrilldownProps) {
  if (data.isPending) {
    return (
      <div
        className={cn(
          "flex h-full min-h-96 w-full items-center justify-center p-10",
          className,
        )}
      >
        <Spinner className="size-12 text-muted-foreground" />
      </div>
    );
  }

  if (data.isError) {
    return (
      <div
        className={cn(
          "flex h-full min-h-96 w-full items-center justify-center p-10",
          className,
        )}
      >
        <ComingSoon
          state="error"
          label="Unable to load metrics"
          onRetry={data.refetch}
        />
      </div>
    );
  }

  const entries = buildPeerStoryEntries(def.collection, data.byKey, entityId);

  return (
    <div
      className={cn(
        "flex min-h-full flex-col gap-4 p-4 transition-opacity sm:p-6",
        data.isFetching && "opacity-60",
        className,
      )}
    >
      {def.drilldown.map((block, index) => (
        <Block
          key={`${block.view}-${block.chart}-${index}`}
          block={block}
          byKey={data.byKey}
          entityId={entityId}
        />
      ))}
      <PeerStory entries={entries} cohortLabel={cohortLabel} />
    </div>
  );
}

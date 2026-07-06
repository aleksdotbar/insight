import type { DateRange } from "@/api/period-to-date-range";
import type { CatalogMetric } from "@/api/catalog-client";
import { useCatalog } from "@/api/use-catalog";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { ComingSoon } from "@/components/widgets/coming-soon";
import {
  PeerStorySection,
  type PeerStoryInput,
} from "@/components/widgets/v2/peer-story-section";
import type { PeerStats } from "@/lib/peers";
import {
  useIcCollabPeerCounters,
  type AiPeerCounterRow,
} from "@/queries/v2/ic-extras";

interface CollabMessagingPanelProps {
  personId: string | null | undefined;
  range: DateRange | null | undefined;
}

/** Build `PeerStats` from a counter row, or null when any band is absent. */
function counterStats(row: AiPeerCounterRow): PeerStats | null {
  const { p25, median, p75, range_min, range_max, n } = row;
  const bands = [p25, median, p75, range_min, range_max, n];
  if (bands.some((b) => b == null || !Number.isFinite(b))) return null;
  return {
    p25: p25 as number,
    p50: median as number,
    p75: p75 as number,
    min: range_min as number,
    max: range_max as number,
    n: n as number,
  };
}

function peerStoryEntries(
  rows: AiPeerCounterRow[],
  byMetricKey: (metricKey: string) => CatalogMetric | undefined,
): PeerStoryInput[] {
  return rows.flatMap((row) => {
    const catalog = byMetricKey(row.metric_key);
    if (!catalog || row.value == null || !Number.isFinite(row.value)) return [];
    return [
      {
        key: row.metric_key,
        label: catalog.label,
        sublabel: catalog.sublabel,
        value: row.value,
        unit: catalog.unit,
        format: catalog.format,
        higherIsBetter: catalog.higher_is_better,
        stats: counterStats(row),
      },
    ];
  });
}

/**
 * Messaging modality peer counters for the Collaboration drilldown (#1527):
 * `messages_sent` + `channel_posts` vs the person's department cohort. Mirrors
 * the AI adoption peer-counters section; fully catalog-driven — any counter row
 * whose `metric_key` is in the catalog renders.
 */
export function CollabMessagingPanel({
  personId,
  range,
}: CollabMessagingPanelProps) {
  const canQuery = Boolean(personId && range);
  const fallbackRange = range ?? { from: "", to: "" };
  const countersQ = useIcCollabPeerCounters(personId ?? "", fallbackRange);
  const catalogQ = useCatalog();

  if (!canQuery) return null;

  const isPending = countersQ.isPending || catalogQ.isLoading;
  const isError = countersQ.isError || catalogQ.isError;

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-semibold">Messaging</CardTitle>
        <CardDescription className="text-xs">
          Messages sent and channel posts vs your department
        </CardDescription>
      </CardHeader>
      <CardContent>
        {isPending ? (
          <Skeleton className="h-48 w-full rounded-lg" />
        ) : isError ? (
          <ComingSoon
            state="error"
            label="Messaging counters — unable to load"
            onRetry={() => {
              void countersQ.refetch();
              catalogQ.refetch();
            }}
          />
        ) : (
          <PeerStorySection
            entries={peerStoryEntries(countersQ.data ?? [], catalogQ.byMetricKey)}
            cohortLabel="department"
          />
        )}
      </CardContent>
    </Card>
  );
}

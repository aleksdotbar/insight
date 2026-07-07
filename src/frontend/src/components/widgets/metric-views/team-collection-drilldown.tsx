import { ComingSoon } from "@/components/widgets/coming-soon";
import { Spinner } from "@/components/ui/spinner";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { formatMetricValue } from "@/lib/format";
import type { MetricGroup } from "@/lib/insight/groups";
import { forEntity } from "@/lib/metrics/collection";
import type { MetricCollectionResult } from "@/queries/metric-results";
import { cn } from "@/lib/utils";

export interface TeamMemberRef {
  entityId: string;
  displayName: string;
}

export interface TeamCollectionDrilldownProps {
  def: MetricGroup;
  data: MetricCollectionResult;
  members: TeamMemberRef[];
  className?: string;
}

/**
 * Team drilldown for a metrics-backed group: per-member period values,
 * straight from the response — no client-side aggregation (ratio metrics
 * cannot be summed from per-member values).
 */
export function TeamCollectionDrilldown({
  def,
  data,
  members,
  className,
}: TeamCollectionDrilldownProps) {
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

  const metrics = def.collection.metrics.flatMap((metricConfig) => {
    const metric = data.byKey.get(metricConfig.key);
    return metric ? [metric] : [];
  });

  if (metrics.length === 0) {
    return (
      <p className="py-6 text-center text-sm text-muted-foreground">
        No data for this group in the selected period.
      </p>
    );
  }

  if (members.length === 0) {
    return (
      <p className="py-6 text-center text-sm text-muted-foreground">
        No team members to display.
      </p>
    );
  }

  return (
    <div
      className={cn(
        "min-h-0 overflow-auto p-4 transition-opacity sm:p-6",
        data.isFetching && "opacity-60",
        className,
      )}
    >
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className="sticky left-0 bg-background">Member</TableHead>
            {metrics.map((metric) => (
              <TableHead key={metric.metric_key} className="text-right">
                {metric.label}
              </TableHead>
            ))}
          </TableRow>
        </TableHeader>
        <TableBody>
          {members.map((member) => (
            <TableRow key={member.entityId}>
              <TableCell className="sticky left-0 bg-background font-medium">
                {member.displayName}
              </TableCell>
              {metrics.map((metric) => {
                const value = forEntity(metric, member.entityId).value;
                return (
                  <TableCell
                    key={metric.metric_key}
                    className="text-right tabular-nums"
                  >
                    {value == null
                      ? "—"
                      : formatMetricValue(value, metric.format, metric.unit)}
                  </TableCell>
                );
              })}
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

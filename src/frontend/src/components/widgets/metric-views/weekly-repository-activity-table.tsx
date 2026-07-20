import { eachWeekOfInterval, format, startOfWeek } from "date-fns";

import type { DateRange } from "@/api/period-to-date-range";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableFooter,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { ComingSoon } from "@/components/widgets/coming-soon";
import { ChartEmpty } from "@/components/widgets/metric-views/chart-empty";
import { Spinner } from "@/components/ui/spinner";
import { forEntity } from "@/lib/metrics/collection";
import type { MetricCollectionResult } from "@/queries/metric-results";

export interface WeeklyRepositoryActivityMetrics {
  commits: string;
  linesAdded: string;
  linesRemoved: string;
}

export interface WeeklyRepositoryActivityTableProps {
  data: MetricCollectionResult;
  entityId: string;
  range: DateRange;
  metrics: WeeklyRepositoryActivityMetrics;
}

interface RepositoryActivity {
  id: string;
  label: string;
  commits: Map<string, number>;
  linesAdded: Map<string, number>;
  linesRemoved: Map<string, number>;
  totalCommits: number;
  totalLinesAdded: number;
  totalLinesRemoved: number;
}

const NUMBER_FORMAT = new Intl.NumberFormat();

function parseDate(value: string): Date | null {
  const parts = value.split("-").map(Number);
  if (parts.length !== 3) return null;
  const [year, month, day] = parts;
  if (!year || !month || !day) return null;
  const date = new Date(year, month - 1, day);
  return date.getFullYear() === year &&
    date.getMonth() === month - 1 &&
    date.getDate() === day
    ? date
    : null;
}

function weeksInRange(range: DateRange): string[] {
  const from = parseDate(range.from);
  const to = parseDate(range.to);
  if (!from || !to || from > to) return [];
  return eachWeekOfInterval(
    { start: startOfWeek(from, { weekStartsOn: 1 }), end: to },
    { weekStartsOn: 1 }
  ).map((date) => format(date, "yyyy-MM-dd"));
}

function repositoryDimension(
  dimensions: Array<{ key: string; value: string; label?: string }>
) {
  return dimensions.find((dimension) => dimension.key === "repository") ?? null;
}

function repositoryActivity(
  data: MetricCollectionResult,
  entityId: string,
  metrics: WeeklyRepositoryActivityMetrics
): RepositoryActivity[] {
  const repositories = new Map<string, RepositoryActivity>();
  const apply = (
    metricKey: string,
    values: "commits" | "linesAdded" | "linesRemoved",
    total: "totalCommits" | "totalLinesAdded" | "totalLinesRemoved"
  ) => {
    const metric = data.byKey.get(metricKey);
    if (!metric) return;
    for (const series of forEntity(metric, entityId).series) {
      const repository = repositoryDimension(series.dimensions);
      if (!repository) continue;
      const activity = repositories.get(repository.value) ?? {
        id: repository.value,
        label: repository.label ?? repository.value,
        commits: new Map<string, number>(),
        linesAdded: new Map<string, number>(),
        linesRemoved: new Map<string, number>(),
        totalCommits: 0,
        totalLinesAdded: 0,
        totalLinesRemoved: 0,
      };
      for (const point of series.points) {
        if (point.value == null || !Number.isFinite(point.value)) continue;
        activity[values].set(
          point.bucket_start,
          (activity[values].get(point.bucket_start) ?? 0) + point.value
        );
        activity[total] += point.value;
      }
      repositories.set(repository.value, activity);
    }
  };

  apply(metrics.commits, "commits", "totalCommits");
  apply(metrics.linesAdded, "linesAdded", "totalLinesAdded");
  apply(metrics.linesRemoved, "linesRemoved", "totalLinesRemoved");

  return [...repositories.values()]
    .filter((repository) => repository.totalCommits > 0)
    .sort(
      (left, right) =>
        right.totalCommits - left.totalCommits ||
        left.label.localeCompare(right.label) ||
        left.id.localeCompare(right.id)
    );
}

function valueOrDash(value: number): string {
  return value === 0 ? "—" : NUMBER_FORMAT.format(value);
}

function Lines({ added, removed }: { added: number; removed: number }) {
  if (added === 0 && removed === 0) return <>—</>;
  return (
    <>
      <span className="text-emerald-600">+{NUMBER_FORMAT.format(added)}</span>
      <span className="text-muted-foreground">/</span>
      <span className="text-red-500">-{NUMBER_FORMAT.format(removed)}</span>
    </>
  );
}

export function WeeklyRepositoryActivityTable({
  data,
  entityId,
  range,
  metrics,
}: WeeklyRepositoryActivityTableProps) {
  if (data.isPending) {
    return (
      <Card className="flex min-h-64 items-center justify-center">
        <Spinner className="size-10 text-muted-foreground" />
      </Card>
    );
  }

  if (data.isError) {
    return (
      <Card className="flex min-h-64 items-center justify-center">
        <ComingSoon
          state="error"
          label="Unable to load weekly activity"
          onRetry={data.refetch}
        />
      </Card>
    );
  }

  const repositories = repositoryActivity(data, entityId, metrics);
  const weeks = weeksInRange(range);
  const totalCommits = repositories.reduce(
    (sum, repository) => sum + repository.totalCommits,
    0
  );
  const totalLinesAdded = repositories.reduce(
    (sum, repository) => sum + repository.totalLinesAdded,
    0
  );
  const totalLinesRemoved = repositories.reduce(
    (sum, repository) => sum + repository.totalLinesRemoved,
    0
  );

  return (
    <Card className="shrink-0 overflow-hidden">
      <CardHeader>
        <CardTitle className="text-sm font-semibold">Weekly activity</CardTitle>
        <CardDescription>
          Authored commits and changed lines by repository
        </CardDescription>
      </CardHeader>
      <CardContent className="px-0">
        {repositories.length === 0 ? (
          <ChartEmpty
            message="No commit activity in this period"
            className="h-40"
          />
        ) : (
          <>
            <Table className="min-w-max">
              <TableHeader>
                <TableRow>
                  <TableHead
                    rowSpan={2}
                    className="sticky left-0 z-20 min-w-40 bg-card"
                  >
                    Week starting
                  </TableHead>
                  {repositories.map((repository) => (
                    <TableHead
                      key={repository.id}
                      colSpan={2}
                      className="border-l text-center"
                    >
                      {repository.label}
                    </TableHead>
                  ))}
                </TableRow>
                <TableRow>
                  {repositories.map((repository) => (
                    <FragmentHeaders key={repository.id} />
                  ))}
                </TableRow>
              </TableHeader>
              <TableBody>
                {weeks.map((week) => (
                  <TableRow key={week}>
                    <TableCell className="sticky left-0 z-10 bg-card font-medium tabular-nums">
                      {week}
                    </TableCell>
                    {repositories.map((repository) => (
                      <RepositoryCells
                        key={repository.id}
                        commits={repository.commits.get(week) ?? 0}
                        linesAdded={repository.linesAdded.get(week) ?? 0}
                        linesRemoved={repository.linesRemoved.get(week) ?? 0}
                      />
                    ))}
                  </TableRow>
                ))}
              </TableBody>
              <TableFooter>
                <TableRow>
                  <TableCell className="sticky left-0 z-10 bg-muted/50 font-semibold">
                    Total
                  </TableCell>
                  {repositories.map((repository) => (
                    <RepositoryCells
                      key={repository.id}
                      commits={repository.totalCommits}
                      linesAdded={repository.totalLinesAdded}
                      linesRemoved={repository.totalLinesRemoved}
                      total
                    />
                  ))}
                </TableRow>
              </TableFooter>
            </Table>
            <div className="space-y-1 border-t px-4 pt-4 text-xs text-muted-foreground sm:px-6">
              <p>
                <span className="font-semibold text-foreground">
                  Grand total
                </span>
                {" — commits: "}
                {NUMBER_FORMAT.format(totalCommits)}, lines +
                {NUMBER_FORMAT.format(totalLinesAdded)}/-
                {NUMBER_FORMAT.format(totalLinesRemoved)}, window {range.from} →{" "}
                {range.to}.
              </p>
              <p>
                Commits are distinct authored non-merge commits. Lines are
                file-change insertions and deletions attributed through those
                commits. Weeks start Monday. Repositories are ordered by
                commits.
              </p>
            </div>
          </>
        )}
      </CardContent>
    </Card>
  );
}

function FragmentHeaders() {
  return (
    <>
      <TableHead className="min-w-24 border-l text-right">Commits</TableHead>
      <TableHead className="min-w-32 text-right">Lines +/-</TableHead>
    </>
  );
}

function RepositoryCells({
  commits,
  linesAdded,
  linesRemoved,
  total = false,
}: {
  commits: number;
  linesAdded: number;
  linesRemoved: number;
  total?: boolean;
}) {
  const className = total ? "font-semibold tabular-nums" : "tabular-nums";
  return (
    <>
      <TableCell className={`border-l text-right ${className}`}>
        {valueOrDash(commits)}
      </TableCell>
      <TableCell className={`text-right ${className}`}>
        <Lines added={linesAdded} removed={linesRemoved} />
      </TableCell>
    </>
  );
}

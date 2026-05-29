import { useEffect, useState } from "react";
import { Maximize2, Minimize2, XIcon } from "lucide-react";

import { CountersBlock } from "@/components/widgets/v2/counters-block";
import { DistributionStrip } from "@/components/widgets/v2/distribution-strip";
import { LocStackedBar } from "@/components/widgets/v2/loc-stacked-bar";
import {
  SectionTrend,
  type SectionTrendPoint,
  type SectionTrendSeries,
} from "@/components/widgets/v2/section-trend";
import { SummaryWithBreakdown } from "@/components/widgets/v2/summary-with-breakdown";
import { TreemapComposition } from "@/components/widgets/v2/treemap-composition";
import {
  Sheet,
  SheetClose,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Spinner } from "@/components/ui/spinner";
import type { DateRange } from "@/api/period-to-date-range";
import { partitionBullets } from "@/lib/insight/v2/partition";
import {
  useIcDrilldownBatch,
  type DrilldownBatchData,
} from "@/queries/v2/ic-extras";
import {
  deriveAiToolComposition,
  deriveCollabActivities,
} from "@/lib/insight/v2/derivations";
import type { PeerCohortLabel, PeerStats } from "@/lib/peers";
import { cn } from "@/lib/utils";
import type { BulletMetric, PeriodValue } from "@/types/insight";

export interface SectionDrilldownSheetProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: string;
  rows: BulletMetric[];
  sectionId?: string | null;
  personId?: string | null;
  range?: DateRange;
  period?: PeriodValue;
  cohortStats?: Map<string, PeerStats>;
  cohortLabel?: PeerCohortLabel;
}

// Demo toggle: render the drilldown as a centered modal dialog instead of a
// bottom sheet. Flip to false for the bottom-sheet presentation.
const DRILL_AS_DIALOG = true;

export function SectionDrilldownSheet({
  open,
  onOpenChange,
  title,
  rows,
  sectionId,
  personId,
  range,
  period,
  cohortStats,
  cohortLabel = "team",
}: SectionDrilldownSheetProps) {
  const [expanded, setExpanded] = useState(false);

  useEffect(() => {
    if (!open) setExpanded(false);
  }, [open]);

  const expandButton = (
    <Button
      variant="ghost"
      size="icon-sm"
      onClick={() => setExpanded((v) => !v)}
      aria-label={expanded ? "Shrink" : "Expand"}
    >
      {expanded ? <Minimize2 /> : <Maximize2 />}
    </Button>
  );

  const body = (
    <div className="no-scrollbar min-h-0 flex-1 overflow-y-auto">
      {open ? (
        <DrilldownBody
          rows={rows}
          sectionId={sectionId}
          personId={personId}
          range={range}
          period={period}
          cohortStats={cohortStats}
          cohortLabel={cohortLabel}
        />
      ) : null}
    </div>
  );

  if (DRILL_AS_DIALOG) {
    return (
      <Dialog open={open} onOpenChange={onOpenChange}>
        <DialogContent
          showCloseButton={false}
          className={cn(
            "flex max-w-none! flex-col gap-0 overflow-hidden p-0",
            expanded ? "h-[95vh] w-[95vw]" : "h-[70vh] w-[80vw]",
          )}
        >
          <DialogHeader className="shrink-0 flex-row items-center justify-between gap-2 border-b p-4">
            <DialogTitle>{title}</DialogTitle>
            <div className="flex items-center gap-0.5">
              {expandButton}
              <DialogClose
                render={
                  <Button variant="ghost" size="icon-sm" aria-label="Close" />
                }
              >
                <XIcon />
              </DialogClose>
            </div>
          </DialogHeader>
          {body}
        </DialogContent>
      </Dialog>
    );
  }

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent
        side="bottom"
        showCloseButton={false}
        className={cn(
          "flex flex-col gap-0 overflow-hidden rounded-t-lg",
          expanded ? "h-[95vh]!" : "h-[60vh]!",
        )}
      >
        <SheetHeader className="shrink-0 flex-row items-center justify-between gap-2 border-b">
          <SheetTitle>{title}</SheetTitle>
          <div className="flex items-center gap-0.5">
            {expandButton}
            <SheetClose
              render={
                <Button variant="ghost" size="icon-sm" aria-label="Close" />
              }
            >
              <XIcon />
            </SheetClose>
          </div>
        </SheetHeader>
        {body}
      </SheetContent>
    </Sheet>
  );
}

function DrilldownBody({
  rows,
  sectionId,
  personId,
  range,
  period,
  cohortStats,
  cohortLabel,
}: {
  rows: BulletMetric[];
  sectionId?: string | null;
  personId?: string | null;
  range?: DateRange;
  period?: PeriodValue;
  cohortStats?: Map<string, PeerStats>;
  cohortLabel: PeerCohortLabel;
}) {
  const { counters, distributions } = partitionBullets(rows);

  const batchQ = useIcDrilldownBatch({
    sectionId: sectionId ?? null,
    personId: personId ?? null,
    range: range ?? null,
    period: period ?? null,
  });

  const batch = batchQ.data;
  const isFirstLoad = batchQ.isPending && batchQ.fetchStatus !== "idle";
  const isBodyEmpty =
    Boolean(batch) &&
    counters.length === 0 &&
    distributions.length === 0 &&
    batch?.histograms.size === 0 &&
    !batch?.delivery?.length &&
    !batch?.loc?.length &&
    !batch?.sectionTrend?.length;
  const showFullSpinner = isFirstLoad || (isBodyEmpty && batchQ.isFetching);

  if (showFullSpinner) {
    return (
      <div className="flex h-full w-full items-center justify-center p-10">
        <Spinner className="size-12 text-muted-foreground" />
      </div>
    );
  }

  return (
    <div
      className={cn(
        "flex flex-col gap-6 p-4 transition-opacity sm:p-6",
        batchQ.isFetching && "opacity-60"
      )}
    >
      {sectionId && batch ? (
        <DrilldownExtras sectionId={sectionId} batch={batch} rows={rows} />
      ) : null}
      {counters.length > 0 ? (
        <CountersBlock
          rows={counters}
          cohortStats={cohortStats}
          cohortLabel={cohortLabel}
        />
      ) : null}
      {distributions.length > 0 ? (
        <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
          {distributions.map((r) => (
            <DistributionStrip
              key={r.metric_key}
              row={r}
              bins={batch?.histograms.get(r.metric_key) ?? null}
              cohortStats={cohortStats?.get(r.metric_key) ?? null}
              cohortLabel={cohortLabel}
            />
          ))}
        </div>
      ) : null}
      {rows.length === 0 ? (
        <p className="py-6 text-center text-sm text-muted-foreground">
          No data for this section in the selected period.
        </p>
      ) : null}
    </div>
  );
}

function DrilldownExtras({
  sectionId,
  batch,
  rows,
}: {
  sectionId: string;
  batch: DrilldownBatchData;
  rows: BulletMetric[];
}) {
  if (sectionId === "task_delivery") {
    const data: SectionTrendPoint[] = (batch.delivery ?? []).map((d) => ({
      date: d.label,
      tasksDone: d.tasksDone,
    }));
    const series: SectionTrendSeries[] = [
      { key: "tasksDone", label: "Tasks closed" },
    ];
    return (
      <SectionTrend
        title="Daily task throughput"
        description="Closed issues per day"
        series={series}
        data={data}
      />
    );
  }
  if (sectionId === "git_output") {
    const data: SectionTrendPoint[] = (batch.delivery ?? []).map((d) => ({
      date: d.label,
      commits: d.commits,
      prsMerged: d.prsMerged ?? 0,
    }));
    const series: SectionTrendSeries[] = [
      { key: "commits", label: "Commits" },
      { key: "prsMerged", label: "PRs merged" },
    ];
    return (
      <div className="flex flex-col gap-4">
        <LocStackedBar data={batch.loc ?? []} />
        <SectionTrend
          title="Commits & PRs merged"
          description="Counts per day"
          series={series}
          data={data}
        />
      </div>
    );
  }
  if (sectionId === "code_quality") {
    const series: SectionTrendSeries[] = [
      { key: "pr_cycle_time", label: "PR cycle (h)" },
      { key: "build_success", label: "Build success (%)", yAxisId: "right" },
    ];
    return (
      <SectionTrend
        title="PR cycle & build trend"
        description="Cycle time and build success per day"
        series={series}
        data={(batch.sectionTrend ?? []) as SectionTrendPoint[]}
        rightAxis
      />
    );
  }
  if (sectionId === "ai_adoption") {
    const trendSeries: SectionTrendSeries[] = [
      {
        key: "cc_lines",
        label: "Claude Code lines",
        type: "area",
        yAxisId: "left",
      },
      {
        key: "cursor_lines",
        label: "Cursor lines",
        type: "area",
        yAxisId: "right",
      },
    ];
    return (
      <div className="flex flex-col gap-4">
        <SectionTrend
          title="Daily AI authored lines"
          description="Claude Code (left) + Cursor (right)"
          series={trendSeries}
          data={(batch.sectionTrend ?? []) as SectionTrendPoint[]}
          rightAxis
        />
        <TreemapComposition
          title="AI tool share"
          description="Share of activity per tool"
          rows={deriveAiToolComposition(rows)}
        />
      </div>
    );
  }
  if (sectionId === "collaboration") {
    const activities = deriveCollabActivities(rows);
    return (
      <div className="grid grid-cols-1 gap-3 md:grid-cols-2 lg:grid-cols-3">
        {activities.map((a) => (
          <SummaryWithBreakdown
            key={a.category}
            label={a.label}
            description={a.description}
            value={a.value}
            unit={a.unit}
            breakdown={[]}
          />
        ))}
      </div>
    );
  }
  return null;
}

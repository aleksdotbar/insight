import {
  BulletRow,
  type PeerCohortLabel,
} from "@/components/widgets/v2/bullet-row";
import { CountersBlock } from "@/components/widgets/v2/counters-block";
import { HistogramStrip } from "@/components/widgets/v2/histogram-strip";
import { LocStackedBar } from "@/components/widgets/v2/loc-stacked-bar";
import { PeriodSelectorBar } from "@/components/widgets/period-selector-bar";
import {
  SectionTrend,
  type SectionTrendPoint,
  type SectionTrendSeries,
} from "@/components/widgets/v2/section-trend";
import { SummaryWithBreakdown } from "@/components/widgets/v2/summary-with-breakdown";
import { TreemapComposition } from "@/components/widgets/v2/treemap-composition";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { Spinner } from "@/components/ui/spinner";
import type { DateRange } from "@/api/period-to-date-range";
import { usePeriod } from "@/hooks/use-period";
import { partitionBullets } from "@/lib/insight/v2/partition";
import {
  useIcDrilldownBatch,
  type DrilldownBatchData,
  type HistogramBin,
} from "@/queries/v2/ic-extras";
import { BULLET_DEFS_BY_KEY } from "@/lib/insight/v2/bullet-defs";
import {
  deriveAiToolComposition,
  deriveCollabActivities,
} from "@/lib/insight/v2/derivations";
import type { PeerStats } from "@/lib/peers";
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
  const {
    period: selectorPeriod,
    customRange,
    setPeriod,
    setCustomRange,
  } = usePeriod();

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent
        side="bottom"
        className="flex h-full! flex-col gap-0 overflow-hidden rounded-t-lg"
      >
        <SheetHeader className="shrink-0 flex-row items-center justify-between gap-3 border-b pe-16">
          <SheetTitle>{title}</SheetTitle>
          <PeriodSelectorBar
            period={selectorPeriod}
            customRange={customRange}
            onPeriodChange={setPeriod}
            onRangeChange={setCustomRange}
          />
        </SheetHeader>
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
  const showFullSpinner =
    isFirstLoad || (isBodyEmpty && batchQ.isFetching);

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
        "flex flex-col gap-6 p-4 sm:p-6 transition-opacity",
        batchQ.isFetching && "opacity-60",
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
          {distributions.map((r) => {
            const bins = batch?.histograms.get(r.metric_key) ?? null;
            return personId && range && bins ? (
              <DistributionHistogram
                key={r.metric_key}
                row={r}
                bins={bins}
              />
            ) : (
              <BulletRow
                key={r.metric_key}
                row={r}
                cohortStats={cohortStats?.get(r.metric_key) ?? null}
                cohortLabel={cohortLabel}
              />
            );
          })}
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

function DistributionHistogram({
  row,
  bins,
}: {
  row: BulletMetric;
  bins: HistogramBin[];
}) {
  const medianRaw = Number(row.median);
  const median = Number.isFinite(medianRaw) ? medianRaw : undefined;
  const def = BULLET_DEFS_BY_KEY[row.metric_key];
  const title = def?.label ?? row.label;
  const unit = row.unit || def?.unit || "";
  return (
    <HistogramStrip title={title} unit={unit} bins={bins} median={median} />
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
        description="Jira · daily closed issues"
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
          description="Bitbucket · per-day counts"
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
        description="Bitbucket + CI · daily"
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


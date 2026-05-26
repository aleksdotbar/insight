import { useMemo } from "react";
import { useTranslation } from "react-i18next";

import { BULLET_DEFS } from "@/api/threshold-config";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { ComingSoon } from "@/components/widgets/coming-soon";
import { cn } from "@/lib/utils";
import type {
  TeamMetricsEntry,
  TeamMetricsSectionId,
} from "@/queries/team-metrics";
import type { TeamMember } from "@/types/insight";

interface MetricMeta {
  label: string;
  unit: string;
}

const METRIC_META: Record<string, MetricMeta> = BULLET_DEFS.reduce<
  Record<string, MetricMeta>
>((acc, d) => {
  if (!acc[d.metric_key]) acc[d.metric_key] = { label: d.label, unit: d.unit };
  return acc;
}, {});

const META_FALLBACK = (metricKey: string): MetricMeta => ({
  label: metricKey,
  unit: "",
});

const SECTION_ORDER: ReadonlyArray<TeamMetricsSectionId> = [
  "task_delivery",
  "collaboration",
  "ai_adoption",
  "git_output",
];

const SECTION_LABEL_KEY: Record<TeamMetricsSectionId, string> = {
  task_delivery: "team_metrics_modal.sections.task_delivery",
  collaboration: "team_metrics_modal.sections.collaboration",
  ai_adoption: "team_metrics_modal.sections.ai_adoption",
  git_output: "team_metrics_modal.sections.git_output",
};

interface Column {
  sectionId: TeamMetricsSectionId;
  metricKey: string;
}

interface Pivot {
  columns: Column[];
  bySection: Map<TeamMetricsSectionId, Column[]>;
  cells: Map<string, Map<string, number>>;
}

function buildPivot(entries: TeamMetricsEntry[]): Pivot {
  const cells = new Map<string, Map<string, number>>();
  const seen = new Map<TeamMetricsSectionId, Set<string>>();

  for (const entry of entries) {
    if (!entry.rows) continue;
    const personCells = cells.get(entry.personId) ?? new Map<string, number>();
    for (const row of entry.rows) {
      if (
        row.value === null ||
        row.value === undefined ||
        !Number.isFinite(row.value)
      ) {
        continue;
      }
      personCells.set(row.metric_key, row.value);
      const sectionKeys = seen.get(entry.sectionId) ?? new Set<string>();
      sectionKeys.add(row.metric_key);
      seen.set(entry.sectionId, sectionKeys);
    }
    cells.set(entry.personId, personCells);
  }

  const bySection = new Map<TeamMetricsSectionId, Column[]>();
  const columns: Column[] = [];

  for (const sectionId of SECTION_ORDER) {
    const keys = seen.get(sectionId);
    if (!keys || keys.size === 0) continue;
    const sectionCols = Array.from(keys)
      .sort((a, b) => {
        const la = (METRIC_META[a] ?? META_FALLBACK(a)).label;
        const lb = (METRIC_META[b] ?? META_FALLBACK(b)).label;
        return la.localeCompare(lb);
      })
      .map((metricKey) => ({ sectionId, metricKey }));
    bySection.set(sectionId, sectionCols);
    columns.push(...sectionCols);
  }

  return { columns, bySection, cells };
}

function formatValue(v: number | undefined): string {
  if (v === undefined || !Number.isFinite(v)) return "—";
  const rounded = Math.round(v);
  return Math.abs(rounded) >= 1000 ? rounded.toLocaleString() : String(rounded);
}

function formatColumnHeader(metricKey: string): string {
  const meta = METRIC_META[metricKey] ?? META_FALLBACK(metricKey);
  if (!meta.unit) return meta.label;
  return `${meta.label} (${meta.unit})`;
}

export interface TeamMetricsTableProps {
  members: TeamMember[];
  entries: TeamMetricsEntry[];
  isPending: boolean;
  containerClassName?: string;
}

export function TeamMetricsTable({
  members,
  entries,
  containerClassName,
  isPending,
}: TeamMetricsTableProps) {
  const { t } = useTranslation();
  const pivot = useMemo(() => buildPivot(entries), [entries]);

  if (isPending && pivot.columns.length === 0) {
    const skeletonRows = Math.min(6, Math.max(1, members.length));
    return (
      <Table
        className="h-full min-w-max"
        containerClassName={containerClassName}
      >
        <TableHeader>
          <TableRow>
            <TableHead className="sticky left-0 z-10 min-w-48 bg-card">
              {t("team_metrics_modal.member_column")}
            </TableHead>
            <TableHead>
              <Skeleton className="h-3 w-24" />
            </TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {Array.from({ length: skeletonRows }).map((_, i) => (
            <TableRow key={i} className="hover:bg-transparent">
              <TableCell className="sticky left-0 z-10 bg-card">
                <Skeleton className="h-4 w-32" />
              </TableCell>
              <TableCell>
                <Skeleton className="h-4 w-16" />
              </TableCell>
            </TableRow>
          ))}
          <TableRow aria-hidden="true" className="h-full hover:bg-transparent">
            <TableCell className="sticky left-0 z-10 bg-card" />
            <TableCell />
          </TableRow>
        </TableBody>
      </Table>
    );
  }

  if (pivot.columns.length === 0) {
    return (
      <div className={cn("p-6", containerClassName)}>
        <ComingSoon
          variant="card"
          state="empty"
          label={t("team_metrics_modal.empty")}
        />
      </div>
    );
  }

  return (
    <Table className="h-full min-w-max" containerClassName={containerClassName}>
      <TableHeader>
        <TableRow className="hover:bg-transparent">
          <TableHead
            rowSpan={2}
            className="sticky left-0 z-20 min-w-48 bg-card align-bottom"
          >
            {t("team_metrics_modal.member_column")}
          </TableHead>
          {SECTION_ORDER.map((sectionId) => {
            const cols = pivot.bySection.get(sectionId);
            if (!cols || cols.length === 0) return null;
            return (
              <TableHead
                key={sectionId}
                colSpan={cols.length}
                className="border-l border-border text-center text-xs font-semibold whitespace-nowrap"
              >
                {t(SECTION_LABEL_KEY[sectionId])}
              </TableHead>
            );
          })}
        </TableRow>
        <TableRow className="hover:bg-transparent">
          {pivot.columns.map((col, idx) => {
            const prevSection =
              idx > 0 ? pivot.columns[idx - 1]?.sectionId : null;
            const isSectionStart = prevSection !== col.sectionId;
            return (
              <TableHead
                key={col.metricKey}
                className={cn(
                  "text-end text-xs font-medium whitespace-nowrap text-muted-foreground",
                  isSectionStart && "border-l border-border"
                )}
              >
                {formatColumnHeader(col.metricKey)}
              </TableHead>
            );
          })}
        </TableRow>
      </TableHeader>
      <TableBody>
        {members.map((m) => {
          const personCells = pivot.cells.get(m.person_id);
          return (
            <TableRow
              key={m.person_id}
              className="border-b border-border hover:bg-transparent"
            >
              <TableCell className="sticky left-0 z-10 bg-card">
                <div className="text-sm font-bold text-foreground">
                  {m.name}
                </div>
                <div className="text-xs text-muted-foreground">
                  {m.seniority}
                </div>
              </TableCell>
              {pivot.columns.map((col, idx) => {
                const prevSection =
                  idx > 0 ? pivot.columns[idx - 1]?.sectionId : null;
                const isSectionStart = prevSection !== col.sectionId;
                return (
                  <TableCell
                    key={col.metricKey}
                    className={cn(
                      "text-end text-sm tabular-nums",
                      isSectionStart && "border-l border-border"
                    )}
                  >
                    {formatValue(personCells?.get(col.metricKey))}
                  </TableCell>
                );
              })}
            </TableRow>
          );
        })}
        <TableRow aria-hidden="true" className="h-full hover:bg-transparent">
          <TableCell className="sticky left-0 z-10 bg-card" />
          {pivot.columns.map((col, idx) => {
            const prevSection =
              idx > 0 ? pivot.columns[idx - 1]?.sectionId : null;
            const isSectionStart = prevSection !== col.sectionId;
            return (
              <TableCell
                key={col.metricKey}
                className={cn(isSectionStart && "border-l border-border")}
              />
            );
          })}
        </TableRow>
      </TableBody>
    </Table>
  );
}

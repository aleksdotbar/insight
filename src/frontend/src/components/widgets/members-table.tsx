import { Link } from "@tanstack/react-router";
import type { MouseEvent, ReactNode } from "react";
import { useTranslation } from "react-i18next";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { cn } from "@/lib/utils";
import type { ColumnThreshold, TeamMember } from "@/types/insight";

import { DynamicWidthBar } from "./dynamic-width-bar";
import { MetricInfo } from "./metric-info";

export interface MembersTableProps {
  members: TeamMember[];
  columnThresholds: ColumnThreshold[];
  loading: boolean;
  onCellDrill?: (personId: string, drillId: string) => void;
  onViewAllStats?: () => void;
}

function colClass(
  v: number | null,
  t: ColumnThreshold | null,
  type: "text" | "bg",
): string {
  if (v === null || t === null)
    return type === "text" ? "text-muted-foreground" : "";
  const good = t.higher_is_better ? v >= t.good : v <= t.good;
  const warn = t.higher_is_better ? v >= t.warn : v <= t.warn;
  if (good) return type === "text" ? "text-success" : "bg-success";
  if (warn) return type === "text" ? "text-warning" : "bg-warning";
  return type === "text" ? "text-destructive" : "bg-destructive";
}

function getThreshold(
  thresholds: ColumnThreshold[],
  key: string,
): ColumnThreshold | null {
  return thresholds.find((t) => t.metric_key === key) ?? null;
}

function DrillCell({
  children,
  className,
  onClick,
}: {
  children: ReactNode;
  className?: string;
  onClick: (e: MouseEvent) => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={cn(
        "hover:text-primary cursor-pointer border-none bg-transparent p-0 text-left underline decoration-dotted underline-offset-2 transition-colors",
        className,
      )}
    >
      {children}
    </button>
  );
}

function FocusBar({
  pct,
  threshold,
}: {
  pct: number | null;
  threshold: ColumnThreshold | null;
}) {
  if (pct === null)
    return <span className="text-muted-foreground text-sm">—</span>;
  return (
    <div className="flex items-center gap-1.5">
      <div className="bg-muted h-1.5 w-20 flex-shrink-0 overflow-hidden rounded-full">
        <DynamicWidthBar pct={pct} colorClass={colClass(pct, threshold, "bg")} />
      </div>
      <span className={cn("text-sm font-bold", colClass(pct, threshold, "text"))}>
        {pct}%
      </span>
    </div>
  );
}

type ColHeader = { label: string; sub: string; info?: string };

type ColumnVisibility = {
  devTime: boolean;
  prs: boolean;
  build: boolean;
  focus: boolean;
  aiTools: boolean;
  aiLoc: boolean;
};

function deriveColumnVisibility(members: TeamMember[]): ColumnVisibility {
  if (members.length === 0) {
    return {
      devTime: true,
      prs: true,
      build: true,
      focus: true,
      aiTools: true,
      aiLoc: true,
    };
  }
  return {
    devTime: members.some((m) => m.dev_time_h !== null),
    prs: members.some((m) => m.prs_merged !== null),
    build: members.some((m) => m.build_success_pct !== null),
    focus: members.some((m) => m.focus_time_pct !== null),
    aiTools: members.some((m) => m.ai_tools.length > 0),
    aiLoc: members.some((m) => m.ai_loc_share_pct !== null),
  };
}

function buildColHeaders(
  columnThresholds: ColumnThreshold[],
  cols: ColumnVisibility,
): ColHeader[] {
  const buildT = getThreshold(columnThresholds, "build_success_pct");
  const focusT = getThreshold(columnThresholds, "focus_time_pct");
  const all: (ColHeader | null)[] = [
    { label: "Name", sub: "" },
    { label: "Tasks", sub: "closed · Jira" },
    { label: "Bugs Fixed", sub: "bug-type tasks · Jira" },
    cols.devTime
      ? {
          label: "Dev Time",
          sub: "time in dev per task · lower = better",
          info: 'Average time a task spends in "In Progress" state. Lower means faster execution.',
        }
      : null,
    cols.prs ? { label: "Pull Requests", sub: "merged to main · Bitbucket" } : null,
    cols.build
      ? {
          label: "Build Success",
          sub: buildT
            ? `CI builds passing · target ≥${buildT.good}%`
            : "CI builds passing",
        }
      : null,
    cols.focus
      ? {
          label: "Focus Time",
          sub: focusT
            ? `uninterrupted work · target ≥${focusT.good}%`
            : "uninterrupted work",
        }
      : null,
    cols.aiTools ? { label: "AI Tools", sub: "active this month" } : null,
    cols.aiLoc
      ? {
          label: "AI Code Acceptance",
          sub: "Cursor + Claude Code",
          info: "Share of authored code lines accepted from AI suggestions out of total lines written in active Cursor sessions.",
        }
      : null,
  ];
  return all.filter((c): c is ColHeader => c !== null);
}

function SkeletonRow({ count }: { count: number }) {
  return (
    <TableRow>
      {Array.from({ length: count }).map((_, i) => (
        <TableCell key={i}>
          <Skeleton className="h-3.5 w-full" />
        </TableCell>
      ))}
    </TableRow>
  );
}

export function MembersTable({
  members,
  columnThresholds,
  loading,
  onCellDrill,
  onViewAllStats,
}: MembersTableProps) {
  const { t } = useTranslation();
  const drill =
    (personId: string, drillId: string) => (e: MouseEvent) => {
      e.stopPropagation();
      onCellDrill?.(personId, drillId);
    };
  const cols = deriveColumnVisibility(members);
  const colHeaders = buildColHeaders(columnThresholds, cols);
  const tBugs = getThreshold(columnThresholds, "bugs_fixed");
  const tDev = getThreshold(columnThresholds, "dev_time_h");
  const tBuild = getThreshold(columnThresholds, "build_success_pct");
  const tFocus = getThreshold(columnThresholds, "focus_time_pct");
  const tAiLoc = getThreshold(columnThresholds, "ai_loc_share_pct");

  return (
    <Card>
      <div className="border-border flex items-center justify-between border-b px-4 pt-3.5 pb-3">
        <span className="text-foreground text-sm font-bold">Team Members</span>
        <div className="flex items-center gap-3">
          {onViewAllStats ? (
            <Button variant="ghost" size="sm" onClick={onViewAllStats}>
              {t("members_table.view_team_stats")}
            </Button>
          ) : null}
          <span className="text-muted-foreground hidden text-xs sm:inline">
            {t("members_table.click_hint")}
          </span>
        </div>
      </div>
      <CardContent className="overflow-x-auto p-0">
        <Table className="min-w-max">
          <TableHeader>
            <TableRow className="bg-muted/30 border-border border-b">
              {colHeaders.map((col) => (
                <TableHead
                  key={col.label}
                  className="text-muted-foreground bg-muted/30 h-9 px-3 text-xs font-bold tracking-wide uppercase"
                >
                  <span>{col.label}</span>
                  {col.info ? <MetricInfo description={col.info} side="bottom" /> : null}
                  {col.sub ? (
                    <>
                      <br />
                      <span className="text-muted-foreground text-xs font-normal tracking-normal normal-case opacity-70">
                        {col.sub}
                      </span>
                    </>
                  ) : null}
                </TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {loading ? (
              <>
                <SkeletonRow count={colHeaders.length} />
                <SkeletonRow count={colHeaders.length} />
                <SkeletonRow count={colHeaders.length} />
              </>
            ) : (
              members.map((m) => (
                <TableRow
                  key={m.person_id}
                  className="border-border border-b hover:bg-transparent"
                >
                  <TableCell className="px-3 py-2.5">
                    <Link
                      to="/ic/$person/personal"
                      params={{ person: m.person_id }}
                      className="block"
                    >
                      <div className="text-foreground text-sm font-bold">
                        {m.name}
                      </div>
                      <div className="text-muted-foreground text-xs">
                        {m.seniority}
                      </div>
                    </Link>
                  </TableCell>
                  <TableCell className="px-3 py-2.5 text-sm">
                    {onCellDrill ? (
                      <DrillCell onClick={drill(m.person_id, "tasks-completed")}>
                        {m.tasks_closed}
                      </DrillCell>
                    ) : (
                      m.tasks_closed
                    )}
                  </TableCell>
                  <TableCell
                    className={cn(
                      "px-3 py-2.5 text-sm font-bold",
                      colClass(m.bugs_fixed, tBugs, "text"),
                    )}
                  >
                    {onCellDrill ? (
                      <DrillCell
                        onClick={drill(m.person_id, "bugs-fixed")}
                        className={colClass(m.bugs_fixed, tBugs, "text")}
                      >
                        {m.bugs_fixed ?? "—"}
                      </DrillCell>
                    ) : (
                      m.bugs_fixed ?? "—"
                    )}
                  </TableCell>
                  {cols.devTime ? (
                    <TableCell
                      className={cn(
                        "px-3 py-2.5 text-sm font-bold",
                        colClass(m.dev_time_h, tDev, "text"),
                      )}
                    >
                      {onCellDrill ? (
                        <DrillCell
                          onClick={drill(m.person_id, "cycle-time")}
                          className={colClass(m.dev_time_h, tDev, "text")}
                        >
                          {m.dev_time_h !== null ? `${m.dev_time_h}h` : "—"}
                        </DrillCell>
                      ) : m.dev_time_h !== null ? (
                        `${m.dev_time_h}h`
                      ) : (
                        "—"
                      )}
                    </TableCell>
                  ) : null}
                  {cols.prs ? (
                    <TableCell className="px-3 py-2.5 text-sm">
                      {onCellDrill ? (
                        <DrillCell onClick={drill(m.person_id, "pull-requests")}>
                          {m.prs_merged ?? "—"}
                        </DrillCell>
                      ) : (
                        m.prs_merged ?? "—"
                      )}
                    </TableCell>
                  ) : null}
                  {cols.build ? (
                    <TableCell
                      className={cn(
                        "px-3 py-2.5 text-sm font-bold",
                        colClass(m.build_success_pct, tBuild, "text"),
                      )}
                    >
                      {onCellDrill ? (
                        <DrillCell
                          onClick={drill(m.person_id, "builds")}
                          className={colClass(m.build_success_pct, tBuild, "text")}
                        >
                          {m.build_success_pct !== null
                            ? `${m.build_success_pct}%`
                            : "—"}
                        </DrillCell>
                      ) : m.build_success_pct !== null ? (
                        `${m.build_success_pct}%`
                      ) : (
                        "—"
                      )}
                    </TableCell>
                  ) : null}
                  {cols.focus ? (
                    <TableCell className="px-3 py-2.5">
                      <FocusBar pct={m.focus_time_pct} threshold={tFocus} />
                    </TableCell>
                  ) : null}
                  {cols.aiTools ? (
                    <TableCell className="px-3 py-2.5">
                      {m.ai_tools.length === 0 ? (
                        <span className="text-muted-foreground">—</span>
                      ) : (
                        <div className="flex flex-wrap gap-1">
                          {m.ai_tools.map((tool) => (
                            <Badge
                              key={tool}
                              variant="outline"
                              className="text-muted-foreground bg-muted/50 h-auto rounded px-1.5 py-0 text-xs font-bold"
                            >
                              {tool}
                            </Badge>
                          ))}
                        </div>
                      )}
                    </TableCell>
                  ) : null}
                  {cols.aiLoc ? (
                    <TableCell
                      className={cn(
                        "px-3 py-2.5 text-sm font-bold",
                        colClass(m.ai_loc_share_pct, tAiLoc, "text"),
                      )}
                    >
                      {m.ai_loc_share_pct === null ? (
                        <span className="text-muted-foreground">—</span>
                      ) : m.ai_loc_share_pct > 0 ? (
                        `${m.ai_loc_share_pct}%`
                      ) : (
                        <span className="text-muted-foreground">0%</span>
                      )}
                    </TableCell>
                  ) : null}
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}

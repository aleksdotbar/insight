import { Badge } from "@/components/ui/badge";
import { Card } from "@/components/ui/card";
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
import type { ExecColumnThreshold, ExecTeamRow } from "@/types/insight";

import { MetricInfo } from "./metric-info";

export interface TeamsTableProps {
  teams: ExecTeamRow[];
  columnThresholds: ExecColumnThreshold[];
  loading: boolean;
}

const STATUS_BADGE_CLASS: Record<ExecTeamRow["status"], string> = {
  good: "bg-success/15 text-success border-success/20",
  warn: "bg-warning/15 text-warning border-warning/20",
  bad: "bg-destructive/15 text-destructive border-destructive/20",
};

function thresholdClass(
  pct: number | null,
  metricKey: string,
  thresholds: ExecColumnThreshold[],
): string {
  if (pct === null) return "text-muted-foreground font-semibold";
  const t = thresholds.find((x) => x.metric_key === metricKey);
  if (!t) return "font-semibold";
  return pct >= t.threshold
    ? "text-success font-semibold"
    : "text-warning font-semibold";
}

function SkeletonRow() {
  return (
    <TableRow>
      {Array.from({ length: 10 }).map((_, i) => (
        <TableCell key={i}>
          <Skeleton className="h-3.5 w-full" />
        </TableCell>
      ))}
    </TableRow>
  );
}

export function TeamsTable({
  teams,
  columnThresholds,
  loading,
}: TeamsTableProps) {
  const buildT =
    columnThresholds.find((t) => t.metric_key === "build_success_pct")
      ?.threshold ?? 90;
  const focusT =
    columnThresholds.find((t) => t.metric_key === "focus_time_pct")
      ?.threshold ?? 60;
  const aiT =
    columnThresholds.find((t) => t.metric_key === "ai_adoption_pct")
      ?.threshold ?? 60;

  return (
    <Card className="overflow-x-auto">
      <Table className="min-w-max">
        <TableHeader>
          <TableRow>
            <TableHead>Team</TableHead>
            <TableHead>Headcount</TableHead>
            <TableHead>Tasks Closed</TableHead>
            <TableHead>Bugs Fixed</TableHead>
            <TableHead>
              Build %
              <MetricInfo
                description={`CI/CD builds passing. Target ≥${buildT}%.`}
                side="bottom"
              />
            </TableHead>
            <TableHead>
              Focus %
              <MetricInfo
                description={`Work time in uninterrupted 60-min+ blocks. Target ≥${focusT}%.`}
                side="bottom"
              />
            </TableHead>
            <TableHead>
              AI Adoption %
              <MetricInfo
                description={`Share of members actively using any AI tool this period. Target ≥${aiT}%.`}
                side="bottom"
              />
            </TableHead>
            <TableHead>
              AI LOC %
              <MetricInfo
                description="Share of authored code lines accepted from AI suggestions (Cursor + Claude Code)."
                side="bottom"
              />
            </TableHead>
            <TableHead>
              PR Cycle
              <MetricInfo
                description="Average time from PR opened to merged, in hours. Lower is better."
                side="bottom"
              />
            </TableHead>
            <TableHead>Status</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {loading ? (
            <>
              <SkeletonRow />
              <SkeletonRow />
              <SkeletonRow />
            </>
          ) : (
            teams.map((team) => (
              <TableRow key={team.team_id}>
                <TableCell className="font-semibold">{team.team_name}</TableCell>
                <TableCell>{team.headcount}</TableCell>
                <TableCell>{team.tasks_closed ?? "—"}</TableCell>
                <TableCell>{team.bugs_fixed ?? "—"}</TableCell>
                <TableCell
                  className={thresholdClass(
                    team.build_success_pct,
                    "build_success_pct",
                    columnThresholds,
                  )}
                >
                  {team.build_success_pct !== null
                    ? `${team.build_success_pct}%`
                    : "—"}
                </TableCell>
                <TableCell
                  className={thresholdClass(
                    team.focus_time_pct,
                    "focus_time_pct",
                    columnThresholds,
                  )}
                >
                  {team.focus_time_pct !== null ? `${team.focus_time_pct}%` : "—"}
                </TableCell>
                <TableCell
                  className={thresholdClass(
                    team.ai_adoption_pct,
                    "ai_adoption_pct",
                    columnThresholds,
                  )}
                >
                  {team.ai_adoption_pct !== null
                    ? `${team.ai_adoption_pct}%`
                    : "—"}
                </TableCell>
                <TableCell>
                  {team.ai_loc_share_pct !== null
                    ? `${team.ai_loc_share_pct}%`
                    : "—"}
                </TableCell>
                <TableCell>
                  {team.pr_cycle_time_h !== null
                    ? `${team.pr_cycle_time_h}h`
                    : "—"}
                </TableCell>
                <TableCell>
                  <Badge
                    variant="outline"
                    className={cn("capitalize", STATUS_BADGE_CLASS[team.status])}
                  >
                    {team.status}
                  </Badge>
                </TableCell>
              </TableRow>
            ))
          )}
        </TableBody>
      </Table>
    </Card>
  );
}

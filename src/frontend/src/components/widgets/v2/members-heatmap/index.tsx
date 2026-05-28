import { useMemo, useState } from "react";
import { ArrowDown, ArrowUp } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { useSettings } from "@/hooks/use-settings";
import { BULLET_DEFS_BY_KEY } from "@/lib/insight/v2/bullet-defs";
import { peerStatusForRow } from "@/lib/insight/v2/peer-status";
import {
  applyFocus,
  PEER_CELL,
  PEER_FILL,
  PEER_LABEL,
  PEER_TEXT,
  peerStatusVsQuartiles,
  type FocusMode,
  type PeerStats,
  type PeerStatusWithNeutral,
} from "@/lib/peers";
import { cn } from "@/lib/utils";
import type { BulletMetric, TeamMember } from "@/types/insight";

import {
  MemberDetailsSheet,
  type MemberDetailRow,
} from "./member-details-sheet";
import { TriageList, type TriageRow } from "./triage-list";

const WOW_THRESHOLD = 0.05;

function computeWowPct(
  current: number | null,
  previous: number | null,
): number | null {
  if (current == null || previous == null) return null;
  if (!Number.isFinite(current) || !Number.isFinite(previous)) return null;
  if (Math.abs(previous) < 1e-9) return null;
  return (current - previous) / previous;
}

type TeamRowKey =
  | "tasks_closed"
  | "prs_merged"
  | "build_success_pct"
  | "bugs_fixed"
  | "focus_time_pct"
  | "ai_loc_share_pct";

interface BaseColumn {
  key: string;
  label: string;
  short: string;
  unit: string;
  higher_is_better: boolean;
  mobile: boolean;
}

interface TeamRowColumn extends BaseColumn {
  source: "team_row";
  teamRowField: TeamRowKey;
}

interface BulletColumn extends BaseColumn {
  source: "bullet";
  metricKey: string;
}

type ColumnDef = TeamRowColumn | BulletColumn;

const COLUMNS: ColumnDef[] = [
  {
    key: "tasks_closed",
    label: "Tasks closed",
    short: "Tasks",
    unit: "",
    higher_is_better: true,
    mobile: true,
    source: "team_row",
    teamRowField: "tasks_closed",
  },
  {
    key: "mean_time_to_resolution",
    label: "Mean time to resolution",
    short: "MTTR",
    unit: "d",
    higher_is_better: false,
    mobile: true,
    source: "bullet",
    metricKey: "mean_time_to_resolution",
  },
  {
    key: "prs_merged",
    label: "PRs merged",
    short: "PRs",
    unit: "",
    higher_is_better: true,
    mobile: true,
    source: "team_row",
    teamRowField: "prs_merged",
  },
  {
    key: "pr_cycle_time_h",
    label: "PR cycle time",
    short: "Cycle",
    unit: "h",
    higher_is_better: false,
    mobile: true,
    source: "bullet",
    metricKey: "pr_cycle_time_h",
  },
  {
    key: "build_success_pct",
    label: "Build success",
    short: "Build %",
    unit: "%",
    higher_is_better: true,
    mobile: false,
    source: "team_row",
    teamRowField: "build_success_pct",
  },
  {
    key: "bugs_fixed",
    label: "Bugs fixed",
    short: "Bugs",
    unit: "",
    higher_is_better: true,
    mobile: false,
    source: "team_row",
    teamRowField: "bugs_fixed",
  },
  {
    key: "focus_time_pct",
    label: "Focus time",
    short: "Focus %",
    unit: "%",
    higher_is_better: true,
    mobile: false,
    source: "team_row",
    teamRowField: "focus_time_pct",
  },
  {
    key: "ai_loc_share_pct",
    label: "AI lines share",
    short: "AI %",
    unit: "%",
    higher_is_better: true,
    mobile: false,
    source: "team_row",
    teamRowField: "ai_loc_share_pct",
  },
  {
    key: "meeting_hours",
    label: "Meeting hours",
    short: "Mtg h",
    unit: "h",
    higher_is_better: false,
    mobile: false,
    source: "bullet",
    metricKey: "meeting_hours",
  },
];

function getNumericTeamRow(m: TeamMember, key: TeamRowKey): number | null {
  const raw = m[key];
  return typeof raw === "number" && Number.isFinite(raw) ? raw : null;
}

function getNumericBullet(
  rows: BulletMetric[] | undefined,
  metricKey: string,
): number | null {
  const r = rows?.find((b) => b.metric_key === metricKey);
  if (!r) return null;
  const n = Number(r.value);
  return Number.isFinite(n) ? n : null;
}

function valueForColumn(
  col: ColumnDef,
  member: TeamMember,
  bullets: BulletMetric[] | undefined,
): number | null {
  if (col.source === "team_row") {
    return getNumericTeamRow(member, col.teamRowField);
  }
  return getNumericBullet(bullets, col.metricKey);
}

type SortKey = "name" | "issues" | string;

export interface MembersHeatmapProps {
  members: TeamMember[];
  bulletsByPerson?: Map<string, BulletMetric[]>;
  previousBulletsByPerson?: Map<string, BulletMetric[]>;
  previousMembers?: Map<string, TeamMember>;
  cohortStats?: Map<string, PeerStats>;
  onMemberClick?: (m: TeamMember) => void;
}

function lookupCohortKey(col: ColumnDef): string {
  return col.source === "team_row" ? col.key : col.metricKey;
}

export function MembersHeatmap({
  members,
  bulletsByPerson,
  previousBulletsByPerson,
  previousMembers,
  cohortStats,
  onMemberClick,
}: MembersHeatmapProps) {
  const { focusMode } = useSettings();
  const [sortKey, setSortKey] = useState<SortKey>("issues");
  const [sheetMember, setSheetMember] = useState<TeamMember | null>(null);
  const [expandedId, setExpandedId] = useState<string | null>(null);

  const statsByColumn = useMemo(() => {
    const map = new Map<ColumnDef["key"], PeerStats | null>();
    for (const col of COLUMNS) {
      map.set(col.key, cohortStats?.get(lookupCohortKey(col)) ?? null);
    }
    return map;
  }, [cohortStats]);

  const rows = useMemo(() => {
    const built = members.map((m) => {
      const personIdKey = m.person_id.toLowerCase();
      const bullets = bulletsByPerson?.get(personIdKey);
      const prevBullets = previousBulletsByPerson?.get(personIdKey);
      const prevMember = previousMembers?.get(personIdKey);
      const cells = COLUMNS.map((col) => {
        const value = valueForColumn(col, m, bullets);
        const previous = prevMember
          ? valueForColumn(col, prevMember, prevBullets)
          : null;
        const stats = statsByColumn.get(col.key);
        const status: PeerStatusWithNeutral =
          value !== null && stats
            ? peerStatusVsQuartiles(value, stats, col.higher_is_better)
            : "neutral";
        return {
          col,
          value,
          previous,
          status,
          median: stats?.p50 ?? null,
        };
      });
      const belowCount = cells.filter((c) => c.status === "bottom").length;
      const topCount = cells.filter((c) => c.status === "top").length;
      const worstBullet =
        (bullets ?? [])
          .map((b) => {
            const value = Number(b.value);
            const stats = cohortStats?.get(b.metric_key);
            const def = BULLET_DEFS_BY_KEY[b.metric_key];
            const higherIsBetter = def?.higher_is_better ?? true;
            let gap = 0;
            if (
              stats &&
              Number.isFinite(value) &&
              Math.abs(stats.p50) > 1e-9
            ) {
              const raw = (value - stats.p50) / Math.abs(stats.p50);
              gap = higherIsBetter ? raw : -raw;
            }
            return {
              bullet: b,
              ps: peerStatusForRow(b, cohortStats),
              gap,
            };
          })
          .filter((e) => e.ps === "bottom")
          .sort((a, b) => a.gap - b.gap)[0]?.bullet ?? null;
      return {
        member: m,
        bullets: bullets ?? [],
        cells,
        belowCount,
        topCount,
        worstMetricLabel: worstBullet?.label ?? null,
      };
    });
    return built;
  }, [members, bulletsByPerson, previousBulletsByPerson, previousMembers, statsByColumn, cohortStats]);

  const sortedRows = useMemo(() => {
    const copy = [...rows];
    if (sortKey === "name") {
      copy.sort((a, b) => a.member.name.localeCompare(b.member.name));
    } else if (sortKey === "issues") {
      copy.sort(
        (a, b) =>
          b.belowCount - a.belowCount ||
          a.member.name.localeCompare(b.member.name),
      );
    } else {
      const colIdx = COLUMNS.findIndex((c) => c.key === sortKey);
      const col = COLUMNS[colIdx];
      if (col) {
        copy.sort((a, b) => {
          const av = a.cells[colIdx]?.value ?? Number.POSITIVE_INFINITY;
          const bv = b.cells[colIdx]?.value ?? Number.POSITIVE_INFINITY;
          return col.higher_is_better ? bv - av : av - bv;
        });
      }
    }
    return copy;
  }, [rows, sortKey]);

  const gridStyle = {
    gridTemplateColumns: `minmax(140px, max-content) repeat(${COLUMNS.length}, minmax(56px, 1fr))`,
  };

  const triageRows: TriageRow[] = sortedRows.map((r) => ({
    member: r.member,
    cells: r.cells.map((c) => ({
      label: c.col.label,
      short: c.col.short,
      value: c.value,
      status: c.status,
    })),
    belowCount: r.belowCount,
    topCount: r.topCount,
  }));

  const sheetRows: MemberDetailRow[] = useMemo(() => {
    if (!sheetMember) return [];
    const row = rows.find((r) => r.member.person_id === sheetMember.person_id);
    if (!row) return [];
    return row.cells.map((c) => ({
      label: c.col.label,
      short: c.col.short,
      value: c.value,
      unit: c.col.unit,
      status: c.status,
      median: c.median,
    }));
  }, [rows, sheetMember]);

  const handleMemberClick = (m: TeamMember) => {
    setSheetMember(m);
    onMemberClick?.(m);
  };

  return (
    <Card>
      <CardHeader className="gap-1">
        <CardTitle>Members × metrics</CardTitle>
        <p className="text-xs text-muted-foreground">
          {members.length} members · cell colour = position vs team
        </p>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        <div className="flex flex-wrap items-center gap-2">
          <span className="text-xs text-muted-foreground">Sort:</span>
          <Button
            size="sm"
            variant={sortKey === "issues" ? "default" : "outline"}
            onClick={() => setSortKey("issues")}
          >
            Most issues
          </Button>
          <Button
            size="sm"
            variant={sortKey === "name" ? "default" : "outline"}
            onClick={() => setSortKey("name")}
          >
            Name
          </Button>
        </div>
        <div className="sm:hidden">
          <TriageList rows={triageRows} onMemberClick={handleMemberClick} />
        </div>
        <div className="hidden overflow-x-auto sm:block">
          <div className="inline-grid min-w-full gap-1" style={gridStyle}>
            <div aria-hidden />
            {COLUMNS.map((c) => (
              <ColumnHeader
                key={c.key}
                col={c}
                active={sortKey === c.key}
                onClick={() => setSortKey(c.key)}
              />
            ))}
            {sortedRows.map((row) => (
              <MemberRow
                key={row.member.person_id}
                row={row}
                focusMode={focusMode}
                cohortStats={cohortStats}
                expanded={expandedId === row.member.person_id}
                onToggleExpand={() =>
                  setExpandedId((cur) =>
                    cur === row.member.person_id ? null : row.member.person_id,
                  )
                }
                onOpenSheet={() => handleMemberClick(row.member)}
              />
            ))}
          </div>
          <Legend />
        </div>
      </CardContent>
      <MemberDetailsSheet
        member={sheetMember}
        rows={sheetRows}
        onOpenChange={(open) => {
          if (!open) setSheetMember(null);
        }}
      />
    </Card>
  );
}

interface CellShape {
  col: ColumnDef;
  value: number | null;
  previous: number | null;
  status: PeerStatusWithNeutral;
  median: number | null;
}

interface RowShape {
  member: TeamMember;
  bullets: BulletMetric[];
  cells: CellShape[];
  belowCount: number;
  topCount: number;
  worstMetricLabel: string | null;
}

function HeatmapCell({
  cell,
  memberName,
  focusMode,
}: {
  cell: CellShape;
  memberName: string;
  focusMode: FocusMode;
}) {
  const focused = applyFocus(cell.status, focusMode);
  const { col, value, previous, median } = cell;
  const wowPct = computeWowPct(value, previous);
  const showWow = wowPct != null && Math.abs(wowPct) >= WOW_THRESHOLD;
  const wowUp = wowPct != null && wowPct > 0;
  const improving = showWow && wowUp === col.higher_is_better;
  const WowIcon = wowUp ? ArrowUp : ArrowDown;
  const wowTint = improving
    ? PEER_TEXT[applyFocus("top", focusMode)]
    : PEER_TEXT[applyFocus("bottom", focusMode)];
  const display =
    value == null
      ? "—"
      : `${Math.round(value)}${col.unit ?? ""}`;
  return (
    <Popover>
      <PopoverTrigger
        render={
          <button
            type="button"
            aria-label={`${memberName} — ${col.label}: ${display} — ${PEER_LABEL[focused]}`}
            className={cn(
              "flex h-12 items-center justify-center gap-1 rounded-sm text-sm font-medium tabular-nums transition hover:brightness-95",
              PEER_CELL[focused],
            )}
          >
            <span>{display}</span>
            {showWow && value != null ? (
              <WowIcon className={cn("size-3 shrink-0", wowTint)} aria-hidden />
            ) : null}
          </button>
        }
      />
      <PopoverContent className="w-64 p-3">
        <div className="flex flex-col gap-1">
          <p className="text-sm font-semibold">{col.label}</p>
          <p className="text-xs text-muted-foreground">{memberName}</p>
          <p
            className={cn(
              "mt-2 text-2xl font-semibold tabular-nums",
              PEER_TEXT[focused],
            )}
          >
            {display}
          </p>
          <p className="text-xs text-muted-foreground">
            {median != null
              ? `Team median: ${Math.round(median * 10) / 10}${col.unit ?? ""}`
              : "no peer data"}
          </p>
          <p className={cn("mt-1 text-xs font-medium", PEER_TEXT[focused])}>
            {PEER_LABEL[focused]}
          </p>
          {showWow && previous != null ? (
            <p className="mt-1 text-xs text-muted-foreground tabular-nums">
              week-over-week:{" "}
              <span className={cn("font-medium", wowTint)}>
                {wowUp ? "+" : ""}
                {Math.round((wowPct ?? 0) * 100)}%
              </span>{" "}
              (was {Math.round(previous)}
              {col.unit ?? ""})
            </p>
          ) : null}
        </div>
      </PopoverContent>
    </Popover>
  );
}

function MemberRow({
  row,
  focusMode,
  cohortStats,
  expanded,
  onToggleExpand,
  onOpenSheet,
}: {
  row: RowShape;
  focusMode: FocusMode;
  cohortStats?: Map<string, PeerStats>;
  expanded: boolean;
  onToggleExpand: () => void;
  onOpenSheet: () => void;
}) {
  const { member, cells, belowCount, topCount, worstMetricLabel, bullets } = row;
  const issueText =
    belowCount > 0
      ? `${belowCount} issue${belowCount === 1 ? "" : "s"}`
      : "on par";
  return (
    <>
      <div className="flex min-h-14 flex-col justify-center gap-0.5 px-2 py-1">
        <div className="flex items-center gap-1.5">
          <Popover>
            <PopoverTrigger
              render={
                <button
                  type="button"
                  className="truncate text-left text-sm font-medium leading-tight hover:underline"
                  aria-expanded={expanded}
                >
                  {member.name}
                </button>
              }
            />
            <PopoverContent className="w-64 p-3">
              <p className="truncate text-sm font-semibold">{member.name}</p>
              {member.seniority ? (
                <p className="truncate text-xs text-muted-foreground">
                  {member.seniority}
                </p>
              ) : null}
              <p className="mt-2 text-xs text-muted-foreground">
                {belowCount} below peers · {topCount} in top
              </p>
              <div className="mt-3 flex flex-col gap-1.5">
                <Button size="sm" onClick={onOpenSheet}>
                  Open in IC view
                </Button>
                <Button size="sm" variant="outline" onClick={onToggleExpand}>
                  {expanded ? "Collapse details" : "Expand details"}
                </Button>
              </div>
            </PopoverContent>
          </Popover>
          <span
            className={cn(
              "rounded-sm px-1.5 py-0.5 text-xs font-medium",
              belowCount > 0
                ? PEER_CELL[applyFocus("bottom", focusMode)]
                : PEER_CELL[applyFocus("in_pack", focusMode)],
            )}
          >
            {issueText}
          </span>
        </div>
        {belowCount > 0 && worstMetricLabel ? (
          <p className="truncate text-[11px] leading-tight text-muted-foreground">
            worst: {worstMetricLabel}
          </p>
        ) : null}
      </div>
      {cells.map((cell) => (
        <HeatmapCell
          key={cell.col.key}
          cell={cell}
          memberName={member.name}
          focusMode={focusMode}
        />
      ))}
      {expanded ? (
        <ExpandedBullets
          bullets={bullets}
          columnCount={cells.length}
          focusMode={focusMode}
          cohortStats={cohortStats}
        />
      ) : null}
    </>
  );
}

function ExpandedBullets({
  bullets,
  columnCount,
  focusMode,
  cohortStats,
}: {
  bullets: BulletMetric[];
  columnCount: number;
  focusMode: FocusMode;
  cohortStats?: Map<string, PeerStats>;
}) {
  const RANK: Record<PeerStatusWithNeutral, number> = {
    bottom: 0,
    in_pack: 1,
    top: 2,
    neutral: 3,
  };
  const annotated = bullets.map((b) => ({
    bullet: b,
    status: peerStatusForRow(b, cohortStats),
  }));
  annotated.sort((a, b) => RANK[a.status] - RANK[b.status]);
  return (
    <div
      className="border-t border-border/60 px-3 py-3"
      style={{ gridColumn: `1 / span ${columnCount + 1}` }}
    >
      <p className="mb-2 text-xs font-medium text-muted-foreground">
        All metrics
      </p>
      <div className="grid grid-cols-1 gap-1.5 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4">
        {annotated.map(({ bullet: b, status }) => {
          const focused = applyFocus(status, focusMode);
          return (
            <span
              key={b.metric_key}
              className={cn(
                "flex items-center justify-between gap-2 rounded-sm border border-border/40 px-2 py-1.5 text-xs",
                PEER_CELL[focused],
              )}
            >
              <span className="truncate font-medium" title={b.label}>
                {b.label}
              </span>
              <span className="shrink-0 font-mono tabular-nums">
                {b.value}
                {b.unit ? ` ${b.unit}` : ""}
              </span>
            </span>
          );
        })}
      </div>
    </div>
  );
}

function ColumnHeader({
  col,
  active,
  onClick,
}: {
  col: ColumnDef;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <Popover>
      <PopoverTrigger
        render={
          <button
            type="button"
            onClick={onClick}
            className={cn(
              "flex h-9 cursor-help items-center justify-center text-xs font-medium uppercase tracking-wider text-muted-foreground transition-colors hover:text-foreground",
              active && "text-foreground underline underline-offset-4",
            )}
            aria-label={`${col.label} — sort by this column`}
          >
            <span className="truncate">{col.short}</span>
          </button>
        }
      />
      <PopoverContent className="w-64 p-3">
        <p className="text-sm font-semibold">{col.label}</p>
        <p className="mt-0.5 text-xs text-muted-foreground">
          Unit: {col.unit || "—"} ·{" "}
          {col.higher_is_better ? "higher is better" : "lower is better"}
        </p>
        <p className="mt-2 text-xs text-muted-foreground">Click to sort.</p>
      </PopoverContent>
    </Popover>
  );
}

function Legend() {
  return (
    <div className="mt-4 flex flex-wrap items-center gap-4 text-xs text-muted-foreground">
      <LegendSwatch className={PEER_FILL.top}>top 25%</LegendSwatch>
      <LegendSwatch className={PEER_FILL.in_pack}>on par</LegendSwatch>
      <LegendSwatch className={PEER_FILL.bottom}>bottom 25%</LegendSwatch>
      <LegendSwatch className={PEER_FILL.neutral}>no peer data</LegendSwatch>
    </div>
  );
}

function LegendSwatch({
  className,
  children,
}: {
  className: string;
  children: React.ReactNode;
}) {
  return (
    <span className="inline-flex items-center gap-1.5">
      <span className={cn("inline-block size-3 rounded-sm", className)} />
      {children}
    </span>
  );
}


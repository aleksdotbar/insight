import { useEffect, useMemo, useState } from "react";

import { ComingSoon } from "@/components/widgets/coming-soon";
import { PeriodSelectorBar } from "@/components/widgets/period-selector-bar";
import { DashboardEmptyState } from "@/components/widgets/v2/dashboard-empty-state";
import { SectionCard } from "@/components/widgets/v2/section-card";
import { SectionDrilldownSheet } from "@/components/widgets/v2/section-drilldown-sheet";
import { MembersHeatmap } from "@/components/widgets/v2/members-heatmap";
import { SectionStatus } from "@/components/widgets/v2/section-status";
import { TeamMembersAttention } from "@/components/widgets/v2/team-members-attention";
import { SidebarTrigger } from "@/components/ui/sidebar";
import { Spinner } from "@/components/ui/spinner";
import { cn } from "@/lib/utils";
import { usePeriod } from "@/hooks/use-period";
import {
  flattenSubordinates,
  findIdentityNode,
} from "@/lib/insight/identity-tree";
import {
  TEAM_SECTIONS,
  type TeamSectionId,
} from "@/lib/insight/v2/sections";
import { orderRowsForSection } from "@/lib/insight/v2/metric-order";
import { hasBulletValue } from "@/lib/insight/v2/peer-status";
import { useIcPerson } from "@/queries/ic-dashboard";
import {
  useTeamBulletSection,
  useTeamMembers,
} from "@/queries/team-view";
import {
  useTeamMemberBullets,
  useTeamMemberBulletsPrevious,
} from "@/queries/v2/team-extras";
import { useIcCohortStats } from "@/queries/v2/ic-extras";
import type { PeerStats } from "@/lib/peers";
import type { BulletMetric, TeamMember } from "@/types/insight";

export interface TeamViewV2ScreenProps {
  teamId: string;
  viewerEmail: string;
}

export function TeamViewV2Screen({ teamId, viewerEmail }: TeamViewV2ScreenProps) {
  const { period, customRange, dateRange, setPeriod, setCustomRange } =
    usePeriod();
  const [openSection, setOpenSection] = useState<TeamSectionId | null>(null);
  const [, setFocusedMember] = useState<TeamMember | null>(null);

  useEffect(() => {
    setOpenSection(null);
  }, [teamId]);

  const viewerQ = useIcPerson(viewerEmail);
  const viewerTree = viewerQ.data ?? null;

  const pivot = useMemo(() => {
    if (!viewerTree) return null;
    if (teamId.includes("@")) return findIdentityNode(viewerTree, teamId);
    return null;
  }, [viewerTree, teamId]);

  const roster = useMemo(
    () => (pivot ? flattenSubordinates(pivot) : null),
    [pivot],
  );
  const teamName = pivot?.display_name ?? teamId;
  const teamSize = roster?.length;

  const membersQ = useTeamMembers(teamId, roster, period, dateRange, {
    keepPrevious: true,
  });
  const members = membersQ.data ?? [];
  const memberIds = useMemo(
    () => members.map((m) => m.person_id),
    [members],
  );
  const bulletsQ = useTeamMemberBullets(memberIds, period, dateRange);
  const prevBulletsQ = useTeamMemberBulletsPrevious(
    memberIds,
    period,
    dateRange,
  );

  const taskQ = useTeamBulletSection(
    "task_delivery",
    teamId,
    teamSize,
    period,
    dateRange,
    { keepPrevious: true },
  );
  const qualityQ = useTeamBulletSection(
    "code_quality",
    teamId,
    teamSize,
    period,
    dateRange,
    { keepPrevious: true },
  );
  const collabQ = useTeamBulletSection(
    "collaboration",
    teamId,
    teamSize,
    period,
    dateRange,
    { keepPrevious: true },
  );
  const aiQ = useTeamBulletSection(
    "ai_adoption",
    teamId,
    teamSize,
    period,
    dateRange,
    { keepPrevious: true },
  );

  const cohortStatsQ = useIcCohortStats("team", teamId, dateRange);
  const cohortStatsByKey = useMemo<Map<string, PeerStats>>(() => {
    const m = new Map<string, PeerStats>();
    for (const row of cohortStatsQ.data ?? []) {
      m.set(row.metric_key, {
        p25: row.p25,
        p50: row.p50,
        p75: row.p75,
        min: row.min,
        max: row.max,
        n: row.n,
      });
    }
    return m;
  }, [cohortStatsQ.data]);

  const cohortSize = cohortStatsQ.data?.[0]?.n ?? 0;

  const rowsBySection: Record<TeamSectionId, BulletMetric[]> = {
    task_delivery: orderRowsForSection("task_delivery", taskQ.data ?? []),
    code_quality: orderRowsForSection("code_quality", qualityQ.data ?? []),
    collaboration: orderRowsForSection("collaboration", collabQ.data ?? []),
    ai_adoption: orderRowsForSection("ai_adoption", aiQ.data ?? []),
  };

  const heroSections = TEAM_SECTIONS.map((s) => ({
    id: s.id,
    label: s.label,
    rows: rowsBySection[s.id],
  }));

  const sectionQByKey = {
    task_delivery: taskQ,
    code_quality: qualityQ,
    collaboration: collabQ,
    ai_adoption: aiQ,
  } as const;
  const heroErrored =
    taskQ.isError || qualityQ.isError || collabQ.isError || aiQ.isError;
  const sectionsPending =
    taskQ.isPending ||
    qualityQ.isPending ||
    collabQ.isPending ||
    aiQ.isPending;
  const sectionsFetching =
    taskQ.isFetching ||
    qualityQ.isFetching ||
    collabQ.isFetching ||
    aiQ.isFetching;
  const isFetching =
    sectionsFetching || membersQ.isFetching || bulletsQ.isFetching;
  const hasSectionData = Object.values(rowsBySection).some((rows) =>
    rows.some(hasBulletValue),
  );
  const hasMembers = members.length > 0;
  const isAllEmpty =
    !sectionsPending &&
    !membersQ.isPending &&
    !hasSectionData &&
    !hasMembers;
  const showFullSpinner =
    sectionsPending || membersQ.isPending || (isAllEmpty && isFetching);

  return (
    <div className="flex flex-col">
      <header className="sticky top-0 z-20 flex flex-wrap items-center justify-between gap-3 border-b bg-background/95 px-4 py-3 backdrop-blur-sm">
        <div className="flex items-center gap-2">
          <SidebarTrigger />
          <div className="flex flex-col">
            <h1 className="text-xl font-semibold tracking-tight">
              Team of {teamName}
            </h1>
            <p className="text-xs text-muted-foreground">
              {members.length} member{members.length === 1 ? "" : "s"}
            </p>
          </div>
        </div>
        <PeriodSelectorBar
          period={period}
          customRange={customRange}
          onPeriodChange={setPeriod}
          onRangeChange={setCustomRange}
        />
      </header>
      <main className="flex flex-1 flex-col gap-8 p-4 md:p-6">
        {showFullSpinner ? (
          <div className="flex min-h-[70vh] items-center justify-center">
            <Spinner className="size-12 text-muted-foreground" />
          </div>
        ) : isAllEmpty ? (
          <div
            className={cn("transition-opacity", isFetching && "opacity-60")}
          >
            <DashboardEmptyState period={period} onSetPeriod={setPeriod} />
          </div>
        ) : (
          <div
            className={cn(
              "flex flex-col gap-8 transition-opacity",
              isFetching && "opacity-60",
            )}
          >
            {heroErrored ? (
              <ComingSoon
                state="error"
                onRetry={() => {
                  void taskQ.refetch();
                  void qualityQ.refetch();
                  void collabQ.refetch();
                  void aiQ.refetch();
                }}
              />
            ) : (
              <>
                <TeamMembersAttention
                  members={members}
                  bulletsByPerson={bulletsQ.data}
                  cohortStats={cohortStatsByKey}
                  cohortSize={cohortSize}
                  onMemberClick={setFocusedMember}
                />
                <SectionStatus
                  sections={heroSections}
                  peerLabel="other teams"
                  cols="four"
                  cohortStats={cohortStatsByKey}
                  onSectionClick={setOpenSection}
                />
              </>
            )}

            {membersQ.isError ? (
              <ComingSoon
                state="error"
                label="Heatmap — unable to load"
                onRetry={() => membersQ.refetch()}
              />
            ) : (
              <MembersHeatmap
                members={members}
                bulletsByPerson={bulletsQ.data}
                previousBulletsByPerson={prevBulletsQ.data}
                cohortStats={cohortStatsByKey}
                onMemberClick={setFocusedMember}
              />
            )}

            <section className="flex flex-col gap-3">
              <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
                Sections
              </p>
              <div className="grid grid-cols-1 gap-3 md:grid-cols-2 lg:grid-cols-4">
                {TEAM_SECTIONS.map((s) => {
                  const q = sectionQByKey[s.id];
                  if (q.isError) {
                    return (
                      <ComingSoon
                        key={s.id}
                        state="error"
                        label={`${s.label} — unable to load`}
                        onRetry={() => q.refetch()}
                      />
                    );
                  }
                  return (
                    <SectionCard
                      key={s.id}
                      title={s.label}
                      sectionId={s.id}
                      rows={rowsBySection[s.id]}
                      cohortStats={cohortStatsByKey}
                      onOpen={() => setOpenSection(s.id)}
                      subtitle="team aggregate · vs other teams"
                    />
                  );
                })}
              </div>
            </section>
          </div>
        )}
      </main>

      <SectionDrilldownSheet
        open={openSection !== null}
        onOpenChange={(open) => {
          if (!open) setOpenSection(null);
        }}
        title={
          openSection
            ? (TEAM_SECTIONS.find((s) => s.id === openSection)?.label ?? "")
            : ""
        }
        rows={openSection ? rowsBySection[openSection] : []}
        cohortStats={cohortStatsByKey}
        cohortLabel="team"
      />
    </div>
  );
}

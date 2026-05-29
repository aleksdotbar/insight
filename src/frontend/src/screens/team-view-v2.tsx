import { useEffect, useMemo, useState } from "react";

import { ComingSoon } from "@/components/widgets/coming-soon";
import { DashboardEmptyState } from "@/components/widgets/v2/dashboard-empty-state";
import { DashboardHeader } from "@/components/widgets/v2/dashboard-header";
import { SectionCard } from "@/components/widgets/v2/section-card";
import { SectionDrilldownSheet } from "@/components/widgets/v2/section-drilldown-sheet";
import { MembersHeatmap } from "@/components/widgets/v2/members-heatmap";
import { SectionStatus } from "@/components/widgets/v2/section-status";
import { TeamMembersAttention } from "@/components/widgets/v2/team-members-attention";
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
  useTeamBulletSections,
  useTeamMembers,
} from "@/queries/team-view";
import {
  useTeamMemberBullets,
  useTeamMemberBulletsPrevious,
} from "@/queries/v2/team-extras";
import { useIcCohortStats } from "@/queries/v2/ic-extras";
import type { PeerStats } from "@/lib/peers";
import type { BulletMetric, TeamMember } from "@/types/insight";

const SECTION_KEYS = TEAM_SECTIONS.map((s) => s.id);

export interface TeamViewV2ScreenProps {
  teamId: string;
  viewerEmail: string;
}

export function TeamViewV2Screen({ teamId, viewerEmail }: TeamViewV2ScreenProps) {
  const { period, dateRange, setPeriod } = usePeriod();
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

  const sectionsQ = useTeamBulletSections(
    SECTION_KEYS,
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

  const sectionData = sectionsQ.data;
  const rowsBySection: Record<TeamSectionId, BulletMetric[]> = {
    task_delivery: orderRowsForSection(
      "task_delivery",
      sectionData?.bySection.task_delivery ?? [],
    ),
    code_quality: orderRowsForSection(
      "code_quality",
      sectionData?.bySection.code_quality ?? [],
    ),
    collaboration: orderRowsForSection(
      "collaboration",
      sectionData?.bySection.collaboration ?? [],
    ),
    ai_adoption: orderRowsForSection(
      "ai_adoption",
      sectionData?.bySection.ai_adoption ?? [],
    ),
  };

  const heroSections = TEAM_SECTIONS.map((s) => ({
    id: s.id,
    label: s.label,
    rows: rowsBySection[s.id],
  }));

  const sectionsPending = sectionsQ.isPending;
  const sectionsFetching = sectionsQ.isFetching;
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
      <DashboardHeader
        title={`Team of ${teamName}`}
        subtitle={`${members.length} member${members.length === 1 ? "" : "s"}`}
        person={teamId}
        hasReports
      />
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
                  if (sectionsQ.isError || sectionData?.errors[s.id]) {
                    return (
                      <SectionCard
                        key={s.id}
                        title={s.label}
                        sectionId={s.id}
                        rows={[]}
                        onOpen={() => {}}
                        unavailable
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

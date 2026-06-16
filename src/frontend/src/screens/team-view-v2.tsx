import { useMemo, useState } from "react";

import { ComingSoon } from "@/components/widgets/coming-soon";
import { DashboardEmptyState } from "@/components/widgets/v2/dashboard-empty-state";
import { DashboardHeader } from "@/components/widgets/v2/dashboard-header";
import { SectionCard } from "@/components/widgets/v2/section-card";
import { SectionDrilldownSheet } from "@/components/widgets/v2/section-drilldown-sheet";
import { MembersHeatmap } from "@/components/widgets/v2/members-heatmap";
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
  useDeptDistributions,
  useTeamMemberBullets,
  useTeamMemberBulletsPrevious,
} from "@/queries/v2/team-extras";
import type { BulletMetric } from "@/types/insight";

const SECTION_KEYS = TEAM_SECTIONS.map((s) => s.id);

export interface TeamViewV2ScreenProps {
  teamId: string;
  viewerEmail: string;
}

export function TeamViewV2Screen({ teamId, viewerEmail }: TeamViewV2ScreenProps) {
  const { period, dateRange, setPeriod } = usePeriod();
  const [openSection, setOpenSection] = useState<TeamSectionId | null>(null);

  // Close any open drilldown when the viewed team changes. Render-phase
  // reset against the previous id rather than an effect (no cascading commit).
  const [prevTeamId, setPrevTeamId] = useState(teamId);
  if (teamId !== prevTeamId) {
    setPrevTeamId(teamId);
    setOpenSection(null);
  }

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
  const memberIds = members.map((m) => m.person_id);
  const bulletsQ = useTeamMemberBullets(memberIds, period, dateRange);
  const prevBulletsQ = useTeamMemberBulletsPrevious(
    memberIds,
    period,
    dateRange,
  );

  const orgUnitIds = [
    ...new Set(
      members
        .map((m) => m.org_unit_id)
        .filter((id): id is string => Boolean(id)),
    ),
  ];
  const deptDistQ = useDeptDistributions(orgUnitIds, period, dateRange);

  const sectionsQ = useTeamBulletSections(
    SECTION_KEYS,
    teamId,
    teamSize,
    period,
    dateRange,
    { keepPrevious: true, roster },
  );

  const sectionData = sectionsQ.data;
  const rowsBySection: Record<TeamSectionId, BulletMetric[]> = {
    task_delivery: orderRowsForSection(
      "task_delivery",
      sectionData?.bySection.task_delivery ?? [],
    ),
    git_output: orderRowsForSection(
      "git_output",
      sectionData?.bySection.git_output ?? [],
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
              deptCohorts={deptDistQ.data}
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
                deptCohorts={deptDistQ.data}
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
                      onOpen={() => setOpenSection(s.id)}
                      subtitle="vs department expectation"
                    />
                  );
                })}
              </div>
            </section>
          </div>
        )}
      </main>

      {TEAM_SECTIONS.map((s) => (
        <SectionDrilldownSheet
          key={s.id}
          open={openSection === s.id}
          onOpenChange={(o) => setOpenSection(o ? s.id : null)}
          title={s.label}
          rows={rowsBySection[s.id]}
          cohortLabel="department"
        />
      ))}
    </div>
  );
}

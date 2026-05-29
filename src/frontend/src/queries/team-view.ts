import { keepPreviousData, useQuery, type UseQueryResult } from "@tanstack/react-query";

import {
  queryBatchWithRange,
  queryMetric,
  type BatchQueryItem,
  type BatchQueryResult,
} from "@/api/analytics-client";
import { METRIC_REGISTRY } from "@/api/metric-registry";
import { odataEscapeValue } from "@/api/odata";
import type { DateRange } from "@/api/period-to-date-range";
import type {
  RawBulletAggregateRow,
  RawDrillRow,
  RawTeamMemberRow,
} from "@/api/raw-types";
import {
  transformBulletMetrics,
  transformDrill,
  transformTeamMembers,
} from "@/api/transforms";
import type { RosterEntry } from "@/lib/insight/identity-tree";
import type {
  BulletMetric,
  DrillData,
  PeriodValue,
  TeamMember,
} from "@/types/insight";

function buildSyntheticMember(
  entry: RosterEntry,
  period: PeriodValue,
): TeamMember {
  return {
    person_id: entry.email,
    period,
    name: entry.display_name,
    seniority: "",
    supervisor_email: entry.supervisor_email,
    tasks_closed: 0,
    bugs_fixed: 0,
    dev_time_h: null,
    prs_merged: null,
    build_success_pct: null,
    focus_time_pct: null,
    ai_tools: [],
    ai_loc_share_pct: null,
  };
}

export function useTeamMembers(
  teamId: string,
  roster: RosterEntry[] | null,
  period: PeriodValue,
  range: DateRange,
  options?: { keepPrevious?: boolean },
): UseQueryResult<TeamMember[]> {
  const rosterKey = roster ? roster.map((r) => r.email).join(",") : null;
  return useQuery({
    queryKey: [
      "team",
      "members",
      teamId,
      rosterKey,
      period,
      range.from,
      range.to,
    ],
    enabled: Boolean(teamId),
    placeholderData: options?.keepPrevious ? keepPreviousData : undefined,
    queryFn: async () => {
      if (roster) {
        const resp = await queryBatchWithRange<RawTeamMemberRow>(
          range,
          roster.map((r) => ({
            id: r.email.toLowerCase(),
            metric_id: METRIC_REGISTRY.TEAM_MEMBER,
            $filter: `person_id eq '${odataEscapeValue(r.email.toLowerCase())}'`,
            $top: 1,
          })),
        );
        const okResults = resp.results.filter(
          (r): r is Extract<typeof r, { status: "ok" }> => r.status === "ok",
        );
        if (resp.results.length > 0 && okResults.length === 0) {
          throw new Error("TEAM_MEMBER batch returned no successful items");
        }
        const byEmail = new Map<string, RawTeamMemberRow>();
        for (const r of okResults) {
          for (const row of r.items) {
            byEmail.set(row.person_id.toLowerCase(), row);
          }
        }
        return roster.map((entry) => {
          const row = byEmail.get(entry.email.toLowerCase());
          return row
            ? transformTeamMembers([row], period)[0]!
            : buildSyntheticMember(entry, period);
        });
      }
      const resp = await queryMetric<RawTeamMemberRow>(
        METRIC_REGISTRY.TEAM_MEMBER,
        range,
        {
          $filter: `org_unit_id eq '${odataEscapeValue(teamId)}'`,
          $orderby: "display_name asc",
          $top: 200,
        },
      );
      const members = transformTeamMembers(resp.items, period);
      members.sort((a, b) => a.name.localeCompare(b.name));
      return members;
    },
  });
}

const TEAM_BULLET_SECTIONS = {
  task_delivery: METRIC_REGISTRY.TEAM_BULLET_DELIVERY,
  code_quality: METRIC_REGISTRY.TEAM_BULLET_QUALITY,
  estimation: METRIC_REGISTRY.TEAM_BULLET_DELIVERY,
  collaboration: METRIC_REGISTRY.TEAM_BULLET_COLLAB,
  ai_adoption: METRIC_REGISTRY.TEAM_BULLET_AI,
} as const;

export type TeamBulletSectionId = keyof typeof TEAM_BULLET_SECTIONS;

export function useTeamBulletSection(
  sectionId: TeamBulletSectionId,
  teamId: string,
  teamSize: number | undefined,
  period: PeriodValue,
  range: DateRange,
  options?: { keepPrevious?: boolean },
): UseQueryResult<BulletMetric[]> {
  const metricId = TEAM_BULLET_SECTIONS[sectionId];
  const scopeId = teamId.includes("@") ? teamId.toLowerCase() : teamId;
  return useQuery({
    queryKey: [
      "team",
      "bullet",
      sectionId,
      scopeId,
      teamSize,
      period,
      range.from,
      range.to,
    ],
    enabled: Boolean(teamId),
    placeholderData: options?.keepPrevious ? keepPreviousData : undefined,
    queryFn: async () => {
      const resp = await queryMetric<RawBulletAggregateRow>(metricId, range, {
        $filter: `org_unit_id eq '${odataEscapeValue(scopeId)}'`,
      });
      return transformBulletMetrics(
        resp.items,
        sectionId,
        period,
        teamSize,
        "team",
      );
    },
  });
}

export interface TeamBulletSectionsData {
  bySection: Record<string, BulletMetric[]>;
  errors: Record<string, boolean>;
}

export function useTeamBulletSections(
  sectionIds: readonly TeamBulletSectionId[],
  teamId: string,
  teamSize: number | undefined,
  period: PeriodValue,
  range: DateRange,
  options?: { keepPrevious?: boolean },
): UseQueryResult<TeamBulletSectionsData> {
  const scopeId = teamId.includes("@") ? teamId.toLowerCase() : teamId;
  return useQuery({
    queryKey: [
      "team",
      "bullet-batch",
      sectionIds.join(","),
      scopeId,
      teamSize,
      period,
      range.from,
      range.to,
    ],
    enabled: Boolean(teamId),
    placeholderData: options?.keepPrevious ? keepPreviousData : undefined,
    queryFn: async () => {
      const items: BatchQueryItem[] = sectionIds.map((id) => ({
        id,
        metric_id: TEAM_BULLET_SECTIONS[id],
        $filter: `org_unit_id eq '${odataEscapeValue(scopeId)}'`,
      }));
      const resp = await queryBatchWithRange<RawBulletAggregateRow>(
        range,
        items,
      );
      const byId = new Map<string, BatchQueryResult<RawBulletAggregateRow>>();
      for (const r of resp.results) {
        if (r.id) byId.set(r.id, r);
      }
      const bySection: Record<string, BulletMetric[]> = {};
      const errors: Record<string, boolean> = {};
      for (const id of sectionIds) {
        const r = byId.get(id);
        if (r && r.status === "ok") {
          errors[id] = false;
          bySection[id] = transformBulletMetrics(
            r.items,
            id,
            period,
            teamSize,
            "team",
          );
        } else {
          errors[id] = true;
          bySection[id] = [];
        }
      }
      return { bySection, errors };
    },
  });
}

export type TeamDrillTarget =
  | { kind: "team"; teamId: string; drillId: string }
  | { kind: "cell"; personId: string; drillId: string };

export function useTeamDrill(
  target: TeamDrillTarget | null,
  range: DateRange,
): UseQueryResult<DrillData | null> {
  const key =
    target == null
      ? null
      : target.kind === "team"
        ? `team:${target.teamId}:${target.drillId}`
        : `cell:${target.personId}:${target.drillId}`;
  return useQuery({
    queryKey: ["team", "drill", key, range.from, range.to],
    enabled: Boolean(target),
    queryFn: async () => {
      if (!target) return null;
      const filter =
        target.kind === "team"
          ? `org_unit_id eq '${odataEscapeValue(target.teamId)}' and drill_id eq '${odataEscapeValue(target.drillId)}'`
          : `person_id eq '${odataEscapeValue(target.personId.toLowerCase())}' and drill_id eq '${odataEscapeValue(target.drillId)}'`;
      const resp = await queryMetric<RawDrillRow>(
        METRIC_REGISTRY.IC_DRILL,
        range,
        { $filter: filter },
      );
      return resp.items[0] ? transformDrill(resp.items[0]) : null;
    },
  });
}

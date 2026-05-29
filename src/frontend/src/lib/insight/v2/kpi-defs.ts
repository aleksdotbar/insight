import type { IcSectionId } from "./sections";

export type IcKpiFormat = "integer" | "decimal1" | "percent" | "hours";

export interface IcKpiDef {
  metric_key: string;
  label: string;
  unit: string;
  description: string;
  higher_is_better: boolean;
  format: IcKpiFormat;
  section: IcSectionId;
}

export const IC_KPI_DEFS: IcKpiDef[] = [
  {
    metric_key: "bugs_fixed",
    label: "Bugs fixed",
    unit: "",
    description: "Bug-type issues closed",
    higher_is_better: true,
    format: "integer",
    section: "code_quality",
  },
  {
    metric_key: "ai_loc_share",
    label: "AI code acceptance",
    unit: "%",
    description: "Authored lines accepted from AI",
    higher_is_better: true,
    format: "percent",
    section: "ai_adoption",
  },
  {
    metric_key: "focus_time_pct",
    label: "Focus time",
    unit: "%",
    description: "Time in uninterrupted 60-min blocks",
    higher_is_better: true,
    format: "percent",
    section: "collaboration",
  },
  {
    metric_key: "tasks_closed",
    label: "Tasks closed",
    unit: "",
    description: "Tasks moved to Done",
    higher_is_better: true,
    format: "integer",
    section: "task_delivery",
  },
  {
    metric_key: "prs_merged",
    label: "PRs merged",
    unit: "",
    description: "PRs authored and merged",
    higher_is_better: true,
    format: "integer",
    section: "git_output",
  },
  {
    metric_key: "pr_cycle_time_h",
    label: "PR cycle time",
    unit: "h",
    description: "Avg hours from PR open to merge",
    higher_is_better: false,
    format: "hours",
    section: "git_output",
  },
  {
    metric_key: "ai_sessions",
    label: "AI sessions",
    unit: "",
    description: "Distinct AI coding sessions",
    higher_is_better: true,
    format: "integer",
    section: "ai_adoption",
  },
];

export const IC_KPI_DEFS_BY_KEY: Record<string, IcKpiDef> = Object.fromEntries(
  IC_KPI_DEFS.map((d) => [d.metric_key, d]),
);

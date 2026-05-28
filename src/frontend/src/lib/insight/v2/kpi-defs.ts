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
    description: "Bug-type issues closed in the selected period.",
    higher_is_better: true,
    format: "integer",
    section: "code_quality",
  },
  {
    metric_key: "ai_loc_share",
    label: "AI code acceptance",
    unit: "%",
    description: "Share of authored lines accepted from AI suggestions.",
    higher_is_better: true,
    format: "percent",
    section: "ai_adoption",
  },
  {
    metric_key: "focus_time_pct",
    label: "Focus time",
    unit: "%",
    description:
      "Share of work time in uninterrupted 60-minute or longer blocks.",
    higher_is_better: true,
    format: "percent",
    section: "collaboration",
  },
  {
    metric_key: "tasks_closed",
    label: "Tasks closed",
    unit: "",
    description: "Tasks moved to Done in the selected period.",
    higher_is_better: true,
    format: "integer",
    section: "task_delivery",
  },
  {
    metric_key: "prs_merged",
    label: "PRs merged",
    unit: "",
    description: "Pull requests authored and merged.",
    higher_is_better: true,
    format: "integer",
    section: "git_output",
  },
  {
    metric_key: "pr_cycle_time_h",
    label: "PR cycle time",
    unit: "h",
    description: "Average hours from pull request opened to merged.",
    higher_is_better: false,
    format: "hours",
    section: "git_output",
  },
  {
    metric_key: "ai_sessions",
    label: "AI sessions",
    unit: "",
    description: "Distinct AI-assisted coding sessions.",
    higher_is_better: true,
    format: "integer",
    section: "ai_adoption",
  },
];

export const IC_KPI_DEFS_BY_KEY: Record<string, IcKpiDef> = Object.fromEntries(
  IC_KPI_DEFS.map((d) => [d.metric_key, d]),
);

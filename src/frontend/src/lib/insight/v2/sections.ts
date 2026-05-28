export type IcSectionId =
  | "task_delivery"
  | "git_output"
  | "code_quality"
  | "collaboration"
  | "ai_adoption";

export type IcHeroSectionId = IcSectionId;

export type TeamSectionId =
  | "task_delivery"
  | "code_quality"
  | "collaboration"
  | "ai_adoption";

export const IC_SECTIONS: ReadonlyArray<{ id: IcSectionId; label: string }> = [
  { id: "task_delivery", label: "Task delivery" },
  { id: "git_output", label: "Git output" },
  { id: "code_quality", label: "Code quality" },
  { id: "collaboration", label: "Collaboration" },
  { id: "ai_adoption", label: "AI dev tools" },
] as const;

export const IC_HERO_SECTIONS: ReadonlyArray<{
  id: IcHeroSectionId;
  label: string;
}> = IC_SECTIONS;

export const TEAM_SECTIONS: ReadonlyArray<{
  id: TeamSectionId;
  label: string;
}> = [
  { id: "task_delivery", label: "Task delivery" },
  { id: "code_quality", label: "Code quality" },
  { id: "collaboration", label: "Collaboration" },
  { id: "ai_adoption", label: "AI adoption" },
] as const;

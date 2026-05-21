import type { ReactElement } from "react";

import { filterBulletsByLayoutGroup } from "@/api/threshold-config";
import { Card, CardContent } from "@/components/ui/card";
import type { BulletMetric } from "@/types/insight";

import { BulletChart } from "./bullet-chart";
import { CollapsibleSection } from "./collapsible-section";
import { ComingSoon } from "./coming-soon";

type SectionId =
  | "task_delivery"
  | "code_quality"
  | "estimation"
  | "ai_adoption"
  | "collaboration";

export interface TeamBulletSectionsProps {
  sections: Record<SectionId, BulletMetric[] | undefined>;
  status: Record<SectionId, "loading" | "loaded" | "errored">;
  errors?: Partial<Record<SectionId, string>>;
  viewMode: "chart" | "tile";
  onDrillClick?: (drillId: string) => void;
  onRetry?: (sectionId: SectionId) => void;
}

function Legend() {
  return (
    <div className="text-muted-foreground mb-2.5 flex items-center gap-3 text-xs">
      <span className="flex items-center gap-1">
        <span className="bg-foreground/60 inline-block h-3 w-[2px] rounded" />
        Company median
      </span>
      <span className="flex items-center gap-1">
        <span className="from-success via-warning to-destructive inline-block h-1.5 w-4 rounded bg-gradient-to-r" />
        Team result · color = vs target
      </span>
    </div>
  );
}

function TwoColCard({
  title,
  subtitle,
  metrics,
  onDrillClick,
}: {
  title: string;
  subtitle: string;
  metrics: BulletMetric[];
  onDrillClick?: (id: string) => void;
}) {
  const left = metrics.filter((_, i) => i % 2 === 0);
  const right = metrics.filter((_, i) => i % 2 !== 0);
  return (
    <Card>
      <CardContent className="px-4 py-3.5">
        <div className="mb-1 flex items-center justify-between">
          <span className="text-foreground text-sm font-bold">{title}</span>
          <span className="text-muted-foreground text-xs">{subtitle}</span>
        </div>
        <Legend />
        <div className="grid grid-cols-1 gap-3.5 sm:grid-cols-2">
          <div className="flex flex-col gap-4">
            {left.map((m) => (
              <BulletChart
                key={m.metric_key}
                metric={m}
                onDrillClick={onDrillClick}
                mode="chart"
              />
            ))}
          </div>
          <div className="flex flex-col gap-4">
            {right.map((m) => (
              <BulletChart
                key={m.metric_key}
                metric={m}
                onDrillClick={onDrillClick}
                mode="chart"
              />
            ))}
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

const ESTIMATION_GROUPS: Array<{ label: string; group: string }> = [
  { label: "1 · Time estimate accuracy", group: "estimate_accuracy" },
  { label: "2 · Sprint scope", group: "sprint_scope" },
  { label: "3 · Deadline (date-driven)", group: "deadline" },
];

function EstimationCard({
  metrics,
  onDrillClick,
}: {
  metrics: BulletMetric[];
  onDrillClick?: (id: string) => void;
}) {
  return (
    <Card>
      <CardContent className="px-4 py-3.5">
        <div className="mb-1 flex items-center justify-between">
          <span className="text-foreground text-sm font-bold">Estimation</span>
          <span className="text-muted-foreground text-xs">
            Team median vs company median · Source: Jira
          </span>
        </div>
        <Legend />
        <div className="grid grid-cols-1 gap-3.5 sm:grid-cols-2 md:grid-cols-3">
          {ESTIMATION_GROUPS.map(({ label, group }) => {
            const groupMetrics = filterBulletsByLayoutGroup(metrics, group);
            return (
              <div key={label}>
                <div className="text-muted-foreground mb-1.5 text-xs font-semibold">
                  {label}
                </div>
                <div className="flex flex-col gap-4">
                  {groupMetrics.map((m) => (
                    <BulletChart
                      key={m.metric_key}
                      metric={m}
                      onDrillClick={onDrillClick}
                      mode="chart"
                    />
                  ))}
                </div>
              </div>
            );
          })}
        </div>
      </CardContent>
    </Card>
  );
}

const AI_RIGHT_GROUPS = ["ai_team_output", "ai_acceptance"] as const;

function AiAdoptionSection({
  metrics,
  onDrillClick,
}: {
  metrics: BulletMetric[];
  onDrillClick?: (id: string) => void;
}) {
  const leftMetrics = filterBulletsByLayoutGroup(metrics, "ai_members");
  const rightMetrics = AI_RIGHT_GROUPS.flatMap((g) =>
    filterBulletsByLayoutGroup(metrics, g),
  );
  return (
    <CollapsibleSection
      title="AI Adoption"
      storageKey="insight:team-view:ai-adoption"
    >
      <div className="px-4 py-3">
        <div className="text-muted-foreground mb-2.5 text-xs font-bold tracking-wide uppercase">
          Cursor · Claude Code · Codex
        </div>
        <Legend />
        <div className="grid grid-cols-1 gap-3.5 sm:grid-cols-2">
          <div className="flex flex-col gap-4">
            {leftMetrics.map((m) => (
              <BulletChart
                key={m.metric_key}
                metric={m}
                onDrillClick={onDrillClick}
                mode="chart"
              />
            ))}
          </div>
          <div className="flex flex-col gap-4">
            {rightMetrics.map((m) => (
              <BulletChart
                key={m.metric_key}
                metric={m}
                onDrillClick={onDrillClick}
                mode="chart"
              />
            ))}
          </div>
        </div>
      </div>
    </CollapsibleSection>
  );
}

function CollaborationSection({
  metrics,
  onDrillClick,
}: {
  metrics: BulletMetric[];
  onDrillClick?: (id: string) => void;
}) {
  const chatMetrics = filterBulletsByLayoutGroup(metrics, "chat");
  const emailMetrics = filterBulletsByLayoutGroup(metrics, "email");
  const meetingsMetrics = filterBulletsByLayoutGroup(metrics, "meetings");
  const filesMetrics = filterBulletsByLayoutGroup(metrics, "files");

  function renderColumn(heading: string, items: BulletMetric[]): ReactElement {
    return (
      <div className="flex flex-col gap-4">
        <div className="text-muted-foreground text-xs font-bold tracking-wide uppercase">
          {heading}
        </div>
        {items.map((m) => (
          <BulletChart
            key={m.metric_key}
            metric={m}
            onDrillClick={onDrillClick}
            mode="chart"
          />
        ))}
      </div>
    );
  }

  return (
    <CollapsibleSection
      title="Collaboration"
      storageKey="insight:team-view:collaboration"
    >
      <div className="px-4 py-3">
        <Legend />
        <div className="mt-2 grid grid-cols-1 gap-x-3.5 gap-y-2 sm:grid-cols-2 md:grid-cols-4">
          {renderColumn("Chat", chatMetrics)}
          {renderColumn("Email", emailMetrics)}
          {renderColumn("Meetings", meetingsMetrics)}
          {renderColumn("Files", filesMetrics)}
        </div>
      </div>
    </CollapsibleSection>
  );
}

function SectionPlaceholder({
  title,
  kind,
  onRetry,
}: {
  title: string;
  kind: "loading" | "errored";
  onRetry?: () => void;
}) {
  return (
    <Card>
      <CardContent className="px-4 py-3.5">
        <div className="text-foreground mb-2 text-sm font-bold">{title}</div>
        <ComingSoon
          variant="card"
          state={kind === "loading" ? "loading" : "error"}
          onRetry={onRetry}
        />
      </CardContent>
    </Card>
  );
}

export function TeamBulletSections({
  sections,
  status,
  viewMode: _viewMode,
  onDrillClick,
  onRetry,
}: TeamBulletSectionsProps) {
  const taskDelivery = sections.task_delivery;
  const codeQuality = sections.code_quality;
  const estimation = sections.estimation;
  const aiAdoption = sections.ai_adoption;
  const collab = sections.collaboration;

  function renderSection(
    sid: SectionId,
    title: string,
    body: () => ReactElement,
  ): ReactElement {
    const st = status[sid];
    if (st === "loading")
      return <SectionPlaceholder title={title} kind="loading" />;
    if (st === "errored")
      return (
        <SectionPlaceholder
          title={title}
          kind="errored"
          onRetry={onRetry ? () => onRetry(sid) : undefined}
        />
      );
    return body();
  }

  return (
    <div className="flex flex-col gap-3.5">
      <div className="grid grid-cols-1 gap-3.5 lg:grid-cols-2">
        {renderSection("task_delivery", "Task Delivery", () => (
          <TwoColCard
            title="Task Delivery"
            subtitle="Team median vs company median"
            metrics={taskDelivery ?? []}
            onDrillClick={onDrillClick}
          />
        ))}
        {renderSection("code_quality", "Code & Quality", () => (
          <TwoColCard
            title="Code & Quality"
            subtitle="Team median vs company median"
            metrics={codeQuality ?? []}
            onDrillClick={onDrillClick}
          />
        ))}
      </div>

      {renderSection("estimation", "Estimation", () => (
        <EstimationCard
          metrics={estimation ?? []}
          onDrillClick={onDrillClick}
        />
      ))}

      {renderSection("ai_adoption", "AI Adoption", () => (
        <AiAdoptionSection
          metrics={aiAdoption ?? []}
          onDrillClick={onDrillClick}
        />
      ))}

      {renderSection("collaboration", "Collaboration", () => (
        <CollaborationSection
          metrics={collab ?? []}
          onDrillClick={onDrillClick}
        />
      ))}
    </div>
  );
}

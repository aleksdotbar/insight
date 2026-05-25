import { Link } from "@tanstack/react-router";
import { AlertTriangle, ArrowRight, CircleAlert } from "lucide-react";
import type { ComponentType, SVGProps } from "react";

import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { cn } from "@/lib/utils";
import type { AlertThreshold, TeamMember } from "@/types/insight";

type Severity = "bad" | "warn";

type AlertItem = {
  member: TeamMember;
  title: string;
  description: string;
  severity: Severity;
};

const SEVERITY_ICON: Record<Severity, ComponentType<SVGProps<SVGSVGElement>>> = {
  bad: CircleAlert,
  warn: AlertTriangle,
};
const SEVERITY_ICON_CLASS: Record<Severity, string> = {
  bad: "bg-destructive/15 text-destructive",
  warn: "bg-warning/15 text-warning",
};

const METRIC_LABEL: Record<string, { label: string; unit: string }> = {
  build_success_pct: { label: "Build Success Rate", unit: "%" },
  focus_time_pct: { label: "Focus Time", unit: "%" },
  ai_loc_share_pct: { label: "AI Code Acceptance", unit: "%" },
};

function buildDescription(
  metricKey: string,
  m: TeamMember,
  trigger: number,
): string {
  const meta = METRIC_LABEL[metricKey] ?? { label: metricKey, unit: "" };
  const val = m[metricKey as keyof TeamMember] as number;
  const base = `${meta.label} is ${val}${meta.unit} vs ${trigger}${meta.unit} target.`;
  if (metricKey === "focus_time_pct")
    return `${base} ${m.tasks_closed} tasks completed this period.`;
  if (metricKey === "build_success_pct")
    return m.prs_merged !== null
      ? `${base} ${m.prs_merged} PRs merged this period.`
      : base;
  if (metricKey === "ai_loc_share_pct")
    return `${base} ${
      m.ai_tools.length === 0
        ? "No AI tools active."
        : `Active tools: ${m.ai_tools.join(", ")}.`
    }`;
  return base;
}

function computeAlerts(
  members: TeamMember[],
  alertThresholds: AlertThreshold[],
): AlertItem[] {
  const alerts: AlertItem[] = [];
  for (const m of members) {
    for (const rule of alertThresholds) {
      const value = m[rule.metric_key as keyof TeamMember];
      if (typeof value !== "number" || !Number.isFinite(value)) continue;
      if (value < rule.trigger) {
        const severity: Severity = value < rule.bad ? "bad" : "warn";
        alerts.push({
          member: m,
          title: `${m.name} — ${rule.reason}`,
          description: buildDescription(rule.metric_key, m, rule.trigger),
          severity,
        });
      }
    }
  }
  return alerts;
}

export interface AttentionNeededProps {
  members: TeamMember[];
  alertThresholds: AlertThreshold[];
}

export function AttentionNeeded({
  members,
  alertThresholds,
}: AttentionNeededProps) {
  const alerts = computeAlerts(members, alertThresholds);
  if (alerts.length === 0) return null;

  return (
    <Card>
      <div className="px-4 pt-3.5 pb-0">
        <span className="text-foreground text-sm font-bold">
          Attention Needed
        </span>
      </div>
      <CardContent className="px-4 py-3">
        {alerts.map((alert, i) => (
          <div
            key={`${alert.member.person_id}-${i}`}
            className={cn(
              "flex items-start gap-3 py-3",
              i < alerts.length - 1 ? "border-border border-b" : "",
            )}
          >
            <div
              className={cn(
                "flex h-8 w-8 flex-shrink-0 items-center justify-center rounded-lg",
                SEVERITY_ICON_CLASS[alert.severity],
              )}
            >
              {(() => {
                const Icon = SEVERITY_ICON[alert.severity];
                return <Icon className="size-4" />;
              })()}
            </div>
            <div className="min-w-0 flex-1">
              <div className="text-foreground mb-0.5 text-sm font-bold">
                {alert.title}
              </div>
              <div className="text-muted-foreground text-xs">
                {alert.description}
              </div>
              <Button
                variant="link"
                size="sm"
                className="text-primary mt-1 h-auto gap-1 p-0 text-xs font-semibold"
                render={
                  <Link
                    to="/ic/$person/personal"
                    params={{ person: alert.member.person_id }}
                  />
                }
              >
                <ArrowRight className="size-3" />
                Open IC dashboard
              </Button>
            </div>
          </div>
        ))}
      </CardContent>
    </Card>
  );
}

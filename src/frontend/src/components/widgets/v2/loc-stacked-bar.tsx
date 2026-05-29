import { Bar, BarChart, CartesianGrid, XAxis, YAxis } from "recharts";

import { ComingSoon } from "@/components/widgets/coming-soon";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  ChartContainer,
  ChartLegend,
  ChartLegendContent,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "@/components/ui/chart";
import { Skeleton } from "@/components/ui/skeleton";
import type { LocDataPoint } from "@/types/insight";

const CHART_CONFIG: ChartConfig = {
  codeLoc: { label: "Clean LOC", color: "var(--chart-1)" },
  aiLoc: { label: "AI accepted", color: "var(--chart-2)" },
  specLines: { label: "Spec", color: "var(--chart-3)" },
};

export interface LocStackedBarProps {
  title?: string;
  description?: string;
  data: LocDataPoint[];
  isPending?: boolean;
  isError?: boolean;
  onRetry?: () => void;
}

export function LocStackedBar({
  title = "LOC breakdown",
  description = "Daily lines added · clean · spec · AI-accepted",
  data,
  isPending,
  isError,
  onRetry,
}: LocStackedBarProps) {
  if (isPending) {
    return <Skeleton className="h-56 w-full rounded-lg" />;
  }
  if (isError) {
    return (
      <ComingSoon
        state="error"
        label={`${title} — unable to load`}
        onRetry={onRetry}
      />
    );
  }
  if (data.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-semibold">{title}</CardTitle>
          <CardDescription>No LOC data yet.</CardDescription>
        </CardHeader>
      </Card>
    );
  }
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-semibold">{title}</CardTitle>
        <CardDescription className="text-xs">{description}</CardDescription>
      </CardHeader>
      <CardContent>
        <ChartContainer config={CHART_CONFIG} className="h-56 w-full">
          <BarChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
            <XAxis
              dataKey="label"
              tick={{ fontSize: 10, fill: "var(--muted-foreground)" }}
              tickLine={false}
              axisLine={false}
              interval="preserveStartEnd"
            />
            <YAxis
              tick={{ fontSize: 10, fill: "var(--muted-foreground)" }}
              tickLine={false}
              axisLine={false}
            />
            <ChartTooltip content={<ChartTooltipContent />} />
            <ChartLegend content={<ChartLegendContent />} />
            <Bar
              dataKey="codeLoc"
              stackId="a"
              fill="var(--color-codeLoc)"
              name="Clean LOC"
            />
            <Bar
              dataKey="aiLoc"
              stackId="a"
              fill="var(--color-aiLoc)"
              name="AI accepted"
            />
            <Bar
              dataKey="specLines"
              stackId="a"
              fill="var(--color-specLines)"
              name="Spec"
            />
          </BarChart>
        </ChartContainer>
      </CardContent>
    </Card>
  );
}

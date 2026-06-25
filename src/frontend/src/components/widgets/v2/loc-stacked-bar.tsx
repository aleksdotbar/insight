import { ComingSoon } from "@/components/widgets/coming-soon";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Bar,
  BarChart,
  CartesianGrid,
  ChartContainer,
  ChartLegend,
  ChartLegendContent,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
  XAxis,
  YAxis,
} from "@/components/ui/chart";
import { Skeleton } from "@/components/ui/skeleton";
import type { LocDataPoint } from "@/types/insight";

const CHART_CONFIG: ChartConfig = {
  codeLoc: { label: "Clean LOC", color: "var(--chart-1)" },
  specLines: { label: "Spec", color: "var(--chart-2)" },
  configLoc: { label: "Config", color: "var(--chart-3)" },
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
  description = "Lines added · clean · spec · config",
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
              dataKey="specLines"
              stackId="a"
              fill="var(--color-specLines)"
              name="Spec"
            />
            <Bar
              dataKey="configLoc"
              stackId="a"
              fill="var(--color-configLoc)"
              name="Config"
            />
          </BarChart>
        </ChartContainer>
      </CardContent>
    </Card>
  );
}

import {
  Bar,
  BarChart,
  CartesianGrid,
  ReferenceLine,
  XAxis,
  YAxis,
} from "recharts";

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
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "@/components/ui/chart";
import { Skeleton } from "@/components/ui/skeleton";
import type { HistogramBin } from "@/queries/v2/ic-extras";

const CHART_CONFIG: ChartConfig = {
  count: { label: "Count", color: "var(--chart-1)" },
};

export interface HistogramStripProps {
  title: string;
  unit?: string;
  bins: HistogramBin[];
  median?: number;
  isPending?: boolean;
  isError?: boolean;
  onRetry?: () => void;
}

export function HistogramStrip({
  title,
  unit,
  bins,
  median,
  isPending,
  isError,
  onRetry,
}: HistogramStripProps) {
  if (isPending) {
    return <Skeleton className="h-40 w-full rounded-lg" />;
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
  if (bins.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-semibold">{title}</CardTitle>
          <CardDescription>No distribution data yet.</CardDescription>
        </CardHeader>
      </Card>
    );
  }
  const data = bins.map((b) => ({
    bucket: `${b.bin}${unit ?? ""}`,
    bin: b.bin,
    count: b.count,
  }));
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-semibold">{title}</CardTitle>
        {median != null ? (
          <CardDescription className="text-xs">
            Median {median}
            {unit ?? ""}
          </CardDescription>
        ) : null}
      </CardHeader>
      <CardContent>
        <ChartContainer config={CHART_CONFIG} className="h-32 w-full">
          <BarChart data={data} margin={{ top: 4, right: 4, left: 0, bottom: 0 }}>
            <CartesianGrid vertical={false} stroke="var(--border)" />
            <XAxis
              dataKey="bucket"
              tickLine={false}
              axisLine={false}
              tick={{ fontSize: 10, fill: "var(--muted-foreground)" }}
            />
            <YAxis hide />
            <ChartTooltip content={<ChartTooltipContent hideIndicator />} />
            <Bar dataKey="count" fill="var(--color-count)" radius={3} />
            {median != null ? (
              <ReferenceLine
                x={`${median}${unit ?? ""}`}
                stroke="var(--foreground)"
                strokeDasharray="3 3"
              />
            ) : null}
          </BarChart>
        </ChartContainer>
      </CardContent>
    </Card>
  );
}

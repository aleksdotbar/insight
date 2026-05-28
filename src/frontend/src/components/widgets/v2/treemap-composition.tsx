import { Treemap } from "recharts";

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
import type { CompositionRow } from "@/queries/v2/ic-extras";

const CHART_CONFIG: ChartConfig = {
  value: { label: "Value", color: "var(--chart-1)" },
};

const PALETTE = [
  "var(--chart-1)",
  "var(--chart-2)",
  "var(--chart-3)",
  "var(--chart-4)",
  "var(--chart-5)",
];

interface TreemapNodeProps {
  x?: number;
  y?: number;
  width?: number;
  height?: number;
  index?: number;
  name?: string;
  value?: number;
}

function TreemapCell({
  x = 0,
  y = 0,
  width = 0,
  height = 0,
  index = 0,
  name,
  value,
}: TreemapNodeProps) {
  const showLabel = width > 60 && height > 28;
  return (
    <g>
      <rect
        x={x}
        y={y}
        width={width}
        height={height}
        fill={PALETTE[index % PALETTE.length]}
        stroke="var(--background)"
        strokeWidth={2}
        rx={4}
      />
      {showLabel && name ? (
        <text
          x={x + 8}
          y={y + 18}
          fontSize={11}
          fontWeight={600}
          fill="var(--background)"
        >
          {name}
        </text>
      ) : null}
      {showLabel && value != null && height > 44 ? (
        <text
          x={x + 8}
          y={y + 34}
          fontSize={11}
          fill="var(--background)"
          opacity={0.85}
        >
          {value}
        </text>
      ) : null}
    </g>
  );
}

export interface TreemapCompositionProps {
  title: string;
  description?: string;
  rows: CompositionRow[];
  isPending?: boolean;
  isError?: boolean;
  onRetry?: () => void;
}

export function TreemapComposition({
  title,
  description,
  rows,
  isPending,
  isError,
  onRetry,
}: TreemapCompositionProps) {
  if (isPending) {
    return <Skeleton className="h-48 w-full rounded-lg" />;
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
  if (rows.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-semibold">{title}</CardTitle>
          <CardDescription>No composition data yet.</CardDescription>
        </CardHeader>
      </Card>
    );
  }
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-semibold">{title}</CardTitle>
        {description ? (
          <CardDescription className="text-xs">{description}</CardDescription>
        ) : null}
      </CardHeader>
      <CardContent>
        <ChartContainer config={CHART_CONFIG} className="h-48 w-full">
          <Treemap
            data={rows as unknown as Record<string, unknown>[]}
            dataKey="value"
            nameKey="name"
            stroke="var(--background)"
            content={<TreemapCell />}
          >
            <ChartTooltip
              content={<ChartTooltipContent hideIndicator />}
            />
          </Treemap>
        </ChartContainer>
      </CardContent>
    </Card>
  );
}

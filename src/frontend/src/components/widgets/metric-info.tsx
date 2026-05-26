import { Info } from "lucide-react";

import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";

interface MetricInfoProps {
  description: string;
  side?: "top" | "bottom" | "left" | "right";
}

export function MetricInfo({ description, side = "top" }: MetricInfoProps) {
  return (
    <TooltipProvider delay={200}>
      <Tooltip>
        <TooltipTrigger
          render={
            <span
              tabIndex={0}
              role="button"
              className="text-muted-foreground focus-visible:ring-ring ml-1 inline-flex cursor-help items-center rounded-sm leading-none select-none focus-visible:ring-1 focus-visible:outline-none"
            >
              <Info className="size-3" />
            </span>
          }
        />
        <TooltipContent side={side} className="max-w-56 text-xs leading-snug">
          {description}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

import { Info } from "lucide-react";

import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { useSettings } from "@/hooks/use-settings";
import { cn } from "@/lib/utils";

export interface MetricSublabelProps {
  description?: string;
  className?: string;
}

/**
 * Inline metric description — visible only when the Explanations toggle is ON.
 * Pair with <MetricInfoIcon /> next to the metric label so the same text
 * stays reachable when the toggle is OFF.
 */
export function MetricSublabel({
  description,
  className,
}: MetricSublabelProps) {
  const { showExplanations } = useSettings();
  if (!showExplanations || !description) return null;
  return (
    <p
      className={cn(
        "line-clamp-2 text-xs leading-snug text-muted-foreground",
        className,
      )}
    >
      {description}
    </p>
  );
}

/**
 * Tiny (i) icon next to the metric label — visible only when the
 * Explanations toggle is OFF. Hover/focus reveals the same description
 * text MetricSublabel would render inline. Renders nothing when no
 * description.
 */
export function MetricInfoIcon({ description }: { description?: string }) {
  const { showExplanations } = useSettings();
  if (showExplanations || !description) return null;
  return (
    <TooltipProvider delay={150}>
      <Tooltip>
        <TooltipTrigger
          render={
            <button
              type="button"
              aria-label={description}
              className="shrink-0 text-muted-foreground hover:text-foreground"
            >
              <Info className="size-3.5" />
            </button>
          }
        />
        <TooltipContent className="max-w-xs text-xs leading-relaxed">
          {description}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

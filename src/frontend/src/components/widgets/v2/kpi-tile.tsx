import { Sparkles, TrendingDownIcon, TrendingUpIcon } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import {
  Card,
  CardAction,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Spinner } from "@/components/ui/spinner";
import { useSettings } from "@/hooks/use-settings";
import type { KpiTileData } from "@/lib/insight/kpi-row";
import type { GroupId } from "@/lib/insight/groups";
import { STATUS_TEXT_CLASS } from "@/lib/status";
import { cn } from "@/lib/utils";

export interface KpiTileProps {
  tile: KpiTileData;
  onOpenGroup?: (id: GroupId) => void;
}

const CARD_SURFACE = "@container/card";

/**
 * Presentational KPI tile: everything display-ready arrives on `tile`
 * (selectors in `lib/insight/kpi-row.ts` own formatting and scoring for both
 * the legacy batch and metric-collection sources).
 */
export function KpiTile({ tile, onOpenGroup }: KpiTileProps) {
  const { showExplanations } = useSettings();
  const interactive = Boolean(onOpenGroup && tile.groupId);

  return (
    <Card
      className={cn(
        CARD_SURFACE,
        interactive && "text-left transition-colors hover:bg-accent/50",
      )}
      render={
        interactive ? (
          <button
            type="button"
            onClick={() => {
              if (tile.groupId) onOpenGroup?.(tile.groupId);
            }}
            aria-label={`Open ${tile.label} details`}
          />
        ) : undefined
      }
    >
      <CardHeader>
        <CardDescription className="flex flex-col gap-0.5">
          <span className="truncate">{tile.label}</span>
          {showExplanations && tile.context ? (
            <span className="truncate font-normal text-muted-foreground/70">
              {tile.context}
            </span>
          ) : null}
        </CardDescription>
        <CardTitle
          className={cn(
            "text-2xl font-semibold tabular-nums @[250px]/card:text-3xl",
            tile.valueStatus !== "neutral" &&
              STATUS_TEXT_CLASS[tile.valueStatus],
          )}
        >
          {tile.value}
        </CardTitle>
        {tile.delta ? (
          <CardAction>
            <Badge
              variant="outline"
              className={STATUS_TEXT_CLASS[tile.delta.status]}
            >
              {tile.delta.down ? <TrendingDownIcon /> : <TrendingUpIcon />}
              {tile.delta.text}
            </Badge>
          </CardAction>
        ) : null}
      </CardHeader>
      <CardFooter className="text-sm text-muted-foreground">
        {tile.medianLabel ?? "No peer data"}
      </CardFooter>
    </Card>
  );
}

/** Loading state: a centered spinner in the card, matching the group cards. */
export function KpiTileLoading() {
  return (
    <Card className={cn(CARD_SURFACE, "min-h-32 items-center justify-center")}>
      <Spinner className="size-5 text-muted-foreground" aria-label="Loading" />
    </Card>
  );
}

export function KpiTilePlaceholder({ label }: { label?: string }) {
  return (
    <Card className={CARD_SURFACE}>
      <CardHeader>
        <CardDescription className="truncate">
          {label ?? " "}
        </CardDescription>
        <CardTitle className="text-2xl font-semibold tabular-nums">—</CardTitle>
      </CardHeader>
      <CardFooter className="gap-1.5 text-sm text-muted-foreground">
        <Sparkles className="size-3.5 shrink-0" aria-hidden />
        Coming soon
      </CardFooter>
    </Card>
  );
}

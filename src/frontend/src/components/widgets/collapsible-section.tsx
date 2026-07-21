import type { ReactNode } from "react";

import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { useLocalStorageState } from "@/hooks/use-local-storage-state";

export interface CollapsibleSectionProps {
  title: string;
  subtitle?: string;
  defaultOpen?: boolean;
  storageKey?: string;
  children: ReactNode;
}

function parseOpen(raw: string): boolean | undefined {
  if (raw === "1") return true;
  if (raw === "0") return false;
  return undefined;
}

function serializeOpen(value: boolean): string {
  return value ? "1" : "0";
}

export function CollapsibleSection({
  title,
  subtitle,
  defaultOpen = false,
  storageKey,
  children,
}: CollapsibleSectionProps) {
  const [open, setOpen] = useLocalStorageState({
    key: storageKey,
    defaultValue: defaultOpen,
    parse: parseOpen,
    serialize: serializeOpen,
  });

  return (
    <Collapsible
      open={open}
      onOpenChange={setOpen}
      className="overflow-hidden rounded-lg border border-border"
    >
      <CollapsibleTrigger
        render={
          <button
            type="button"
            className="flex w-full cursor-pointer items-start justify-between border-none bg-card px-4 py-3 text-left transition-colors hover:bg-accent/40"
          >
            <span className="flex flex-col">
              <span className="text-sm font-semibold text-foreground">
                {title}
              </span>
              {subtitle ? (
                <span className="mt-0.5 text-xs text-muted-foreground">
                  {subtitle}
                </span>
              ) : null}
            </span>
            <span className="flex shrink-0 items-center gap-1.5">
              <span className="rounded bg-muted px-1.5 py-px text-xs text-muted-foreground">
                {open ? "Expanded" : "Collapsed"}
              </span>
              <span className="text-xs text-muted-foreground">
                {open ? "▴" : "▾"}
              </span>
            </span>
          </button>
        }
      />
      <CollapsibleContent className="border-t border-border bg-card">
        {children}
      </CollapsibleContent>
    </Collapsible>
  );
}

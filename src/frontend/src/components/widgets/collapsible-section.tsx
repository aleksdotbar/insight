import { useEffect, useState, type ReactNode } from "react";

import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";

export interface CollapsibleSectionProps {
  title: string;
  subtitle?: string;
  defaultOpen?: boolean;
  storageKey?: string;
  children: ReactNode;
}

function readLs(storageKey: string | undefined, fallback: boolean): boolean {
  if (!storageKey || typeof window === "undefined") return fallback;
  const raw = window.localStorage.getItem(storageKey);
  if (raw === "1") return true;
  if (raw === "0") return false;
  return fallback;
}

function writeLs(storageKey: string | undefined, value: boolean): void {
  if (!storageKey || typeof window === "undefined") return;
  try {
    window.localStorage.setItem(storageKey, value ? "1" : "0");
  } catch {
    /* ignore */
  }
}

export function CollapsibleSection({
  title,
  subtitle,
  defaultOpen = false,
  storageKey,
  children,
}: CollapsibleSectionProps) {
  const [open, setOpen] = useState<boolean>(() =>
    readLs(storageKey, defaultOpen),
  );

  useEffect(() => {
    writeLs(storageKey, open);
  }, [storageKey, open]);

  return (
    <Collapsible
      open={open}
      onOpenChange={setOpen}
      className="border-border overflow-hidden rounded-lg border"
    >
      <CollapsibleTrigger
        render={
          <button
            type="button"
            className="bg-card hover:bg-accent/40 flex w-full cursor-pointer items-start justify-between border-none px-4 py-3 text-left transition-colors"
          >
            <span className="flex flex-col">
              <span className="text-foreground text-sm font-semibold">
                {title}
              </span>
              {subtitle ? (
                <span className="text-muted-foreground mt-0.5 text-xs">
                  {subtitle}
                </span>
              ) : null}
            </span>
            <span className="flex shrink-0 items-center gap-1.5">
              <span className="bg-muted text-muted-foreground rounded px-1.5 py-px text-xs">
                {open ? "Expanded" : "Collapsed"}
              </span>
              <span className="text-muted-foreground text-xs">
                {open ? "▴" : "▾"}
              </span>
            </span>
          </button>
        }
      />
      <CollapsibleContent className="border-border bg-card border-t">
        {children}
      </CollapsibleContent>
    </Collapsible>
  );
}

import { ExternalLink } from "lucide-react";

import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { cn } from "@/lib/utils";
import type { DrillData } from "@/types/insight";

import { ComingSoon } from "./coming-soon";

export interface DrillModalProps {
  drill: DrillData | null;
  open: boolean;
  loading?: boolean;
  errored?: boolean;
  onClose: () => void;
  onRetry?: () => void;
}

export function DrillModal({
  drill,
  open,
  loading = false,
  errored = false,
  onClose,
  onRetry,
}: DrillModalProps) {
  const placeholderState = loading ? "loading" : errored ? "error" : "empty";

  return (
    <Dialog
      open={open}
      onOpenChange={(next) => {
        if (!next) onClose();
      }}
    >
      <DialogContent className="flex max-h-[80vh] w-full max-w-2xl flex-col gap-0 p-0">
        {drill ? (
          <>
            <DialogHeader className="border-border flex-row items-center gap-2.5 space-y-0 border-b px-4 py-3.5 pr-12">
              <DialogTitle className="text-foreground flex-1 text-base font-bold">
                {drill.title}
              </DialogTitle>
              <span
                className={cn(
                  "rounded px-2 py-0.5 text-xs font-semibold text-white",
                  drill.srcClass,
                )}
              >
                {drill.source}
              </span>
              <span className="text-foreground text-sm font-bold">
                {drill.value}
              </span>
            </DialogHeader>

            <div className="bg-muted text-muted-foreground shrink-0 px-4 py-2 text-xs">
              {drill.filter}
            </div>

            <div className="flex-1 overflow-auto">
              {drill.rows.length === 0 ? (
                <div className="p-4">
                  <ComingSoon variant="card" />
                </div>
              ) : (
                <Table>
                  <TableHeader>
                    <TableRow className="bg-muted/50">
                      {drill.columns.map((col) => (
                        <TableHead
                          key={col}
                          className="text-muted-foreground text-xs font-semibold whitespace-nowrap"
                        >
                          {col}
                        </TableHead>
                      ))}
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {drill.rows.map((row, rowIdx) => (
                      <TableRow key={rowIdx}>
                        {drill.columns.map((col) => (
                          <TableCell
                            key={col}
                            className="text-foreground text-xs"
                          >
                            {row[col] ?? "—"}
                          </TableCell>
                        ))}
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              )}
            </div>

            {drill.rows.length > 0 ? (
              <div className="border-border flex shrink-0 items-center justify-between border-t px-4 py-2.5 text-xs">
                <a
                  href="#"
                  className="text-primary inline-flex items-center gap-1 font-medium no-underline"
                  onClick={(e) => e.preventDefault()}
                >
                  Open all in {drill.source}
                  <ExternalLink className="size-3" />
                </a>
                <span className="text-muted-foreground">
                  {drill.rows.length} records
                </span>
              </div>
            ) : null}
          </>
        ) : (
          <>
            <DialogHeader className="border-border space-y-0 border-b px-4 py-3.5 pr-12">
              <DialogTitle className="text-foreground text-base font-bold">
                Drill-down
              </DialogTitle>
            </DialogHeader>
            <div className="p-4">
              <ComingSoon
                variant="card"
                state={placeholderState}
                onRetry={onRetry}
              />
            </div>
          </>
        )}
      </DialogContent>
    </Dialog>
  );
}

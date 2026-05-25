import { useTranslation } from "react-i18next";

import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { TeamMetricsTable } from "@/components/widgets/team-metrics-table";
import type { DateRange } from "@/api/period-to-date-range";
import { useTeamMetrics } from "@/queries/team-metrics";
import type { TeamMember } from "@/types/insight";

export interface TeamMetricsModalProps {
  open: boolean;
  onClose: () => void;
  members: TeamMember[];
  range: DateRange;
}

export function TeamMetricsModal({
  open,
  onClose,
  members,
  range,
}: TeamMetricsModalProps) {
  const { t } = useTranslation();
  const { entries, isPending } = useTeamMetrics(members, range, {
    enabled: open,
  });

  return (
    <Dialog
      open={open}
      onOpenChange={(next) => {
        if (!next) onClose();
      }}
    >
      <DialogContent className="flex h-[85vh] max-h-[85vh] w-[95vw] max-w-[95vw] flex-col gap-0 p-0 sm:max-w-[95vw]">
        <DialogHeader className="border-b border-border px-6 py-4">
          <DialogTitle>{t("team_metrics_modal.title")}</DialogTitle>
        </DialogHeader>
        <TeamMetricsTable
          members={members}
          entries={entries}
          isPending={isPending}
          containerClassName="min-h-0 flex-1 overflow-auto"
        />
      </DialogContent>
    </Dialog>
  );
}

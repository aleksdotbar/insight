import { CalendarIcon } from "lucide-react";
import { useState } from "react";
import type { DateRange as DayPickerRange } from "react-day-picker";

import { resolveDateRange, toISODate } from "@/api/period-to-date-range";
import { Button } from "@/components/ui/button";
import { Calendar } from "@/components/ui/calendar";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import type { CustomRange, PeriodValue } from "@/types/insight";

function formatShortDate(iso: string): string {
  const [y, m, d] = iso.split("-").map(Number);
  const date = new Date(y ?? 1970, (m ?? 1) - 1, d ?? 1);
  return date.toLocaleDateString("en-GB", { day: "numeric", month: "short" });
}

function formatLongDate(d: Date): string {
  return d.toLocaleDateString("en-GB", {
    day: "numeric",
    month: "short",
    year: "numeric",
  });
}

const TABS: { value: PeriodValue; label: string; short: string }[] = [
  { value: "week", label: "Week", short: "W" },
  { value: "month", label: "Month", short: "M" },
  { value: "quarter", label: "Quarter", short: "Q" },
  { value: "year", label: "Year", short: "Y" },
];

export interface PeriodSelectorBarProps {
  period: PeriodValue;
  customRange: CustomRange | null;
  onPeriodChange: (period: PeriodValue) => void;
  onRangeChange: (range: CustomRange | null) => void;
}

export function PeriodSelectorBar({
  period,
  customRange,
  onPeriodChange,
  onRangeChange,
}: PeriodSelectorBarProps) {
  const [calOpen, setCalOpen] = useState(false);
  const [tempRange, setTempRange] = useState<DayPickerRange | undefined>(
    customRange
      ? {
          from: new Date(`${customRange.from}T00:00:00`),
          to: new Date(`${customRange.to}T00:00:00`),
        }
      : undefined
  );

  const activeRange = resolveDateRange(period, customRange);
  const activeRangeLabel = `${formatShortDate(activeRange.from)} – ${formatShortDate(activeRange.to)}`;

  const handleOpenChange = (open: boolean) => {
    if (open) {
      setTempRange({
        from: new Date(`${activeRange.from}T00:00:00`),
        to: new Date(`${activeRange.to}T00:00:00`),
      });
    }
    setCalOpen(open);
  };

  const groupValue = customRange ? ["custom"] : [period];
  const customLabel = customRange ? activeRangeLabel : "Custom";

  return (
    <div className="flex items-center gap-2">
      <ToggleGroup
        value={groupValue}
        onValueChange={(values) => {
          const next = Array.isArray(values) ? values[0] : values;
          if (
            next === "week" ||
            next === "month" ||
            next === "quarter" ||
            next === "year"
          ) {
            onPeriodChange(next);
            setCalOpen(false);
          }
        }}
        variant="outline"
        size="default"
      >
        {TABS.map(({ value, label, short }) => (
          <ToggleGroupItem key={value} value={value}>
            <span className="hidden sm:inline">{label}</span>
            <span className="sm:hidden">{short}</span>
          </ToggleGroupItem>
        ))}
        <Popover open={calOpen} onOpenChange={handleOpenChange}>
          <PopoverTrigger
            render={
              <ToggleGroupItem value="custom" className="gap-1.5">
                <CalendarIcon className="size-3.5" />
                <span>{customLabel}</span>
              </ToggleGroupItem>
            }
          />
          <PopoverContent align="end" className="w-auto p-0">
            {tempRange?.from ? (
              <div className="border-b border-border px-4 py-3">
                <p className="text-sm font-semibold text-foreground">
                  {formatLongDate(tempRange.from)}
                  {tempRange.to ? (
                    ` – ${formatLongDate(tempRange.to)}`
                  ) : (
                    <span className="text-muted-foreground">
                      {" → pick end date"}
                    </span>
                  )}
                </p>
              </div>
            ) : null}
            <Calendar
              mode="range"
              selected={tempRange}
              onSelect={(r) => setTempRange(r)}
              defaultMonth={tempRange?.from}
              numberOfMonths={
                typeof window !== "undefined" && window.innerWidth < 640 ? 1 : 2
              }
            />
            <div className="flex items-center gap-3 border-t border-border px-4 py-2">
              {customRange ? (
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => {
                    setTempRange(undefined);
                    onRangeChange(null);
                    setCalOpen(false);
                  }}
                >
                  Clear
                </Button>
              ) : null}
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setCalOpen(false)}
              >
                Cancel
              </Button>
              <Button
                variant="default"
                size="sm"
                className="ml-auto"
                disabled={!tempRange?.from}
                onClick={() => {
                  if (!tempRange?.from) return;
                  const toDate = tempRange.to ?? tempRange.from;
                  onRangeChange({
                    from: toISODate(tempRange.from),
                    to: toISODate(toDate),
                  });
                  setCalOpen(false);
                }}
              >
                Apply
              </Button>
            </div>
          </PopoverContent>
        </Popover>
      </ToggleGroup>
    </div>
  );
}

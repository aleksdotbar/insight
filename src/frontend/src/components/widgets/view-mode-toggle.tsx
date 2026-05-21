import { LayoutGrid, Rows3 } from "lucide-react";

import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import type { ViewMode } from "@/types/insight";

export interface ViewModeToggleProps {
  mode: ViewMode;
  onChange: (mode: ViewMode) => void;
}

export function ViewModeToggle({ mode, onChange }: ViewModeToggleProps) {
  return (
    <ToggleGroup
      value={[mode]}
      onValueChange={(values) => {
        const next = Array.isArray(values) ? values[0] : values;
        if (next === "chart" || next === "tile") onChange(next);
      }}
      variant="outline"
      size="default"
      className="hidden sm:flex"
    >
      <ToggleGroupItem value="chart" className="gap-1.5">
        <Rows3 className="size-4" />
        Charts
      </ToggleGroupItem>
      <ToggleGroupItem value="tile" className="gap-1.5">
        <LayoutGrid className="size-4" />
        Tiles
      </ToggleGroupItem>
    </ToggleGroup>
  );
}

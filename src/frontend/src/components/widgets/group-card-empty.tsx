import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyTitle,
} from "@/components/ui/empty";

/**
 * Empty body for a dashboard group card. `min-h` approximates the populated
 * card's content (summary line + three preview rows) so an empty card holds
 * the same footprint in the grid instead of collapsing.
 */
export function GroupCardEmpty() {
  return (
    <Empty className="min-h-28 gap-1 p-4">
      <EmptyHeader>
        <EmptyTitle className="text-sm">No data</EmptyTitle>
        <EmptyDescription className="text-xs">
          No metrics with data for this period.
        </EmptyDescription>
      </EmptyHeader>
    </Empty>
  );
}

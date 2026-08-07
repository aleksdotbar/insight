export interface PersonCoverageProps {
  /** Sections with no reading at all; empty when everything on the nav reads. */
  uncovered: string[];
}

/**
 * What this page cannot see.
 *
 * Without it the screen reads as a whole picture of a person, and for someone
 * whose work leaves few traces in the connected systems that picture is mostly
 * their chat and their calendar. Naming the gap costs one line and no request
 * — the standings behind it are the ones the navigation already asked for.
 *
 * "No data reaches us" rather than "no activity": the two are different, and
 * only one of them is something we know.
 */
export function PersonCoverage({ uncovered }: PersonCoverageProps) {
  if (uncovered.length === 0) return null;
  return (
    <p className="text-xs text-muted-foreground">
      No data reaches us for {uncovered.join(", ")} — this page shows what is
      measured, not everything this person does.
    </p>
  );
}

export interface ProgressTrackProps {
  barLeftPct: number;
  barWidthPct: number;
  medianLeftPct: number;
  barColorClass: string;
}

const clampPct = (v: number): number => Math.max(0, Math.min(100, v));

export function ProgressTrack({
  barLeftPct,
  barWidthPct,
  medianLeftPct,
  barColorClass,
}: ProgressTrackProps) {
  const safeLeft = clampPct(barLeftPct);
  const safeWidth = Math.min(clampPct(barWidthPct), 100 - safeLeft);
  const safeMedian = clampPct(medianLeftPct);

  return (
    <div className="bg-muted relative mt-1.5 h-5 rounded">
      <div
        style={{ left: `${safeLeft}%`, width: `${safeWidth}%` }}
        className={`absolute top-[3px] bottom-[3px] rounded-sm transition-[width] duration-500 ease-in-out ${barColorClass}`}
      />
      <div
        style={{ left: `${safeMedian}%` }}
        className="bg-foreground/60 absolute -top-0.5 -bottom-0.5 w-[2px] -translate-x-1/2 rounded"
      />
    </div>
  );
}

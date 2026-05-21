export interface DynamicWidthBarProps {
  pct: number;
  colorClass: string;
}

export function DynamicWidthBar({ pct, colorClass }: DynamicWidthBarProps) {
  return (
    <div
      style={{ width: `${Math.min(pct, 100)}%` }}
      className={`h-full rounded-full transition-[width] duration-300 ease-in-out ${colorClass}`}
    />
  );
}

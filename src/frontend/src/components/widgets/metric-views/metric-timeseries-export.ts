import type { DateRange } from "@/api/period-to-date-range";

export function metricTimeseriesFilename(
  id: string,
  range: DateRange,
  extension: "csv" | "xlsx"
): string {
  const safeId =
    id
      .trim()
      .replace(/[^a-zA-Z0-9._-]+/g, "-")
      .replace(/^-+|-+$/g, "") || "timeseries";
  return `${safeId}_${range.from}_${range.to}.${extension}`;
}

export function downloadBlob(blob: Blob, filename: string): void {
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  anchor.hidden = true;
  document.body.append(anchor);
  anchor.click();
  anchor.remove();
  window.requestAnimationFrame(() => URL.revokeObjectURL(url));
}

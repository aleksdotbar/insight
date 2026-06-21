function hashStr(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++) {
    h = ((h << 5) - h + s.charCodeAt(i)) | 0;
  }
  return Math.abs(h);
}

function rng(seed: number): () => number {
  let s = seed || 1;
  return () => {
    s = (s * 9301 + 49297) % 233280;
    return s / 233280;
  };
}

export interface HistogramBin {
  metric_key: string;
  bin: number;
  bin_end: number;
  count: number;
}

export function mockHistogramBins(
  personId: string,
  metricKey: string,
  periodDays = 30,
): HistogramBin[] {
  const scale = periodDays / 30;
  const r = rng(hashStr(`${personId}|${metricKey}|hist`));
  const binCount = 8;
  const bins: HistogramBin[] = [];
  const max = 50 + Math.floor(r() * 50);
  const step = Math.ceil(max / binCount);
  for (let i = 0; i < binCount; i++) {
    bins.push({
      metric_key: metricKey,
      bin: i * step,
      bin_end: (i + 1) * step,
      count: Math.max(1, Math.round((2 + r() * 14) * scale)),
    });
  }
  return bins;
}

const SECTION_TREND_SERIES: Record<string, { key: string; base: number; spread: number }[]> = {
  code_quality: [
    { key: "pr_cycle_time", base: 22, spread: 12 },
    { key: "build_success", base: 88, spread: 8 },
  ],
  ai_adoption: [
    { key: "cc_lines", base: 180, spread: 90 },
    { key: "cursor_lines", base: 240, spread: 120 },
  ],
};

export interface SectionTrendLongRow {
  metric_date: string;
  section_id: string;
  series_key: string;
  value: number;
}

export function mockSectionTrend(
  personId: string,
  sectionId: string,
  periodDays: number,
): SectionTrendLongRow[] {
  const defs = SECTION_TREND_SERIES[sectionId];
  if (!defs) return [];
  const days = Math.max(7, Math.min(periodDays, 90));
  const out: SectionTrendLongRow[] = [];
  const today = new Date();
  for (let i = days - 1; i >= 0; i--) {
    const d = new Date(today);
    d.setDate(d.getDate() - i);
    const metric_date = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
    for (const def of defs) {
      const r = rng(hashStr(`${personId}|${sectionId}|${def.key}|${i}`));
      const v = def.base + (r() - 0.5) * 2 * def.spread;
      out.push({
        metric_date,
        section_id: sectionId,
        series_key: def.key,
        value: Math.max(0, Math.round(v * 10) / 10),
      });
    }
  }
  return out;
}

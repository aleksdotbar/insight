function hashString(input: string): number {
  let h = 2166136261;
  for (let i = 0; i < input.length; i++) {
    h ^= input.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
}

export function swatchPalette(seeds: string[]): Record<string, string> {
  const ordered = [...new Set(seeds)].sort((a, b) => hashString(a) - hashString(b));
  const palette: Record<string, string> = {};
  ordered.forEach((seed) => {
    const hue = hashString(seed) % 360;
    palette[seed] = `oklch(var(--swatch-l) 0.14 ${hue})`;
  });
  return palette;
}

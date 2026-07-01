function hashString(input: string): number {
  let h = 2166136261;
  for (let i = 0; i < input.length; i++) {
    h ^= input.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
}

export function swatchPalette(seeds: string[]): Record<string, string> {
  const palette: Record<string, string> = {};
  for (const seed of new Set(seeds)) {
    const hue = hashString(seed) % 360;
    palette[seed] = `oklch(var(--swatch-l) 0.14 ${hue})`;
  }
  return palette;
}

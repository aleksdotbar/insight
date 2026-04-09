/**
 * Chart color constants — hex values are allowed in uikit/base/
 * Import these in composite chart components to avoid hex literals there.
 */

export const CHART_BLUE = '#2563EB';
export const CHART_PURPLE = '#7C3AED';
export const CHART_GREEN = '#16A34A';
export const CHART_LIGHT_BLUE = '#93C5FD';
export const CHART_GRAY = '#9CA3AF';
export const CHART_TRACK_BG = '#F0F2F7';
export const CHART_AI_LOC = '#BFDBFE';
export const CHART_SPEC_LINES = '#A78BFA';

/**
 * Chart font sizes (px) — Recharts SVG requires numeric px values.
 * Single source of truth for all chart text sizing.
 */
export const CHART_FONT_TICK = 10;       // axis ticks, legend
export const CHART_FONT_LABEL = 12;      // axis labels, data labels
export const CHART_FONT_VALUE = 14;      // inline values
export const CHART_FONT_HERO = 36;       // gauge center number

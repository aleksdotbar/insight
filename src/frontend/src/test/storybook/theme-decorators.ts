/**
 * Theme toolbar for stories. Adaptation of the corporate
 * `commonPreviewDecorators` (react-tests/src/storybook/preview.ts) to
 * Insight's class convention: our `ThemeProvider`
 * (src/components/theme-provider.tsx) toggles `light`/`dark` classes on
 * `<html>` (Tailwind / shadcn convention), not `acv-color-scheme-*`.
 */

import { withThemeByClassName } from "@storybook/addon-themes";
import type { Decorator } from "@storybook/react-vite";

export const themeDecorators: Decorator[] = [
  withThemeByClassName({
    themes: { light: "light", dark: "dark" },
    defaultTheme: "light",
  }),
];

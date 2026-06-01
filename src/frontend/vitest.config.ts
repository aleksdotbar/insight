/**
 * Test runner config. Kept separate from `vite.config.ts` so vite's build
 * pipeline doesn't pull in the testing-library deps in production bundles,
 * and so the test setup file can run jsdom + jest-dom matchers without
 * interfering with dev-mode HMR.
 */

import path from "path";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  test: {
    environment: "jsdom",
    globals: false,
    setupFiles: ["./src/test/setup.ts"],
    css: false,
    include: ["src/**/*.test.ts", "src/**/*.test.tsx"],
  },
});

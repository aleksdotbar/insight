/**
 * Vitest setup. Loaded once per test process per `vitest.config.ts`.
 *
 * Brings in `@testing-library/jest-dom` matchers so component tests can use
 * `expect(node).toBeInTheDocument()` and friends. Other global setup goes
 * here as it accrues.
 */

import "@testing-library/jest-dom/vitest";
import { afterEach } from "vitest";
import { cleanup } from "@testing-library/react";

afterEach(() => {
  cleanup();
});

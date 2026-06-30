/**
 * PoC story + component test for <KpiTile>.
 *
 * Demonstrates the vendored corporate Storybook setup end-to-end. Note the
 * @storybook/addon-vitest model: EVERY story runs as a browser test — a
 * story with no `play` is a smoke test (renders without throwing); a story
 * with `play` adds interaction assertions. Opt a story out with the
 * `skip-test` tag (see the `tags.skip` list in vitest.config.ts).
 *
 *   - `Default`    — demo story for the Storybook UI; also a smoke test.
 *   - `TestOkRow`  — play-driven test asserting the same catalog→widget rule
 *                    the existing `kpi-tile.test.tsx` covers, but in a real
 *                    browser with the catalog mocked over the wire via MSW
 *                    instead of `vi.mock`.
 *
 * See docs/testing/storybook-component-tests.md.
 */

import type { Meta, StoryObj } from "@storybook/react-vite";
import { http, HttpResponse } from "msw";
import { expect, waitFor } from "storybook/test";

import type { CatalogResponse } from "@/api/catalog-client";
import { authStore } from "@/auth/auth-store";
import type { IcKpi, PeriodValue } from "@/types/insight";

import { KpiTile } from "./kpi-tile";

const TENANT = "t-1";

function makeKpi(overrides: Partial<IcKpi> = {}): IcKpi {
  return {
    period: "month" as PeriodValue,
    metric_key: "bugs_fixed",
    label: "Bugs Fixed",
    value: "12",
    raw_value: 12,
    unit: "",
    sublabel: "Jira",
    description: "Bug-type Jira issues closed in the selected period.",
    delta: "",
    delta_type: "neutral",
    ...overrides,
  };
}

function catalog(rows: CatalogResponse["metrics"]): CatalogResponse {
  return {
    tenant_id: TENANT,
    generated_at: "2026-06-01T00:00:00Z",
    metrics: rows,
    links: [],
  };
}

const okCatalog = catalog([
  {
    id: "id-0",
    metric_key: "ic_kpis.bugs_fixed",
    label: "Bugs Fixed",
    higher_is_better: true,
    is_member_scale: false,
    source_tags: [],
    schema_status: "ok",
    thresholds: {
      good: 1,
      warn: 0,
      resolved_from: "product-default",
      bounded_by_lock: false,
    },
  },
]);

const errorCatalog = catalog([
  {
    id: "id-0",
    metric_key: "ic_kpis.bugs_fixed",
    label: "Bugs Fixed",
    higher_is_better: true,
    is_member_scale: false,
    source_tags: [],
    schema_status: "error",
    schema_error_code: "column_not_found",
    thresholds: {
      good: 1,
      warn: 0,
      resolved_from: "product-default",
      bounded_by_lock: false,
    },
  },
]);

const meta: Meta<typeof KpiTile> = {
  title: "Widgets/v2/KpiTile",
  component: KpiTile,
  // The catalog query keys off the signed-in tenant; align it with the
  // mocked catalog's `tenant_id` so the row is not treated as a cross-tenant
  // mismatch. The global preview `beforeEach` (authStore.reset + localStorage
  // clear) handles cleanup between stories, so we only set up here.
  beforeEach: () => {
    authStore.setTenantId(TENANT);
  },
};
export default meta;

type Story = StoryObj<typeof KpiTile>;

/** Demo story for the Storybook UI (not a test — no `test` tag). */
export const Default: Story = {
  args: { kpi: makeKpi({ peer_median: 6, peer_n: 4 }) },
  parameters: {
    msw: {
      handlers: [
        http.post("/api/analytics/v1/catalog/get_metrics", () =>
          HttpResponse.json(okCatalog),
        ),
      ],
    },
  },
};

/** Component test: ok catalog row drives the peer-median label. */
export const TestOkRow: Story = {
  tags: ["test"],
  args: { kpi: makeKpi({ peer_median: 6, peer_n: 4 }) },
  parameters: {
    msw: {
      handlers: [
        http.post("/api/analytics/v1/catalog/get_metrics", () =>
          HttpResponse.json(okCatalog),
        ),
      ],
    },
  },
  play: async ({ canvas }) => {
    // Singular `getByText` (throws on >1 match) doubles as a guard that the
    // preview decorators wrap the story exactly once — a double-applied
    // decorator would render two <KpiTile>s and fail here.
    await expect(canvas.getByText("Bugs Fixed")).toBeInTheDocument();
    await expect(canvas.getByText("12")).toBeInTheDocument();
    // The median footer appears only once the (mocked) catalog query
    // resolves and supplies the `ic_kpis.bugs_fixed` row.
    await waitFor(() =>
      expect(canvas.getByText(/median 6/i)).toBeInTheDocument(),
    );
  },
};

/**
 * Component test: a `schema_status='error'` catalog row suppresses the peer
 * median (the metric's column is broken), so the footer reads "No peer data".
 * Also exercises story isolation — it relies on its OWN MSW handler and a
 * clean authStore, proving the previous story's mock/state did not leak.
 */
export const TestSchemaError: Story = {
  tags: ["test"],
  args: { kpi: makeKpi({ peer_median: 6, peer_n: 4 }) },
  parameters: {
    msw: {
      handlers: [
        http.post("/api/analytics/v1/catalog/get_metrics", () =>
          HttpResponse.json(errorCatalog),
        ),
      ],
    },
  },
  play: async ({ canvas }) => {
    await expect(canvas.getByText("Bugs Fixed")).toBeInTheDocument();
    // No median label for a broken-schema row; footer falls back.
    await waitFor(() =>
      expect(canvas.getByText(/no peer data/i)).toBeInTheDocument(),
    );
    await expect(canvas.queryByText(/median/i)).not.toBeInTheDocument();
  },
};

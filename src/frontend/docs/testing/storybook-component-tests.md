# Storybook + component tests in Insight Frontend

> Status: **PoC implemented** (scaffold + one example test). The setup is wired
> end-to-end; a sample `KpiTile` test is green in a real browser. Date:
> 2026-06-30. Owner: Sergey Mozhaev (after consulting architect E. Deisling).

## 1. Context and decision

We need component tests following the **corporate approach** (Storybook-based).
That setup lives in two adjacent monorepos:

- `@constructor/react-tests` (`react-kit/packages/react-tests`) — the **base
  layer** for unit and Storybook tests. Standalone, no runtime coupling to the
  micro-frontend system.
- `@constructor/fragment-storybook-react` (`federation/packages/storybook-react`)
  — a React wrapper on top of the base layer **for fragments** (Module
  Federation). Bound to `FragmentContextProvider`, feature flags, fragment
  storage, etc.

**Insight is a plain React SPA, not a fragment**, and it is a **public,
Apache-2.0 repository** (`github.com/constructorfabric/insight-front`) with
**zero `@constructor/*` dependencies**. The corporate registry is a private
Nexus (`@constructor:registry=https://nexus.constr.dev/...`).

### Decision

1. **Do not add a private `@constructor/*` dependency.** A private Nexus package
   would break `pnpm install` for anyone without corporate access (external
   contributors, public CI). This matches the architect's direct guidance:
   "all you can take is a reference to our setup and write it inside Insight."
2. **Vendor the approach into the repo** using public packages (Storybook 10 +
   `@storybook/addon-vitest` + MSW), with `@constructor/react-tests` and
   `fragment-storybook-react` as the reference.
3. **Execution model = full parity with the corporate "base layer":** stories
   run as tests **in a real browser** via `@storybook/addon-vitest` +
   `@vitest/browser-playwright` (headless chromium). Play functions are the tests.

## 2. What we reused vs. cut

| Layer | Source | Reused | Cut |
|---|---|---|---|
| Storybook config (`createStorybookConfig`, `getStorybookProjectConfig`) | `react-tests/src/storybook/config.ts` | ✅ inlined (factory → `.storybook/main.ts` + browser vitest project) | — |
| Theme preview decorator (`commonPreviewDecorators`) | `react-tests/src/storybook/preview.ts` | ✅ adapted to our `light`/`dark` classes | `acv-color-scheme-*` classes |
| Manager UI (`setupStorybookManager`) | `react-tests/src/storybook/manager.ts` | ✅ copied | — |
| Memory router for stories | `storybook-react/src/preview/router.ts` | ✅ copied (TanStack Router, memory history) | — |
| Wrapper decorator | `storybook-react/.../withFederation.tsx` | ✏️ rewritten as `WithProviders` | `FragmentContextProvider`, fragment localization, federation mock |
| Federation state API | `storybook-react/.../storybookApp.tsx` | ❌ | `setFragmentFeatureFlags/Storage/...`, `createFederationMock` |
| MSW in stories | both packages (`msw-storybook-addon`) | ✅ reuse our `src/mocks/` | — |

The key difference from fragments (architect's words): **their router is wired at
the fragment level**. We need a router too, but not the fragment router — we use a
plain **TanStack Router with memory history**, exactly as in
`storybook-react/src/preview/router.ts`.

## 3. File layout (as implemented)

```
cyber-insight-front/
├─ .storybook/
│   ├─ main.ts                 # createStorybookConfig equivalent (stories, addons, staticDirs)
│   ├─ preview.tsx             # global decorators (theme + WithProviders), MSW init, global beforeEach reset
│   └─ manager.ts              # Storybook UI theme (dark)
├─ src/test/storybook/
│   ├─ test-router.ts          # createTestingRouter() — memory-history TanStack Router
│   ├─ with-providers.tsx      # QueryClient + router + theme + i18n decorator
│   └─ theme-decorators.ts     # addon-themes adapted to light/dark
├─ vitest.config.ts            # test.projects = [unit (jsdom), storybook (browser)]
├─ public/mockServiceWorker.js # already present (msw workerDirectory: public)
└─ src/components/widgets/v2/kpi-tile.stories.tsx  # PoC story + 2 play tests
```

> Existing `*.test.tsx` (RTL/jsdom) are untouched and run in the `unit` project.
> Storybook tests are added as a second, independent `storybook` project.
>
> Note: `@storybook/addon-vitest` ≥ Storybook 10.3 auto-applies preview
> annotations, so **no `.storybook/vitest.setup.ts` / `setProjectAnnotations` is
> needed** — see §13.

## 4. Dependencies (devDependencies, public)

Versions match corporate parity (Storybook 10.3.5) and our `vitest@4.1.7`,
`vite@8`, `react@19.2.6`.

```jsonc
{
  "devDependencies": {
    "storybook": "10.3.5",
    "@storybook/react-vite": "10.3.5",
    "@storybook/addon-vitest": "10.3.5",
    "@storybook/addon-themes": "10.3.5",
    "@storybook/addon-a11y": "10.3.5",
    "@storybook/addon-docs": "10.3.5",
    "@vitest/browser-playwright": "4.1.7", // aligned with vitest
    "playwright": "1.57.0",                 // browser binaries for browser-mode
    "msw-storybook-addon": "2.0.7"          // 2.0.7, not 2.0.6 — see §13
  }
}
```

One-time browser install: `pnpm exec playwright install chromium`.

**Run on Node 24** (`.nvmrc`): vite 8 / Storybook 10 require Node ≥ 20.19;
`nvm use` picks up 24.

## 5. Configuration

### 5.1 `.storybook/main.ts`

```ts
import type { StorybookConfig } from "@storybook/react-vite";

const config: StorybookConfig = {
  stories: ["../src/**/*.stories.@(ts|tsx)"],
  addons: [
    "@storybook/addon-themes",
    "@storybook/addon-vitest",
    "@storybook/addon-a11y",
    "@storybook/addon-docs",
  ],
  framework: { name: "@storybook/react-vite", options: {} },
  core: { disableTelemetry: true, enableCrashReports: false },
  typescript: { reactDocgen: "react-docgen-typescript" },
  staticDirs: ["../public"], // serves public/mockServiceWorker.js
};

export default config;
```

### 5.2 `vitest.config.ts` (two projects)

We keep a dedicated `vitest.config.ts` (so test deps don't leak into the prod
`vite.config.ts`). The corporate `getStorybookProjectConfig()` factory is inlined:

```ts
import path from "path";
import react from "@vitejs/plugin-react";
import { playwright } from "@vitest/browser-playwright";
import { storybookTest } from "@storybook/addon-vitest/vitest-plugin";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [react()],
  resolve: { alias: { "@": path.resolve(__dirname, "./src") } },
  test: {
    projects: [
      // project 1: existing RTL/jsdom unit & component tests (unchanged)
      {
        extends: true,
        test: {
          name: "unit",
          environment: "jsdom",
          globals: false,
          css: false,
          setupFiles: ["./src/test/setup.ts"],
          include: ["src/**/*.test.ts", "src/**/*.test.tsx"],
        },
      },
      // project 2: stories run as tests in a real browser (corporate parity)
      {
        extends: true,
        plugins: [
          storybookTest({
            configDir: path.resolve(__dirname, ".storybook"),
            tags: { include: ["test"], exclude: [], skip: ["skip-test"] },
          }),
        ],
        test: {
          name: "storybook",
          browser: {
            enabled: true,
            headless: true,
            provider: playwright(),
            instances: [{ browser: "chromium" }],
          },
          // No setupFiles: addon-vitest auto-applies .storybook/preview.tsx (≥10.3).
        },
      },
    ],
  },
});
```

### 5.3 `.storybook/manager.ts`

```ts
import { addons } from "storybook/manager-api";
import { themes } from "storybook/theming";

addons.setConfig({ theme: themes.dark });
```

### 5.4 `package.json` scripts

```jsonc
{
  "scripts": {
    "storybook:dev": "storybook dev -p 6006",
    "storybook:build": "storybook build",
    "test": "vitest run --project=unit",          // keep current CI behaviour
    "test:watch": "vitest --project=unit",
    "test:storybook": "vitest --project=storybook",
    "test:storybook:ci": "vitest run --project=storybook"
  }
}
```

## 6. The `WithProviders` decorator (replaces `WithFederation`)

This is the core adaptation. We keep the structure of
`storybook-react/.../withFederation.tsx` but swap fragment providers for
Insight's own stack from `src/main.tsx` (QueryClient + memory router + theme +
i18n). Federation context / feature flags are removed.

`src/test/storybook/test-router.ts` (copied from `storybook-react/src/preview/router.ts`):

```ts
import {
  createMemoryHistory,
  createRootRoute,
  createRouter,
  Outlet,
} from "@tanstack/react-router";

export function createTestingRouter() {
  const rootRoute = createRootRoute({ component: Outlet });
  const history = createMemoryHistory({ initialEntries: ["/"] });
  const router = createRouter({ routeTree: rootRoute, history });
  return { router, history, reset: () => history.replace("/") };
}
```

`src/test/storybook/with-providers.tsx`:

```tsx
import type { Decorator } from "@storybook/react-vite";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { RouterProvider } from "@tanstack/react-router";
import { I18nextProvider } from "react-i18next";
import { useEffect, useMemo } from "react";

import i18n from "@/i18n";
import { ThemeProvider } from "@/components/theme-provider";
import { createTestingRouter } from "./test-router";

export const WithProviders: Decorator = (Story) => {
  // Fresh QueryClient + router per story (isolation).
  const client = useMemo(
    () => new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: 0 } } }),
    [],
  );
  const { router } = useMemo(() => createTestingRouter(), []);

  // Mount the story as the memory-router's not-found component (same trick as
  // the corporate fragment decorator) so it gets full router context.
  useEffect(() => {
    router.update({ defaultNotFoundComponent: () => <Story /> });
  }, [router, Story]);

  return (
    <QueryClientProvider client={client}>
      <ThemeProvider>
        <I18nextProvider i18n={i18n}>
          <RouterProvider router={router} />
        </I18nextProvider>
      </ThemeProvider>
    </QueryClientProvider>
  );
};
```

> `CatalogProvider` / `AppErrorBoundary` from `main.tsx` are intentionally NOT in
> the shared decorator: catalog-driven widgets mock `POST /catalog/get_metrics`
> via MSW per story, so a story exercises the real `useCatalog()` wiring instead
> of a stubbed provider.

## 7. Theme decorator (adaptation of `commonPreviewDecorators`)

Our `ThemeProvider` sets `light`/`dark` classes on `<html>` (Tailwind / shadcn
convention, `src/components/theme-provider.tsx`), not `acv-color-scheme-*`:

```ts
// src/test/storybook/theme-decorators.ts
import { withThemeByClassName } from "@storybook/addon-themes";
import type { Decorator } from "@storybook/react-vite";

export const themeDecorators: Decorator[] = [
  withThemeByClassName({
    themes: { light: "light", dark: "dark" },
    defaultTheme: "light",
  }),
];
```

## 8. `.storybook/preview.tsx` (decorators + MSW + global reset)

```tsx
import type { Preview } from "@storybook/react-vite";
import { initialize, mswLoader } from "msw-storybook-addon";

import "@/index.css"; // Tailwind v4 + theme tokens (our styles, not @constructor/globals)
import { authStore } from "@/auth/auth-store";
import { themeDecorators } from "@/test/storybook/theme-decorators";
import { WithProviders } from "@/test/storybook/with-providers";

initialize({ onUnhandledRequest: "bypass" });

const preview: Preview = {
  parameters: {
    layout: "centered",
    docs: { stories: { filter: (s: { name: string }) => !s.name.startsWith("Test") } },
  },
  // Reset app-level singletons before every story — parity with the corporate
  // per-story reset. QueryClient/router are already fresh (WithProviders), MSW
  // handlers are reset by the addon; this covers module singletons (auth) and
  // persisted UI prefs (theme / locale / settings in localStorage). A story
  // needing a tenant sets it in its own beforeEach (runs after this one).
  beforeEach: () => {
    authStore.reset();
    try {
      window.localStorage.clear();
    } catch {
      /* localStorage unavailable */
    }
  },
  loaders: [mswLoader],
  decorators: [...themeDecorators, WithProviders],
};

export default preview;
```

> The MSW worker (`public/mockServiceWorker.js`) is already generated
> (`"msw": { "workerDirectory": ["public"] }` in `package.json`). Handlers come
> from the existing `src/mocks/` or are defined per story (see §9).

## 9. Example story + play test (`KpiTile`)

Covers the same rules as the existing `kpi-tile.test.tsx`, but as browser tests.
The catalog is mocked via MSW (`/api/analytics/v1/catalog/get_metrics`).

```tsx
import type { Meta, StoryObj } from "@storybook/react-vite";
import { http, HttpResponse } from "msw";
import { expect, waitFor } from "storybook/test";

import { KpiTile } from "./kpi-tile";

const meta: Meta<typeof KpiTile> = {
  title: "Widgets/v2/KpiTile",
  component: KpiTile,
  beforeEach: () => { authStore.setTenantId("t-1"); }, // global beforeEach handles cleanup
};
export default meta;
type Story = StoryObj<typeof KpiTile>;

// Demo story for the Storybook UI; also runs as a smoke test (see §13).
export const Default: Story = { args: { kpi: /* ... */ } };

// Component test: ok catalog row drives the peer-median label.
export const TestOkRow: Story = {
  args: { kpi: /* ... peer_median 6 ... */ },
  parameters: {
    msw: { handlers: [http.post("/api/analytics/v1/catalog/get_metrics",
      () => HttpResponse.json(okCatalog))] },
  },
  play: async ({ canvas }) => {
    await expect(canvas.getByText("Bugs Fixed")).toBeInTheDocument();
    await waitFor(() => expect(canvas.getByText(/median 6/i)).toBeInTheDocument());
  },
};
```

Pattern (from both corporate repos): every story is a test (see §13), `play(...)`
is the test body, `expect`/`waitFor`/`userEvent` come from `storybook/test`, and
assertions run against `canvas`.

## 10. Migration of existing tests

- Current `src/**/*.test.tsx` (RTL/jsdom, `vi.mock`) — **untouched**, run in `unit`.
- `pnpm test` stays scoped to `unit` (CI behaviour unchanged).
- New component tests are written story-style (`storybook` project).
- Over time, visual / interaction cases move to stories; pure-logic tests
  (`lib/`, transforms) stay as plain vitest units.

## 11. CI

- Add `pnpm exec playwright install --with-deps chromium` before `test:storybook:ci`.
- Browser-mode needs headless chromium (OS deps). For the test CI job, use a
  Playwright base image (`mcr.microsoft.com/playwright`) or install on the fly.
- Prod build (`pnpm build`) is unaffected — Storybook/test deps are dev-only.

## 12. Rollout phases

1. **Phase 0 — scaffold (done):** deps, `.storybook/*`, `src/test/storybook/*`,
   `storybook` project in `vitest.config.ts`, scripts.
2. **Phase 1 — first widget (done):** stories + play tests for `KpiTile`; green
   `test:storybook`. MSW catalog-mock pattern established.
3. **Phase 2 — CI:** playwright install + `test:storybook:ci` job.
4. **Phase 3 — rollout:** stories for key widgets (`bullet-chart`,
   `members-heatmap`, `attention-needed`, `metric-card`, `members-table`).

## 13. Notes and decisions

- **`setProjectAnnotations` is deliberately NOT used.** Since Storybook 10.3,
  `@storybook/addon-vitest` auto-applies preview annotations from `configDir`.
  Adding a `.storybook/vitest.setup.ts` with `setProjectAnnotations([preview])`
  would **double-apply decorators** (a known footgun — the plugin even logs a
  warning and skips its own provisioning when it detects the call). Verified:
  the singular `canvas.getByText(...)` queries in the PoC pass, which would throw
  on a doubled render — so decorators apply exactly once. Both corporate repos
  still carry the setup file; for us it is legacy and intentionally omitted.
- **Tag model: every story is a test (not opt-in).** `addon-vitest` defaults to
  `include: ['test']` and Storybook auto-tags every story with `test`, so all
  stories run: a story with no `play` is a smoke test, a story with `play` adds
  assertions. Opt a story out with the `skip-test` tag (`tags.skip` in
  `vitest.config.ts`). Verified via `vitest list --project=storybook` (both
  `Default` and the play stories are listed as tests).
- **`msw-storybook-addon` 2.0.6 → 2.0.7.** 2.0.6's `initialize()` writes to
  `worker.context.activationPromise`, but in our `msw@2.14.6` `worker.context`
  is `undefined` (corporate uses `msw@2.12.7`). 2.0.7 stores the promise in a
  module variable and works.
- **TanStack Router version drift:** corporate `react-tests` pins `1.150.0`; we
  use `1.169.2`. Since we don't depend on `react-tests`, there's no conflict.
- **`tanstackRouter` plugin in `vite.config.ts`** (autoCodeSplitting, scans
  `src/routes`) is picked up by Storybook. Both `test:storybook` and
  `storybook build` are green, so it does not interfere. Stories use the
  memory router, not `routeTree.gen`.
- **`kpi-tile.test.tsx` (RTL) is kept** alongside the story as a deliberate PoC
  duplicate. Decide its fate later (keep "logic in unit, render in stories", or
  migrate).
- **Apache-2.0 hygiene:** no `@constructor/*` imports and no private `.npmrc` —
  every dependency above is public (npmjs).

## 14. Reference sources (siblings in `../`)

- Base: `react-kit/packages/react-tests/src/storybook/{config,plugin,preview,manager}.ts`
- React wrapper (structure): `federation/packages/storybook-react/src/preview/{router.ts,decorators/withFederation.tsx}`
- Consumer example: `react-kit/packages/react-kit/{.storybook/*,vite.config.ts,package.json}`
- Story + play example: `federation/packages/storybook-react/src/entries/**/**.stories.tsx`
```

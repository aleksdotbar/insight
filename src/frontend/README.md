# Insight Frontend

Frontend application for **Insight** — a decision intelligence platform for engineering analytics, productivity insights, bottleneck detection, AI adoption tracking, and team health visibility.

Single-page application built on React 19 + TanStack Router + TanStack Query + shadcn/ui. Uses MSW for offline / demo mocking; talks to the Insight backend in production.

- [Insight monorepo](https://github.com/constructorfabric/insight) (backend, infra, Helm charts)
- [Insight spec](https://github.com/constructorfabric/insight-spec) (connector specs, API contracts)

<!-- CI rebuild marker — bumped to retrigger the frontend image build on
the constructorfabric/* namespace flip (2026-06-09). -->


## Tech Stack

| Layer | Technology |
|---|---|
| Routing | TanStack Router (file-based, auto-generated route tree) |
| Data | TanStack Query (per-query hooks under `src/queries/`) |
| Build | Vite 8 |
| Language | TypeScript 6 (strict) |
| Styling | Tailwind CSS 4 + shadcn/ui (`base-vega` style, CSS variables) |
| Charts | Recharts 3 |
| Auth | OIDC via `oidc-client-ts` (Authorization Code + PKCE) |
| i18n | `i18next` + `react-i18next` (English only today) |
| Mocks | MSW (Mock Service Worker) |
| Linting | ESLint (flat config) |
| Package manager | pnpm 10 |
| Node | 24 (see `.nvmrc`) |

## Prerequisites

- Node.js 24 (`nvm use` picks up `.nvmrc`)
- pnpm 10+
- Docker (for container builds)

## Quick Start

```bash
git clone https://github.com/constructorfabric/insight-front.git
cd insight-front
pnpm install
pnpm dev
```

Open http://localhost:5173.

### Mock API

**Mocks are OFF by default** — `pnpm dev` talks to the Vite proxy (see `VITE_API_PROXY_TARGET`).

To enable synthetic data for an offline / demo session, copy `.env.example` to `.env.local` and set:

```
VITE_ENABLE_MOCKS=true
VITE_DEV_USER_EMAIL=bob.park@example.com
```

A yellow warning strip renders at the top of the page whenever mocks are active so synthetic values cannot be mistaken for real ones. Set `VITE_HIDE_MOCK_BANNER=true` to hide the strip during screenshots — mocks remain active. Prod builds (`pnpm build`) drop the mock subtree entirely.

Seeded mock people: `bob.park@example.com`, `carol.chen@example.com`, `alice.kim@example.com`, `frank.moss@example.com` (see [src/mocks/registry.ts](src/mocks/registry.ts)).

## Scripts

| Script | Description |
|---|---|
| `pnpm dev` | Start Vite dev server |
| `pnpm build` | Production build (`tsc -b && vite build`) |
| `pnpm preview` | Serve production build locally |
| `pnpm typecheck` | TypeScript strict check (`tsc --noEmit`) |
| `pnpm lint` | ESLint (zero warnings) |
| `pnpm format` | Prettier write |

## Project Structure

```
src/
  auth/                  # OIDC manager singleton, useAuth hook, start-url capture
  api/                   # Fetch clients (analytics, identity, accounts) + fetchWithAuth wrapper
  queries/               # React Query hooks per screen (ic-dashboard, team-view, executive-view)
  routes/                # TanStack Router file-based routes (auto-discovered)
  routeTree.gen.ts       #   ← auto-generated, do not edit
  screens/               # Page components composed by routes
  components/
    ui/                  #   shadcn/ui primitives (button, card, dialog, alert, …)
    widgets/             #   Feature widgets (metric-card, bullet-chart, drill-modal, …)
    app-sidebar.tsx      #   Org-tree sidebar (recursive nav)
    theme-provider.tsx   #   Light/dark/system theme (localStorage-backed)
    mock-banner.tsx      #   Warning strip when mocks are on
    app-error-boundary.tsx
    error-fallback.tsx
  hooks/                 # Shared hooks (use-period, use-mobile)
  lib/                   # Domain helpers (format, status, scoring, peers, …)
  mocks/                 # MSW handlers, factories, registry (dev-only, tree-shaken in prod)
  locales/en/            # i18next translation files
  i18n/                  # i18next setup
  types/                 # Shared TypeScript types
  index.css              # Tailwind v4 inline config + theme tokens (light + dark)
  main.tsx               # Entry: storeStartUrl → enableMocking → OidcManager.init → render
  router.ts              # createRouter(routeTree)
```

## Authentication (OIDC)

Authorization Code + PKCE via [`oidc-client-ts`](https://github.com/authts/oidc-client-ts). The OIDC issuer/client are not baked into the build — they're injected at container start.

### Flow

1. [src/main.tsx](src/main.tsx) calls `storeStartUrl()` (captures the full URL with any `?code=…&state=…` before the router strips it), then `OidcManager.init()` reads `window.__OIDC_CONFIG__` and restores a session from `sessionStorage` if one exists.
2. Root route's `beforeLoad` ([src/routes/__root.tsx](src/routes/__root.tsx)) inspects `authStore`. `authenticated` → render; `idle` / `expired` → `OidcManager.signIn()` (redirects to the IdP). `/callback` is whitelisted.
3. [src/routes/callback.tsx](src/routes/callback.tsx) calls `OidcManager.handleCallback(startUrl)` to exchange the code for tokens, then `window.location.replace(state.returnUrl)`.
4. [src/api/fetch-with-auth.ts](src/api/fetch-with-auth.ts) injects `Authorization: Bearer <token>` on every request. `X-Tenant-ID` is reserved for future use (current backend doesn't require it); when `authStore.tenantId` is populated by something downstream, the header fires automatically. On 401, it calls `OidcManager.refresh()` once and retries — concurrent in-flight refreshes are deduplicated inside `OidcManager.refresh()`.
5. Viewer identity is sourced directly from JWT claims via `oidc-client-ts` (`user.profile.email` / `user.profile.sub`) — no extra `/api/accounts/user/current` round-trip. The auth module's `useViewer()` hook is the single source of truth: it prefers the OIDC-authenticated user's email and falls back to `VITE_DEV_USER_EMAIL` in dev.

### Dev bypass

When `window.__OIDC_CONFIG__` is absent (typical for local dev) **and** `import.meta.env.DEV` is true, `OidcManager.init()` sets status to `authenticated` and resolves with no user. Combine with `VITE_DEV_USER_EMAIL` to impersonate a person from the identity service.

### Runtime config (Docker)

The container's `docker-entrypoint.sh` writes `/oidc-config.js` from env vars and injects a `<script src="/oidc-config.js">` tag into `index.html`. The script sets `window.__OIDC_CONFIG__ = { issuer_url, client_id, scopes }`.

| Variable | Description | Example |
|---|---|---|
| `OIDC_ISSUER` | OIDC issuer URL | `https://auth.example.com/application/o/insight/` |
| `OIDC_CLIENT_ID` | OAuth2 public client ID | `C6YjC67CCDBUMygEeoBIlSX3mhRkNpCPxQxa2zaT` |
| `OIDC_SCOPES` | Space-separated scopes | `openid profile email api://insight/Access.Default` |

## Environment Variables

Build-time (Vite, `.env.local`):

| Variable | Description |
|---|---|
| `VITE_ENABLE_MOCKS` | `"true"` to enable MSW (dev only; stripped from prod). |
| `VITE_HIDE_MOCK_BANNER` | `"true"` to hide the warning strip while mocks are on (for screenshots). |
| `VITE_DEV_USER_EMAIL` | Impersonate a person by email when no OIDC session is present. |
| `VITE_API_PROXY_TARGET` | Dev-only `/api` proxy target (e.g. `http://localhost:8080`). |
| `VITE_API_BASE` | Override analytics API base URL (default `/api/analytics/v1`). |
| `VITE_IDENTITY_BASE` | Override identity API base URL (default `/api/identity/v1`). |
| `VITE_ACCOUNTS_BASE` | Override accounts API base URL (default `/api/accounts`). |

Runtime (container only, **no** `VITE_` prefix): `OIDC_ISSUER`, `OIDC_CLIENT_ID`, `OIDC_SCOPES`.

## Routes

| Path | Screen | Notes |
|---|---|---|
| `/` | IC dashboard (or impersonation prompt) | Resolves viewer via OIDC user or `VITE_DEV_USER_EMAIL`. |
| `/ic/$person` | (redirects to `/ic/$person/personal`) | |
| `/ic/$person/personal` | IC dashboard | Branches on viewer department (engineering vs sales). |
| `/ic/$person/team` | Team view | Members table, bullet sections, drill modals. |
| `/ic/$person/exec` | Executive view | Org KPIs, health radar, teams table. |
| `/callback` | OIDC callback handler | Exchanges code for tokens, redirects to original URL. |

## Theming

`light` / `dark` / `system`. Theme tokens are CSS variables defined in [src/index.css](src/index.css); use the semantic Tailwind utilities (`bg-background`, `text-muted-foreground`, `border-border`, `text-destructive`, `bg-warning/10`, etc.). The shadcn theme is `base-vega` with `cssVariables: true` (see [components.json](components.json)).

## i18n

`i18next` + `react-i18next`. English-only today (`supportedLngs: ["en"]`). Translations live in [src/locales/en/translation.json](src/locales/en/translation.json); component code uses `const { t } = useTranslation()` + `t("key")`.

## Docker

### Build

```bash
docker build -t insight-frontend:local .
```

### Run with OIDC

```bash
docker run -d -p 8080:80 \
  -e OIDC_ISSUER=https://auth.example.com/application/o/insight/ \
  -e OIDC_CLIENT_ID=your-client-id \
  -e OIDC_SCOPES="openid profile email" \
  insight-frontend:local
```

### Run without a backend (mock mode)

```bash
VITE_ENABLE_MOCKS=true pnpm build
docker run -d -p 8080:80 insight-frontend:local
```

All screens render synthetic data and the warning strip stays visible.

### Docker Compose

```bash
cp docker-compose.yml docker-compose.override.yml
# Edit OIDC_ISSUER / OIDC_CLIENT_ID / OIDC_SCOPES in the override

docker compose up -d --build
```

### With Insight Backend (Kind cluster)

From the [insight monorepo](https://github.com/constructorfabric/insight):

```bash
./up.sh frontend    # builds image + deploys to Kind via Helm
./up.sh app         # backend + frontend together
./up.sh             # full stack (ingestion + backend + frontend)
```

Helm chart supports OIDC config via `--set oidc.issuer=… --set oidc.clientId=… --set oidc.scopes=…`.

## License

See [LICENSE](LICENSE) and [NOTICE](NOTICE).

# Insight Frontend

Frontend application for **Insight** — a decision intelligence platform for engineering analytics, productivity insights, bottleneck detection, AI adoption tracking, and team health visibility.

Built with HAI3 framework as a single-page application. Uses mock API layer for development; switches to real backend automatically in production.

- [Insight monorepo](https://github.com/cyberfabric/insight) (backend, infra, Helm charts)
- [Insight spec](https://github.com/cyberfabric/insight-spec) (connector specs, API contracts)

## Tech Stack

| Layer | Technology |
|---|---|
| Framework | HAI3 (React 19 + Redux + event-driven architecture) |
| Build | Vite 6 |
| Language | TypeScript 5 (strict) |
| Styling | Tailwind CSS 3 + HAI3 UIKit (Radix primitives) |
| Charts | Recharts 3 |
| Auth | OIDC via oidc-client-ts (Okta, Authentik, any OIDC provider) |
| Linting | ESLint + custom architecture rules |
| Package manager | npm 11 |
| Node | >= 25.1.0 (see `.nvmrc`) |

## Prerequisites

- Node.js >= 25.1.0 (`nvm use` will pick up `.nvmrc`)
- npm >= 11.6.0
- Docker (for container builds)
- Chrome (for browser-based dev tools via MCP)

## Quick Start

```bash
git clone https://github.com/cyberfabric/insight-front.git
cd insight-front
npm install
npm run dev
```

Open http://localhost:5173. HAI3 Studio panel (bottom-right) controls screenset, theme, and language.

### Mock API

**Mocks are OFF by default** — `npm run dev` talks to whatever your Vite proxy is pointed at (`/api/*` → `http://localhost:8080` by default, i.e. the real backend gateway port-forwarded from k8s).

To opt in to synthetic data for a demo / offline session, copy `.env.example` to `.env.local` and set:

```
VITE_ENABLE_MOCKS=true
```

Restart `npm run dev`. A yellow warning strip renders at the top of the page whenever mocks are active so you cannot mistake synthetic values for real ones. Prod builds (`npm run build`) drop the flag's branch entirely — there is no way for fake analytics data to leak into a deployed bundle.

## NPM Scripts

| Script | Description |
|---|---|
| `npm run dev` | Start Vite dev server with HMR |
| `npm run build` | Production build (generate colors + Vite) |
| `npm run preview` | Serve production build locally |
| `npm run type-check` | TypeScript strict check (`tsc --noEmit`) |
| `npm run lint` | ESLint with zero warnings policy |
| `npm run arch:check` | Architecture compliance (type-check + lint + dependency rules) |
| `npm run arch:deps` | Dependency graph validation via dependency-cruiser |

## Project Structure

```
src/
  app/                        # Application shell (shared across screensets)
    auth/                     #   OIDC manager, withAuth HOC, startUrl capture
    api/                      #   App-level API services (Auth, Identity, Accounts)
    layout/                   #   Layout component (sidebar, mobile nav)
    plugins/                  #   REST plugins (AuthPlugin — token injection)
    slices/                   #   App-level Redux slices (auth state)
    effects/                  #   App-level effects (auth lifecycle)
    events/                   #   App-level events (bootstrap, auth)
    themes/                   #   Theme definitions (default, dark, light, dracula)
    main.tsx                  #   Entry point — HAI3 app init, theme registration
    App.tsx                   #   Root component — Layout + AppRouter + StudioOverlay
  screensets/
    insight/                  # Main product screenset
      screens/
        executive-view/       #   Org-level KPIs, team health radar, metrics table
        team-view/            #   Per-team member table, bullet sections, drill modals
        ic-dashboard/         #   Individual contributor dashboard (KPIs, charts, AI tools)
        dashboard/            #   Legacy dashboard (dev)
        speed/                #   Speed gauge (dev)
      api/                    #   InsightApiService + ConnectorManager + IdentityResolution
      api/mocks/              #   Mock data fixtures (team members, IC data, drills)
      slices/                 #   Redux state per domain (executive, team, IC, period)
      actions/                #   Flux actions (emit events, call API services)
      effects/                #   Event listeners → slice updates
      events/                 #   Typed event enums per domain
      uikit/                  #   Screenset-local UI components (base + composite)
      i18n/                   #   36 language files (screenset-level)
    auth/                     # OIDC callback screenset
      screens/callback/       #   Handles /callback route after OIDC redirect
```

## Architecture

### Event-Driven (Flux)

All state flows through: **Component -> Action -> Event -> Effect -> Slice -> Store**

- **Actions** — pure functions that emit events and call API services. No `getState`, no direct dispatch.
- **Effects** — subscribe to events, update only their own slice. Registered via `registerSlice(slice, initEffects)`.
- **Events** — past-tense named, scoped to `screensetId/domainId/eventName`. Type-safe payloads via module augmentation.

### API Services

Each service extends `BaseApiService`, self-registers via `apiRegistry.register()`, and conditionally registers a `RestMockPlugin` when `VITE_ENABLE_MOCKS=true` (see `src/app/config/mocksEnabled.ts` — the single gate consulted by every service). Mock modules are `import()`ed lazily, so prod builds tree-shake the entire mocks subtree and the bundle never includes synthetic fixtures.

### Screens

| Screen | Path | Description |
|---|---|---|
| Executive View | `/executive-view` | Org KPIs, team health radar, team metrics table |
| Team View | `/team-view` | Per-team member metrics, bullet sections, drills |
| IC Dashboard | `/ic-dashboard` | Individual contributor KPIs, LOC charts, delivery trends |
| My Dashboard | `/my-dashboard` | Same as IC Dashboard, scoped to current user |

## Authentication (OIDC)

The app uses OpenID Connect (Authorization Code + PKCE) for authentication.

### Flow

1. `main.tsx` — captures start URL, initializes `OidcManager`
2. `withAuth` HOC — wraps protected screens, redirects to OIDC provider if no session
3. `/callback` screen — exchanges authorization code for tokens
4. `AuthPlugin` — injects `Bearer` token into all API requests
5. `authEffects` — listens for token/expiry events, updates `authSlice`

### Configuration

OIDC config is provided at **runtime** (not build time) via environment variables:

| Variable | Description | Example |
|---|---|---|
| `OIDC_ISSUER` | OIDC provider issuer URL | `https://auth.example.com/application/o/insight/` |
| `OIDC_CLIENT_ID` | OAuth2 client ID (public client) | `C6YjC67CCDBUMygEeoBIlSX3mhRkNpCPxQxa2zaT` |

The Docker entrypoint injects these into `index.html` as `window.__OIDC_CONFIG__` at container start. Without these variables, the app falls back to empty config (OIDC disabled).

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
  insight-frontend:local
```

### Docker Image (GHCR)

#### Prerequisites

1. Create a GitHub Personal Access Token (classic) with `write:packages` scope
2. If the repo is in an org, authorize the token for SSO (Settings → Developer settings → PAT → Configure SSO)
3. Log in to GHCR:
   ```bash
   echo "YOUR_TOKEN" | docker login ghcr.io -u YOUR_USERNAME --password-stdin
   ```

#### Build & Push

```bash
./scripts/ghcr-push.sh          # pushes as :latest
./scripts/ghcr-push.sh 0.1.0    # pushes as :0.1.0
```

Image: `ghcr.io/cyberfabric/insight-front`

#### Pull

```bash
docker pull ghcr.io/cyberfabric/insight-front:latest
```

### Run without a backend (mock mode)

For a self-contained demo with no backend, build with mocks enabled:

```bash
VITE_ENABLE_MOCKS=true npm run build
docker run -d -p 8080:80 insight-frontend:local
```

All screens render synthetic data and the yellow "MOCK DATA" strip stays
visible at the top of every page. Without `VITE_ENABLE_MOCKS=true`, builds
drop the mock plugins entirely and the UI tries to talk to a real backend.

### Docker Compose

```bash
# Copy and edit env vars
cp docker-compose.yml docker-compose.override.yml
# Edit OIDC_ISSUER and OIDC_CLIENT_ID in the override

docker compose up -d --build
```

Open http://localhost:8080.

### With Insight Backend (Kind cluster)

From the [insight monorepo](https://github.com/cyberfabric/insight):

```bash
./up.sh frontend    # builds image + deploys to Kind via Helm
./up.sh app         # backend + frontend together
./up.sh             # full stack (ingestion + backend + frontend)
```

Frontend is available at http://localhost:8000. Helm chart supports OIDC config via `--set oidc.issuer=... --set oidc.clientId=...`.

## Development Guidelines

This project uses HAI3 development guidelines. Before making changes:

1. Read `.ai/GUIDELINES.md` — routing table, import rules, type rules, blocklist
2. Route to the correct target file (SCREENSETS.md, EVENTS.md, API.md, STYLING.md)
3. Run checks before submitting:

```bash
npm run type-check    # TypeScript
npm run lint          # ESLint (zero warnings)
npm run arch:check    # Full architecture compliance
```

### Key Rules

- **Event-driven only** — no direct dispatch, no prop drilling
- **Self-registration** — services, screensets, slices register themselves
- **No `any`** — no `as unknown as`, no `eslint-disable`
- **No inline styles** outside `uikit/base/` — use theme tokens
- **lodash** for non-trivial array/object operations
- **Constants/enums** for all string IDs

### AI-Assisted Development

The project supports AI-assisted workflows via HAI3 CLI:

| Command | Description |
|---|---|
| `/hai3-new-screenset` | Scaffold a new screenset |
| `/hai3-new-screen` | Add a screen to existing screenset |
| `/hai3-validate` | Run architecture validation |
| `/hai3-quick-ref` | Common patterns reference |
| `/hai3-rules` | Show guidelines for a topic |

## Themes

Built-in themes: **Default**, **Light**, **Dark**, **Dracula**, **Dracula Large**.

Switch via HAI3 Studio panel or programmatically:

```ts
app.themeRegistry.apply(DARK_THEME_ID);
```

## i18n

36 languages supported. Two-tier system:
- **Screenset-level** — shared keys (menu labels, common terms)
- **Screen-level** — screen-specific translations

Translations use `I18nRegistry.createLoader` with lazy-loaded JSON files per language.

## License

See [LICENSE](LICENSE) and [NOTICE](NOTICE).

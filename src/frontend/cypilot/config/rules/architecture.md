---
cypilot: true
type: project-rule
topic: architecture
generated-by: auto-config
version: 1.0
---
# Architecture

<!-- toc -->

- [Layers](#layers)
  - [App Layer](#app-layer)
  - [Screenset Layer](#screenset-layer)
  - [Boundary Enforcement](#boundary-enforcement)
- [Bootstrap Sequence](#bootstrap-sequence)
  - [App Initialization](#app-initialization)
  - [Screenset Self-Registration](#screenset-self-registration)
- [Critical Files](#critical-files)
- [Source Layout](#source-layout)

<!-- /toc -->

## Layers

### App Layer
`src/app/` — authentication, layout, themes, bootstrap events, global API services. Only framework deps allowed: `@hai3/react`, `@hai3/uikit`, `react`, `react-dom`.
Evidence: `hai3.config.json:3`, `src/app/main.tsx:1-39`

### Screenset Layer
`src/screensets/{name}/` — self-contained vertical slices. Each screenset owns: screens, slices, effects, events, actions, hooks, api, components, i18n, uikit.
Evidence: `src/screensets/insight/insightScreenset.tsx:86-185`

### Boundary Enforcement
Cross-screenset imports are forbidden. Dependency-cruiser enforces regex boundary `^src/screensets/([^/]+)/`.
Evidence: `.dependency-cruiser.cjs:31-39`

## Bootstrap Sequence

### App Initialization
1. Import screenset registry (side-effect auto-discovery)
2. Register app-level slices with effects
3. Register app-level API services to apiRegistry
4. Create HAI3 app, register themes, render
Evidence: `src/app/main.tsx:10-72`

### Screenset Self-Registration
Screensets call `screensetRegistry.register()` at module scope. Registration includes: i18n loaders, slice+effects pairs, screenset config.
Evidence: `src/screensets/insight/insightScreenset.tsx:41-185`

## Critical Files

| File | Purpose | When to read |
|---|---|---|
| `src/app/main.tsx` | App bootstrap, service registration | Adding app-level services or slices |
| `src/screensets/screensetRegistry.tsx` | Screenset auto-discovery | Creating new screensets |
| `.dependency-cruiser.cjs` | Boundary enforcement rules | Modifying import rules |
| `hai3.config.json` | HAI3 framework config | Changing framework settings |
| `vite.config.ts` | Build config, code splitting, path aliases | Modifying build or adding vendors |
| `tailwind.config.ts` | Theme tokens, semantic colors | Adding or changing design tokens |

## Source Layout

```
src/
├── app/                    # Application core
│   ├── api/                # App-level API services
│   ├── effects/            # App-level effects
│   ├── events/             # App-level events
│   ├── layout/             # Layout, Menu, Footer
│   ├── plugins/            # REST plugins (Auth)
│   ├── slices/             # App-level Redux slices
│   └── types/              # App-level types
└── screensets/
    ├── screensetRegistry.tsx
    └── {name}/             # Vertical slice
        ├── ids.ts
        ├── {name}Screenset.tsx
        ├── actions/
        ├── api/
        ├── components/
        ├── effects/
        ├── events/
        ├── hooks/
        ├── i18n/
        ├── screens/{screen-name}/
        ├── slices/
        ├── types/
        └── uikit/
```
Evidence: `src/screensets/insight/` — full structure

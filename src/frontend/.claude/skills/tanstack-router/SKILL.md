---
name: tanstack-router
description: TanStack Router (v1+) reference for code-based routing, loaders, search params, navigation, link composition, route guards, and integration patterns. Use when working with files under src/ that import from '@tanstack/react-router', when creating or modifying routes/route trees, when designing data-loading via route loaders or beforeLoad, when adding type-safe Links/useNavigate/useParams/useSearch, when configuring RouterProvider or createRouter, or whenever the user mentions "tanstack router", "route loader", "useParams", "useNavigate", "search params", "beforeLoad", or "code-based routes". Also triggers for migration questions from react-router or other routers.
user-invocable: false
---

# TanStack Router

Reference docs split into focused sections. Read the section matching the task:

| File | When to use |
|---|---|
| [api.md](./api.md) | API surface: every export, type, hook, component. Use when you need exact signatures (`useNavigate`, `useRouterState`, `Link`, `redirect`, `createRoute`, etc.). |
| [routing.md](./routing.md) | Route tree composition: `createRootRoute`, `createRoute`, `addChildren`, path params (`$person`), splats, layout routes, route options. |
| [guide.md](./guide.md) | Deep-dive patterns: authenticated routes, search params, data loading via loaders, code splitting, custom search-param serialization, navigation, redirect semantics. |
| [installation.md](./installation.md) | Setup: install command, RouterProvider mount, TypeScript Register augmentation. |
| [setup-and-architecture.md](./setup-and-architecture.md) | Architecture rationale, type-safety model, loader-vs-on-mount tradeoffs, comparison with other routers. |

## Quick reference (most common patterns)

### Code-based route definition

```ts
import { createRootRoute, createRoute, createRouter, Outlet } from '@tanstack/react-router';

const rootRoute = createRootRoute({
  component: () => <Outlet />,
});

const personRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/ic-dashboard/$person',
  loader: ({ params }) => fetchPerson(params.person),  // runs BEFORE render
  component: PersonScreen,
});

const routeTree = rootRoute.addChildren([personRoute]);
export const router = createRouter({ routeTree });

declare module '@tanstack/react-router' {
  interface Register { router: typeof router }
}
```

### Reading route params + loader data inside a component

```ts
function PersonScreen() {
  const { person } = personRoute.useParams();
  const data = personRoute.useLoaderData();
  // ...
}
```

### Type-safe navigation

```ts
const navigate = useNavigate();
navigate({ to: '/ic-dashboard/$person', params: { person: email } });
```

### Type-safe Link

```tsx
<Link to="/ic-dashboard/$person" params={{ person: email }}>{name}</Link>
```

### Redirect from loader / beforeLoad

```ts
beforeLoad: ({ context, location }) => {
  if (!context.user) {
    throw redirect({ to: '/login', search: { redirect: location.href } });
  }
}
```

### Search params (typed, validated)

```ts
const route = createRoute({
  // ...
  validateSearch: (search: Record<string, unknown>) => ({
    page: Number(search.page ?? 1),
    sort: (search.sort as 'asc' | 'desc') ?? 'asc',
  }),
});
// In component:
const { page, sort } = route.useSearch();
```

## Loader-first data loading

Prefer route `loader` over on-mount React Query for primary screen data:

- Runs in parallel with code-split route component download → no waterfall
- Data available on first render, no `isPending` flash
- Pairs with React Query via `queryClient.ensureQueryData(...)` inside the loader

```ts
loader: ({ params, context: { queryClient } }) =>
  queryClient.ensureQueryData(personQuery(params.person)),
```

Then the component reads the same query via `useSuspenseQuery(personQuery(person))` — cache hit guaranteed.

See [guide.md](./guide.md) "Data Loading" section for full pattern.

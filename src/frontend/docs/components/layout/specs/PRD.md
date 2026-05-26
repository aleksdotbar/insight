# Layout

## Purpose

App shell. Mounts navigation on every page, owns responsive desktop / mobile composition, surfaces the active route in the navigation tree.

## Users

- **App user** — any signed-in viewer. Navigates between dashboards via the sidebar.

## Requirements

1. **Two-column desktop, sheet on mobile.** Below a fixed breakpoint the sidebar collapses into an off-canvas sheet. Same navigation content in both — no forked component.
2. **Org-tree navigation.** Recursive person tree, arbitrary depth. Root and active branches default open; manual expand/collapse persists per session.
3. **Active highlight.** Current route's person/team highlights; ancestor branches in the tree render with a softened active style so the viewer stays oriented at depth.
4. **One viewer, resolved at the root.** Viewer identity comes from auth state, not from per-screen env reads. Screens trust the viewer is available.
5. **Theme + i18n controls** live in the shell, not per screen.

## Non-goals

- Per-tenant shell customization.
- Right-to-left text testing beyond what UI primitives ship.
- Standalone shell preview surfaces.

## Acceptance

- Desktop and mobile render the same navigation; toggling between breakpoints never duplicates or drops state.
- Active highlight on a deep route lights every ancestor in the tree.
- Expand/collapse state survives navigation within the session.
- If the viewer can't be resolved in dev (no auth, no impersonation env), the shell shows an explicit diagnostic, not an empty tree. Prod never reaches this state — the router guard redirects first.

## Risks

- Org-tree depth keeps growing. Indent classes are enumerated, not template-built; new depth tiers need an explicit extension.
- Expand/collapse state is in-memory only; full reload resets it.

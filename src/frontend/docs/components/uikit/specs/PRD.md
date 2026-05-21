# UI Composites

## Purpose

Insight-owned visual building blocks (KPI strips, bullet sections, metric cards, trend charts, drill modals, period controls, placeholders) that every dashboard composes from. Encode metric semantics, threshold coloring, and null-handling once.

## Users

- **Consuming screen.** Every dashboard. Owns data + period; passes derived props down; receives intent through callbacks.
- **Indirect end user.** Never touches a composite directly. Experiences consistent accessibility, keyboard navigation, and threshold semantics across every screen.

## Requirements

1. **Stateless.** Composites receive data via props, raise intent via callbacks. No store reads, no event emissions, no API calls. Internal state only for ephemeral UI (popover open, calendar draft).
2. **Honest null.** Numeric values that arrive null render as em-dash or a placeholder, never coerced to zero. Applies to KPIs, bullets, trend series, tables.
3. **Universal placeholder.** One component covers all absent-data surfaces. Two states: *empty* (backend OK, no rows) and *error* (load rejected, retry CTA). The screen decides which; the composite never guesses.
4. **Threshold coloring via props.** Good / warn / bad colors flow in. Composites never look thresholds up themselves. Same coloring rules everywhere.
5. **Two render modes for bullets.** Compact card grid vs. full track + footer. Caller picks; both supported.
6. **Theme tokens only.** All styling via semantic theme tokens (background, foreground, primary, destructive, warning, success, etc.). No raw color utilities. No inline styles for static values.
7. **Per-card error isolation.** One card flipping to error never blanks its siblings.

## Non-goals

- Replacing or shadowing UI library primitives.
- Per-tenant theming.
- Visual regression / storybook infrastructure.

## Acceptance

- No composite imports application state, dispatches actions, or fires events.
- Every composite consuming numeric props renders the placeholder for null, never zero.
- One sibling card erroring leaves the others rendering normally.
- Period and view-mode controls are fully controlled — props in, callbacks out, no internal state for app data.

## Risks

- Drift between threshold definitions in the FE and the backend metric catalog post-migration. Treat FE thresholds as transitional.
- Boundary discipline (no state imports inside composites) is enforced by review, not code.

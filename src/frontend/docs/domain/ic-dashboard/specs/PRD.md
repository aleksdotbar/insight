# IC Dashboard

## Purpose

Per-person engineering view. The same view a person sees of themself and a team lead sees of them. Task delivery, git output, code quality, AI adoption, collaboration, trends, time-off context, drill on every bullet.

For sales personas the view branches into a sales-flavored layout (deal flow, win rate, year-over-year pacing). Engineering remains the default.

## Users

- **Self-viewing IC** — their own dashboard ("My Dashboard").
- **Team lead drilling down** — inspecting a direct report.
- **Executive drilling down** — continued drill chain from Team View.

## Requirements

1. **Person header.** Name, role, avatar, supervisor. Skeleton while loading. After load completes with no person, render a "not found" surface — never during loading.
2. **KPI strip.** Period-filtered KPIs. Null KPIs render honest null.
3. **Time-off banner.** Appears only when an absence overlaps the selected period. Informational; never adjusts metric values.
4. **Bullet sections.** Task delivery, git output, code quality, AI adoption, collaboration. Sections with all-null bullets render a placeholder instead of vanishing.
5. **Trend charts.** LOC composition over time, delivery activity over time. Empty series renders the placeholder; never silently zero-bars.
6. **Drill modal.** Opens from any bullet click. Closes cleanly; never persists across person or period changes.
7. **Period + view-mode controls.** Period change refires the load. View mode toggles compact tile layout vs. full chart layout.
8. **Per-section error recovery.** A single failed section shows an inline retry. Siblings continue rendering. One connector failure never blanks the whole screen.
9. **Sales variant.** When the viewer's department is sales, render the sales layout (hero KPIs in currency + win rate, year-over-year pacing band, velocity/quality bullets, outreach activity bullets, deal flow chart). Same route, branched at the dispatcher.
10. **Privacy footer.** Persistent privacy notice on the most person-identifying surface.

## Non-goals

- Personal customization that diverges from what a team lead sees of the same person. Side-by-side comparability across people in the same role is load-bearing — **explicitly rejected, not deferred**.
- Editing thresholds.
- Cross-person comparison (use Team View).
- Export, real-time updates.

## Acceptance

- The dashboard for a person is identical regardless of viewer (self / team lead / executive).
- Null KPIs, bullets, and trend series render honest null, never zero.
- A per-section failure surfaces an inline retry without affecting siblings.
- "Not found" renders only after a complete load with no person — never mid-load.
- Sales department triggers the sales layout deterministically; non-sales gets the engineering layout.
- Drill modal closes cleanly; no leaked state across person or period changes.

## Risks

- Personal customization layered over the role-default would break comparability across people. Rejected, not deferred.
- Snapshot bullets (e.g., "stale in progress") don't fit period-filtered semantics. Long-term: separate operational-health section.
- Threshold drift between local config and backend metric catalog post-migration.

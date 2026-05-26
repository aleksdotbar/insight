# Team View

## Purpose

Per-team picture. Team leads see who needs attention; executives drill in from the org view. Member-by-member metrics with section-level aggregates and per-member drill-downs.

## Users

- **Team lead** — supervisor of the team being viewed. Their email anchors the direct-reports filter.
- **Executive drilling down** — VP / department head inspecting a subordinate's team.

## Requirements

1. **Team hero strip.** KPI chips (at-risk count, below-focus count, no-AI count, dev-time median) computed client-side over the *visible* member set. Toggling the direct-reports filter refreshes them.
2. **Attention needed.** List of members tripping configured alert thresholds. Null metric values never trigger alerts — missing data is not a red flag.
3. **Members table.** Sortable, threshold-colored cells. Row click navigates to the person's IC Dashboard. Cell click opens a per-cell drill.
4. **Bullet sections.** Task delivery, code quality, estimation, AI adoption, collaboration. All-null section renders a placeholder instead of disappearing.
5. **Direct reports filter.** Toggle, default on, anchored on the team lead's email. Hidden when no anchor resolvable. Counter shows `(visible/total)`.
6. **Period control.** Predefined ranges + custom. Period change refires the load; previous data stays visible until the new load completes.
7. **Drill modal.** Opened from cell or section interaction; closes cleanly; never leaks across team or period changes.
8. **Navigation to IC Dashboard.** Row click sets the selected person and routes there. Current period is preserved.

## Non-goals

- Editing team membership or supervisor relationships.
- Defining alert thresholds.
- Cross-team comparison (lives in Executive View).
- Per-viewer column composition.
- Export.
- Real-time updates.

## Acceptance

- Hero strip recomputes when the direct-reports filter changes — never stale to the unfiltered set.
- Attention-needed never lists members whose triggering metric is null.
- Null cells in the table render as em-dash, never zero.
- Direct-reports toggle is hidden when no anchor email resolves.
- Drill modal closes cleanly; underlying screen state unchanged.

## Known limitation

- Bullet sections come from server-side per-team aggregates. The direct-reports toggle narrows the table + hero only; bullets remain whole-team in v1. The toggle's tooltip clarifies this.

## Risks

- Race when period changes mid-load — older response can overwrite newer state. Abort-on-change needed.
- Team identifier ambiguity (email when drilling from above, org-unit string when default). Anchor resolution branches on shape; canonicalization is a longer-term data-model task.

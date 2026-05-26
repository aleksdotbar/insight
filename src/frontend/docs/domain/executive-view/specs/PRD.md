# Executive View

## Purpose

Org-wide engineering health on one page. VPs and senior leaders see aggregate KPIs, every team in one table, outliers visible at a glance.

## Users

- **Executive viewer** — VP, CTO, head of department. Reads, never edits.

## Requirements

1. **Org KPI cards.** Averages across teams for build success, AI adoption, focus time. Null inputs filtered before the mean; all-null KPI renders honest null.
2. **Org health radar.** Multi-axis chart of org KPIs. While loading, render an empty placeholder of equivalent height — no layout shift.
3. **Per-team bar.** Comparison across teams along executive-scope metrics. Teams with all-null metrics still appear so their existence is visible.
4. **Teams table.** Sortable, threshold-colored cells. Null cells render as em-dash. Teams with no data are still listed.
5. **Period control.** Predefined ranges + custom date range. Period change refires the load; previous data stays visible until new data arrives (no flicker).
6. **Connector availability** (optional, recommended). When a metric is all-null org-wide, surface why — connector not wired vs. genuinely zero.
7. **Error surface.** Failed load shows a non-blocking error with retry. Period selection is preserved across retries.

## Non-goals

- Drill into team internals (lives in Team View).
- Drill into per-person detail (lives in IC Dashboard).
- Per-viewer customization of which metrics appear.
- Export.
- Real-time updates.

## Acceptance

- All-null KPIs render as em-dash, never zero.
- Period change keeps previous data on screen until new data arrives.
- Failed load preserves the selected period; retry recovers without page reload.
- Layout holds for orgs up to ~200 teams.

## Risks

- Org grows past the unvirtualized table limit. Pagination or virtualization needed when a real tenant trips it.
- Threshold drift between local config and backend metric catalog post-migration.

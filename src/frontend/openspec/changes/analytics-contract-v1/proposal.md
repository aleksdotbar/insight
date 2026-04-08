# Proposal: analytics-contract-v1

## Context

The frontend Analytics API contract was sketched in `add-insight-backend-api` as
five view endpoints (`/views/executive`, `/views/team`, `/views/ic/{personId}`, two
drill endpoints). That change established the shape of the response types but did not
trace each field to a backend data source.

The backend DESIGN (PR #49 in cyberfabric/insight) defines an OData generic endpoint
(`POST /api/v1/analytics/metrics/query`) but no screen-specific view endpoints.
The ingestion layer (Silver tables) was fully catalogued: GitHub, Bitbucket Cloud/Server,
Claude Code (Claude Team), Cursor, BambooHR, Zoom, M365, Jira (in progress).

This change produces a full analytics API contract that:
1. Confirms the view-endpoint architecture vs. OData (see Decision section)
2. Maps every response field to its Silver source table and formula
3. Marks fields by availability status so backend knows what to implement vs. defer
4. Covers all three views (Executive, Team, IC Dashboard) and drill endpoints

---

## Data Sources Inventory

| Source | Silver Table | Status | Connector PR |
|---|---|---|---|
| GitHub | `class_git_commits`, `class_git_pull_requests`, `class_git_pull_requests_reviewers` | ✓ available | #57 merged, #63 draft |
| Bitbucket Cloud | same `class_git_*` tables (data_source discriminator) | ⚠ in progress | #58 draft |
| Bitbucket Server | same `class_git_*` tables | ⚠ configured, no models | — |
| Claude Team (Code) | `class_ai_dev_usage` | ✓ available | #50 merged |
| Cursor | `class_ai_dev_usage` | ✓ available | (merged) |
| BambooHR | `class_people` | ✓ available | #47 merged |
| Zoom | `class_comms_events` | ✓ available | #61 merged |
| M365 | `class_comms_events` | ✓ available | (merged) |
| Jira | TBD (`class_tasks`?) | ⚠ in progress | #62 open |
| GitHub Actions (CI) | **no connector** | ❌ missing | — |
| Slack | `class_comms_events` | ✓ available | #48 merged |

---

## View Endpoints vs. OData

The backend DESIGN exposes `POST /metrics/query` (OData). The frontend does NOT use it
directly. Screen-specific view endpoints are proposed on top of the Analytics API
service for three reasons:

1. **Pre-aggregated positioning** — bullet chart visuals require `bar_left_pct`,
   `median_left_pct` etc. computed server-side from distribution data not available
   to the client.
2. **Single RTT** — each screen needs 15–40 fields in one call; OData would require
   many queries and client-side joins.
3. **Config co-location** — `TeamViewConfig` (alert thresholds, column thresholds)
   must travel with the data so the frontend is fully data-driven.

The backend team implements these as thin compositions over the OData/ClickHouse layer.

---

## What This Change Produces

- `proposal.md` — this file
- `design.md` — field-level source mapping for all view types
- `specs/analytics-api/spec.md` — updated frontend-facing contract (replaces existing)
- `tasks.md` — backend and frontend implementation checklist

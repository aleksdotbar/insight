# Design: analytics-contract-v1

**Change:** `analytics-contract-v1`
**Date:** 2026-04-08

---

## Field Source Map

Legend:
- ✓ **available** — Silver table exists and is populated
- ⚠ **pending** — connector exists, Silver model in progress
- ❌ **missing** — no data source exists yet

---

## Executive View

### `ExecTeamRow` fields

| Field | Type | Source | Silver Table / Formula | Status |
|---|---|---|---|---|
| `team_id` | string | Identity Service | `org_units.id` | ✓ |
| `team_name` | string | Identity Service | `org_units.name` | ✓ |
| `headcount` | number | BambooHR | `COUNT(*) FROM class_people WHERE org_unit_id = ? AND status = 'active'` | ✓ |
| `tasks_closed` | number | Jira | `COUNT(*) FROM class_tasks WHERE assignee_org = ? AND status = 'done' AND issue_type != 'Bug'` | ⚠ pending-jira |
| `bugs_fixed` | number | Jira | `COUNT(*) FROM class_tasks WHERE assignee_org = ? AND status = 'done' AND issue_type = 'Bug'` | ⚠ pending-jira |
| `build_success_pct` | number | GitHub Actions | `(successful_runs / total_runs) * 100` — no connector | ❌ missing |
| `focus_time_pct` | number | Zoom + M365 | `(work_days_h - meeting_h) / work_days_h * 100` where `meeting_h = SUM(duration_seconds)/3600 FROM class_comms_events WHERE person_id IN team` | ✓ |
| `ai_adoption_pct` | number | Claude Team + Cursor | `(COUNTDISTINCT person_id FROM class_ai_dev_usage WHERE org_unit = ?) / headcount * 100` | ✓ |
| `ai_loc_share_pct` | number | Claude Team | `SUM(lines_added) / total_loc * 100 FROM class_ai_dev_usage WHERE source = 'claude_team'` | ✓ |
| `pr_cycle_time_h` | number | GitHub / Bitbucket | `AVG(merged_at - created_at) IN HOURS FROM class_git_pull_requests WHERE author IN team AND merged_at IS NOT NULL` | ✓ |
| `status` | enum | backend | `'good'/'warn'/'bad'` computed from `ExecViewConfig.column_thresholds` | ✓ |

### `OrgKpis` fields

| Field | Source | Formula | Status |
|---|---|---|---|
| `avgBuildSuccess` | GitHub Actions | AVG(`build_success_pct`) across teams | ❌ missing |
| `avgAiAdoption` | Claude Team + Cursor | AVG(`ai_adoption_pct`) across teams | ✓ |
| `avgFocus` | Zoom + M365 | AVG(`focus_time_pct`) across teams | ✓ |
| `bugResolutionScore` | Jira | `bugs_fixed / (bugs_opened + 1)` normalized 0–100 | ⚠ pending-jira |
| `prCycleScore` | GitHub / Bitbucket | score derived from `pr_cycle_time_h` vs threshold | ✓ |

---

## Team View

### `TeamKpi` hero strip fields

| `metric_key` | Source | Formula | Status |
|---|---|---|---|
| `at_risk_count` | computed | `COUNT(members) WHERE any alert_threshold triggered` (from `config.alert_thresholds`) | ✓ |
| `focus_gte_60` | Zoom + M365 | `COUNT(members) WHERE focus_time_pct >= 60` | ✓ |
| `not_using_ai` | Claude Team + Cursor | `COUNT(members) WHERE no row in class_ai_dev_usage in period` | ✓ |
| `avg_pr_cycle` | GitHub / Bitbucket | `AVG(pr_cycle_time_h)` for team | ✓ |
| `total_loc` | GitHub / Bitbucket | `SUM(additions - deletions) FROM class_git_commits WHERE author IN team` | ✓ |

### `TeamMember` fields

| Field | Source | Silver Table / Formula | Status |
|---|---|---|---|
| `person_id` | Identity Service | `persons.id` | ✓ |
| `name` | BambooHR | `class_people.display_name` | ✓ |
| `seniority` | BambooHR | `class_people.custom_str_attrs['seniority']` or job_title mapping | ✓ |
| `tasks_closed` | Jira | `COUNT(*) FROM class_tasks WHERE assignee = ? AND status = 'done' AND issue_type != 'Bug'` | ⚠ pending-jira |
| `bugs_fixed` | Jira | `COUNT(*) FROM class_tasks WHERE assignee = ? AND status = 'done' AND issue_type = 'Bug'` | ⚠ pending-jira |
| `dev_time_h` | Zoom + M365 | `work_days_h - meeting_h` where `meeting_h = SUM(duration_seconds)/3600 FROM class_comms_events` | ✓ |
| `prs_merged` | GitHub / Bitbucket | `COUNT(*) FROM class_git_pull_requests WHERE author = ? AND merged_at IS NOT NULL` | ✓ |
| `build_success_pct` | GitHub Actions | `(successful_runs / total_runs) * 100` | ❌ missing |
| `focus_time_pct` | Zoom + M365 | `(work_days_h - meeting_h) / work_days_h * 100` | ✓ |
| `ai_tools` | Claude Team + Cursor | `ARRAY_AGG(DISTINCT source FROM class_ai_dev_usage WHERE person_id = ?)` | ✓ |
| `ai_loc_share_pct` | Claude Team | `SUM(lines_added) / total_loc * 100 FROM class_ai_dev_usage WHERE source = 'claude_team'` | ✓ |
| `trend_label` | GitHub / Bitbucket | rolling comparison of `prs_merged` or `focus_time_pct` across prior periods | ✓ |

### `BulletSection` / `BulletMetric` fields

Bullet metrics are per-person benchmark comparisons within the org unit.
The backend computes all visual positions from team distribution:

| Field | Source | Formula | Status |
|---|---|---|---|
| `value` | varies | see metric_key below | — |
| `range_min` / `range_max` | computed | P5 / P95 of org-unit distribution for the metric | ✓ |
| `median` | computed | P50 of org-unit distribution | ✓ |
| `bar_left_pct` | computed | `(value - range_min) / (range_max - range_min) * 100` | ✓ |
| `bar_width_pct` | computed | fixed width constant (e.g. 8%) or proportional | ✓ |
| `median_left_pct` | computed | `(median - range_min) / (range_max - range_min) * 100` | ✓ |
| `status` | computed | vs. `column_thresholds` | ✓ |

**Bullet metric_keys and their sources:**

| metric_key | Source | Silver Table | Status |
|---|---|---|---|
| `pr_cycle_time_h` | GitHub / Bitbucket | `class_git_pull_requests` | ✓ |
| `pr_review_time_h` | GitHub / Bitbucket | `class_git_pull_requests_reviewers` | ✓ |
| `focus_time_pct` | Zoom + M365 | `class_comms_events` | ✓ |
| `ai_loc_share_pct` | Claude Team | `class_ai_dev_usage` | ✓ |
| `loc_per_day` | GitHub / Bitbucket | `class_git_commits` | ✓ |
| `build_success_pct` | GitHub Actions | — | ❌ missing |
| `tasks_per_sprint` | Jira | `class_tasks` | ⚠ pending-jira |

---

## IC Dashboard

### `IcKpi` fields

| metric_key | Source | Silver Table / Formula | Status |
|---|---|---|---|
| `loc` | GitHub / Bitbucket | `SUM(additions - deletions) FROM class_git_commits WHERE author = ?` | ✓ |
| `ai_loc_share_pct` | Claude Team | `SUM(lines_added) / loc * 100 FROM class_ai_dev_usage` | ✓ |
| `prs_merged` | GitHub / Bitbucket | `COUNT FROM class_git_pull_requests WHERE author = ? AND merged_at IS NOT NULL` | ✓ |
| `pr_cycle_time_h` | GitHub / Bitbucket | `AVG(merged_at - created_at)` | ✓ |
| `focus_time_pct` | Zoom + M365 | `(work_h - meeting_h) / work_h * 100` | ✓ |
| `tasks_closed` | Jira | `COUNT FROM class_tasks WHERE assignee = ? AND status = 'done'` | ⚠ pending-jira |
| `bugs_fixed` | Jira | `COUNT FROM class_tasks WHERE assignee = ? AND status = 'done' AND type = 'Bug'` | ⚠ pending-jira |
| `build_success_pct` | GitHub Actions | — | ❌ missing |
| `ai_sessions` | Claude Team + Cursor | `SUM(session_count) FROM class_ai_dev_usage` | ✓ |

### Chart fields

#### `LocDataPoint` (per period point)

| Field | Source | Formula | Status |
|---|---|---|---|
| `label` | backend | period-correct: Mon/W1/Jan/Q1 | ✓ |
| `aiLoc` | Claude Team | `SUM(lines_added) FROM class_ai_dev_usage GROUP BY period_bucket` | ✓ |
| `codeLoc` | GitHub / Bitbucket | `SUM(additions - deletions) FROM class_git_commits GROUP BY period_bucket` | ✓ |
| `specLines` | — | **no source defined** — placeholder | ❌ missing |

#### `DeliveryDataPoint` (per period point)

| Field | Source | Formula | Status |
|---|---|---|---|
| `label` | backend | period-correct granularity | ✓ |
| `commits` | GitHub / Bitbucket | `COUNT FROM class_git_commits GROUP BY period_bucket` | ✓ |
| `prsMerged` | GitHub / Bitbucket | `COUNT FROM class_git_pull_requests WHERE merged GROUP BY period_bucket` | ✓ |
| `tasksDone` | Jira | `COUNT FROM class_tasks WHERE status = 'done' GROUP BY period_bucket` | ⚠ pending-jira |

### `TimeOffNotice`

| Field | Source | Status |
|---|---|---|
| `days`, `dateRange` | BambooHR `leave_requests` table | ✓ available |
| `bambooHrUrl` | BambooHR | ✓ available |

---

## Missing Data — Action Items

### ❌ CI/CD — `build_success_pct`

No CI connector exists for any git source. **Decision: return `null` for v1.**

CI data will be added as a separate change. See **CI Roadmap** section below.

### ⚠ Jira — `tasks_closed`, `bugs_fixed`, `tasks_per_sprint`

PR #62 open. Silver model (`class_tasks`) not yet designed. Fields can be returned
as `0` until connector merges, with `data_availability.tasks = 'pending'` in the response.

### ❌ `specLines` in LocDataPoint

No clear source for "spec lines". Options:
1. Map to markdown/docs files changed in commits (file extension filter)
2. Remove from v1 contract

**Recommendation:** Remove from v1, add when design docs pipeline exists.

---

---

## CI Roadmap

### Source systems

| Git source | CI system | API endpoint | Auth |
|---|---|---|---|
| GitHub | GitHub Actions | `GET /repos/{owner}/{repo}/actions/runs` | GitHub token (already used by git connector) |
| Bitbucket Cloud | Bitbucket Pipelines | `GET /repositories/{workspace}/{repo}/pipelines/` | App Password / OAuth (same as PR #58) |
| Bitbucket Server | Bamboo | `GET /rest/api/latest/result?buildstate=…` | Personal token |
| Bitbucket Server | Jenkins (common alternative) | `GET /job/{name}/builds` | Basic auth / token |

### Proposed `class_ci_runs` Silver table

Unified schema across all CI sources via `data_source` discriminator
(mirrors `class_git_*` pattern):

```sql
class_ci_runs (
  tenant_id          UUID,
  source_id          String,        -- e.g. 'insight_github', 'insight_bitbucket_cloud'
  unique_key         String,        -- tenant + source + run_id
  run_id             String,        -- native run/build ID
  pipeline_name      String,        -- workflow name (Actions) / pipeline slug (Pipelines)
  branch             String,
  commit_sha         String,        -- links to class_git_commits
  repo_name          String,
  triggered_by       String,        -- person email or 'scheduler'
  status             LowCardinality(String),  -- 'success' | 'failure' | 'cancelled'
  started_at         DateTime,
  finished_at        DateTime,
  duration_seconds   UInt32,
  person_id          UUID NULL      -- resolved in Silver step 2 via identity resolution
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(started_at)
ORDER BY (tenant_id, source_id, run_id);
```

### Derived metrics from `class_ci_runs`

| metric_key | Formula |
|---|---|
| `build_success_pct` | `COUNTIf(status='success') / COUNT(*) * 100` grouped by person_id / org_unit / period |
| `build_failure_rate` | `COUNTIf(status='failure') / COUNT(*) * 100` |
| `avg_build_duration_m` | `AVG(duration_seconds) / 60` |
| `builds_total` | `COUNT(*)` per person / team / period |

### Implementation plan

**Step 1 — GitHub Actions stream** (add to existing GitHub connector, PR #57 follow-up):
- New stream `workflow_runs`: `GET /repos/{owner}/{repo}/actions/runs`
- Incremental by `updated_at`
- Fields: `id`, `name`, `head_branch`, `head_sha`, `status`, `conclusion`,
  `created_at`, `updated_at`, `run_started_at`, `run_attempt`, `triggering_actor.login`
- Bronze table: `bronze_github.workflow_runs`

**Step 2 — Bitbucket Pipelines stream** (add to PR #58 before merge):
- New stream `pipelines`: `GET /repositories/{workspace}/{repo}/pipelines/`
- Incremental by `created_on`
- Fields: `uuid`, `state.name`, `state.result.name`, `target.ref_name`,
  `target.commit.hash`, `trigger.name`, `created_on`, `completed_on`, `duration_in_seconds`
- Bronze table: `bronze_bitbucket_cloud.pipelines`

**Step 3 — dbt Silver model** `class_ci_runs`:
- Union `github__workflow_runs` + `bitbucket_cloud__pipelines` via `union_by_tag`
- Map `conclusion='success'` / `state.result.name='SUCCESSFUL'` → `status='success'`
- Map `head_sha` / `target.commit.hash` → `commit_sha` for join with `class_git_commits`

**Step 4 — Identity resolution**:
- `triggering_actor.login` (GitHub login) / `trigger.name` (Bitbucket username)
- → resolve to `person_id` via `bootstrap_inputs` (alias_type = 'username')

### Impact on view endpoints

Once `class_ci_runs` is available, `build_success_pct` in all three views
changes from `null` to a real value. No frontend type changes needed —
`number | null` already handles both states.

`DataAvailability.ci` changes from `'no-connector'` to `'available'`.

---

## Config Object Design

### `TeamViewConfig` — stays as designed

```
alert_thresholds  → drives Attention Needed section
column_thresholds → drives MembersTable column coloring
```

Backend stores these in MariaDB (`Dashboard` / metric catalog) and returns them
as part of the view response. Frontend is fully data-driven — no hardcoded numbers.

### `ExecViewConfig` — stays as designed

```
column_thresholds → drives TeamsTable coloring (single threshold per metric)
```

---

## Period Granularity Contract

The backend returns chart data at period-correct granularity:

| period | points | labels |
|---|---|---|
| `week` | 5 | Mon, Tue, Wed, Thu, Fri |
| `month` | 4 | W1, W2, W3, W4 |
| `quarter` | 3 | Jan, Feb, Mar (or month names of the quarter) |
| `year` | 4 | Q1, Q2, Q3, Q4 |

The frontend renders `label` as the x-axis key directly. No client-side aggregation.

---

## Data Availability Envelope (v1 proposal)

To let the frontend show "Data not yet available" gracefully for missing sources,
each view response optionally includes:

```ts
type DataAvailability = {
  git: 'available' | 'no-connector' | 'syncing';
  tasks: 'available' | 'no-connector' | 'syncing';
  ci: 'available' | 'no-connector' | 'syncing';
  comms: 'available' | 'no-connector' | 'syncing';
  hr: 'available' | 'no-connector' | 'syncing';
  ai: 'available' | 'no-connector' | 'syncing';
};
```

Fields with `no-connector` source return `null` / `0` / `[]` depending on type.
The frontend can show a tooltip "CI data not configured" instead of misleading zeros.

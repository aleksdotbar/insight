# Tasks: analytics-contract-v1

## Phase 1 — Frontend: add DataAvailability type and null handling

- [x] Add `DataAvailability` type to `src/screensets/insight/types/index.ts`
- [x] Add `data_availability: DataAvailability` to `ExecViewData`, `TeamViewData`, `IcDashboardData`
- [x] Update `ExecTeamRow`: `build_success_pct: number | null`, `tasks_closed: number | null`, `bugs_fixed: number | null`
- [x] Update `TeamMember`: `build_success_pct: number | null`
- [x] Update `OrgKpis`: `avgBuildSuccess: number | null`, `bugResolutionScore: number | null`
- [x] Update mock fixtures to include `data_availability` with realistic values
- [x] Update `TeamsTable`, `MembersTable` to render `null` fields as "—" or "N/A"
- [x] Update `OrgKpiCards` to show tooltip "Not configured" when value is null

## Phase 2 — Frontend: metric key catalog

- [x] Add `METRIC_KEYS` constant object to `src/screensets/insight/types/index.ts`
  - [x] One entry per metric_key in the catalog (key, label, unit, sourceTag, v1Status)
- [x] Use `METRIC_KEYS` in `AttentionNeeded` `buildDescription()` instead of hardcoded strings
- [x] Use `METRIC_KEYS` in `BulletChart` tooltip labels

## Phase 3 — Backend proposal (shared with cyberfabric/insight team)

- [x] Open issue/PR in cyberfabric/insight proposing view endpoints — cyberfabric/insight-spec#67
  - [x] `/views/executive?period=`
  - [x] `/views/team?period=`
  - [x] `/views/ic/{personId}?period=`
  - [x] `/views/ic/{personId}/drill/{drillId}`
  - [x] `/views/team/drill/{drillId}?period=`
- [x] Attach source map and CI roadmap as backend implementation guide (§8 in proposal)
- [ ] Confirm `class_tasks` Silver schema with Jira connector author (PR #62)
- [ ] Confirm CI connector roadmap (GitHub Actions — no connector exists)

## Phase 4 — CI connector (cyberfabric/insight backend)

### GitHub Actions stream
- [ ] Add `workflow_runs` stream to GitHub connector (follow-up to PR #57)
  - [ ] `GET /repos/{owner}/{repo}/actions/runs` — incremental by `updated_at`
  - [ ] Bronze table: `bronze_github.workflow_runs`
  - [ ] Fields: `id`, `name`, `head_branch`, `head_sha`, `status`, `conclusion`,
    `created_at`, `run_started_at`, `duration_ms`, `triggering_actor.login`

### Bitbucket Pipelines stream
- [ ] Add `pipelines` stream to Bitbucket Cloud connector (before PR #58 merge)
  - [ ] `GET /repositories/{workspace}/{repo}/pipelines/` — incremental by `created_on`
  - [ ] Bronze table: `bronze_bitbucket_cloud.pipelines`
  - [ ] Fields: `uuid`, `state.name`, `state.result.name`, `target.ref_name`,
    `target.commit.hash`, `trigger.name`, `created_on`, `completed_on`, `duration_in_seconds`

### dbt Silver model
- [ ] Create `class_ci_runs` Silver table (union GitHub + Bitbucket via `union_by_tag`)
  - [ ] Normalize status: `conclusion='success'` / `state.result.name='SUCCESSFUL'` → `'success'`
  - [ ] Map `head_sha` / `target.commit.hash` → `commit_sha` for join with `class_git_commits`
  - [ ] Add `person_id UUID NULL` column (resolved in Silver step 2)
- [ ] Add `[ci]` tag to both staging models
- [ ] Add schema tests (not_null on `tenant_id`, `source_id`, `unique_key`)

### Identity resolution
- [ ] Register `triggering_actor.login` (GitHub) and `trigger.name` (Bitbucket)
  as alias_type `'username'` in `bootstrap_inputs`

### Frontend
- [ ] Verify `number | null` typing on `build_success_pct` handles live data correctly
  (no type changes needed — already nullable)
- [ ] Test `DataAvailability.ci = 'available'` path in `TeamsTable` / `MembersTable`

## Phase 5 — Sync spec to main

- [ ] Copy `specs/analytics-api/spec.md` from this change → `openspec/specs/analytics-api/spec.md`
- [ ] Archive this change

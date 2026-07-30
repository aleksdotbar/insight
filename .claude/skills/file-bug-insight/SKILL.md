---
name: file-bug-insight
description: "File an Insight defect as a GitHub issue in constructorfabric/insight — triage against existing issues, gather evidence, localize the fix to the layer that owns it, draft a report that reads in under a minute, confirm, create, and verify the metadata landed. Use whenever the user asks to file/report/raise/log a bug, ticket, defect or issue, and trigger PROACTIVELY once an investigation has converged on 'this is broken and should be recorded' — don't wait for the words 'file a bug'. Also fires on 'log this', 'report it', 'this is broken, make a ticket', 'turn this into an issue', 'we should file two bugs for X and Y'. The repo is PUBLIC, so the default flow is draft → confirm → create and the body must be scrubbed of internal detail. Prefer this over the general `file-bug` skill for anything in the Insight product — dashboards, metrics, connectors, dbt, ClickHouse, identity, the Helm install — since it carries the medallion evidence walk, the layer localization and the live board IDs; the general skill is for a Constructor *platform* defect that belongs in YouTrack."
disable-model-invocation: false
user-invocable: true
allowed-tools: Bash, Read, Write, Edit, Glob, Grep, Skill, Agent, AskUserQuestion
---

# File an Insight bug

Turn an observed defect into an issue someone else can act on in under a minute, carrying the evidence that proves it and a root cause traced from the real code.

**The tracker is always `constructorfabric/insight` on GitHub** — inside this repo there is no routing decision to make. (A Constructor *platform* bug — APS, Learn, Proctor, a platform stand's auth or navigation — goes to YouTrack instead; that routing lives in the general `file-bug` skill, not here.)

**The repo is public.** PRs close issues with `Closes #N`, so an issue here is outward-facing. Two consequences: draft → confirm → create is the default flow, and the body gets scrubbed before it goes anywhere.

## Coming from the QA fleet

A finding may arrive already carrying a `verdict`, an `existing_issue` and a `layer`. That shape is defined in `.claude/skills/explore-ui/references/finding-contract.md` — read it when the file is there, and don't stall when it isn't: the fields are self-describing, and a defect you found yourself has to clear the same four gates anyway.

- **`verdict` must be `CONFIRMED`.** An `UNVERIFIED` finding is a hypothesis, and filing one spends a reader's attention on a maybe. Reproduce it yourself first, or hand it to the `qa-finding-refuter` agent where the fleet is installed.
- **`existing_issue` must have been searched.** If it names an issue, comment on that issue instead of filing. If the match is *closed*, say so in the comment — a regression is more urgent than a new bug.
- **`layer: stand` is not a product bug.** A `join_use_nulls` view mismatch, a stale `schema_status` cache, an unseeded connector, a tenant mismatch — these are environment faults. File one only when the deploy path itself is the defect, and then it's a `deploy` bug about the chart or migration, not a metric bug.
- **`layer: unknown` is not filable.** Localize it first — walk the medallion by hand as below, or hand it to `qa-warehouse-analyst` where the fleet is installed — because the assignee and the grooming call both follow the layer.

## Companion skills

Each of these owns a slice of the work. Some are still being built out here, so check that one exists before relying on it, and fall back to the hand-run commands in this skill rather than stalling.

| Skill | Owns | Reach for it when |
|---|---|---|
| `playwright-cli` | the browser command surface — snapshots, refs, clicks, screenshots, console, network | exploring a stand or reproducing any UI defect |
| `drive-ui` | getting an *authenticated* browser on any stand — fakeidp and the `DEV_USER_EMAIL` seed locally, a passkey attach on a remote one — plus the routes and the evidence set | any UI defect, local or remote |
| `metric-parity` | the full bronze → silver → gold walk | localizing a wrong number to a layer |
| `release-verify` | install and seed health | settling "product bug, or empty instance?" |

One check belongs here rather than in `drive-ui`, because getting it wrong misroutes the bug: before calling a wrong on-screen value a frontend defect, look at the browser console and the API response behind it (`playwright-cli console`, then `requests` and `request <n>`). If the API already returned the wrong number, the layer is `analytics` or below and the UI is only the messenger.

## Triage — before you gather

Three checks that routinely change the plan.

**Search first.** Never file a blind duplicate:

```sh
gh issue list --repo constructorfabric/insight --state all --search "<key phrase>" --limit 100
```

**Keep the limit high.** Closed issues rank after every open one, so a short window returns open matches only — a search for "threshold" gives 30 open and 0 closed at `--limit 30`, and 57 open plus 43 closed at 100. A closed match is the *more* urgent finding, since it means a regression.

Search more than once with different vocabulary — the metric key, the field name, the group title, the error code, the user-visible label. Same defect → add your evidence to the existing issue. Genuinely different root cause or fix site → file new and cross-link with a one-line `related to #N` (a bare link, not a "how this differs" writeup — that reads as noise).

**Product bug, or environment artifact?** A metric that is empty because nothing was seeded or synced is not a product defect. File only what would still be wrong on a correctly populated instance. The cheapest check is the bottom of the medallion: no bronze rows for that connector and window means a seed or sync gap, so stop. (`release-verify` sweeps this for the whole install where it exists.)

**One bug or several?** One issue per distinct root cause and fix site. Split a shared symptom with different causes; use a fix checklist for several touch-points of the *same* fix.

## Gather evidence — never write from memory

Collect first, write second. The evidence must let someone else reproduce this.

**Artifacts do not go in this repo.** Nothing in this tree is gitignored for scratch output — `scratch/`, `tmp/`, `artifacts/` are merely untracked, so a screenshot or a body file left behind surfaces in someone's `git status` and rides along on the next `git add -A`. Write evidence and the issue body to the session scratchpad directory your environment names, or to a fresh `mktemp -d`; that is what the `--body-file` path below assumes. (`../insight-workspace/scratch/` also works when that checkout sits alongside this one.)

- **Data / metric bugs** — trace the medallion to where the value *first* goes wrong. Empty **bronze** means a sync or seed artifact, not a bug. Rows in bronze dropped at **silver** is a staging bug. Rows in silver but wrong in **gold** is a model or view bug. Don't file "gold is broken" when the story is "nothing upstream".
  ```sh
  CH=(docker exec insight-clickhouse clickhouse-client -u insight --password "${CLICKHOUSE_PASSWORD:-insight-local}")
  "${CH[@]}" -q "SELECT … FROM insight.<gold> WHERE …"                      # gold — served
  "${CH[@]}" -q "SELECT … FROM silver.class_<domain>_<entity> WHERE …"      # silver — dedup / identity
  "${CH[@]}" -q "SELECT … FROM bronze_<connector>.<table> WHERE …"          # bronze — raw ingest
  ```
  Every layer needs its database prefix: the client connects to `insight`, so an unqualified `class_*` resolves to the wrong database. The password is required — compose sets `CLICKHOUSE_PASSWORD` with `insight-local` as the default.
  For a remote stand: `../insight-workspace/scripts/ch.sh query --target <target> "<sql>"` (`ch.sh` lists its targets). Those three queries *are* the three-layer walk; `metric-parity` automates it where it exists.
- **UI bugs** — reproduce it in a browser first (`drive-ui` owns the stand and the browser; `playwright-cli` owns the commands), then lead with a tight annotated shot of the broken widget plus a contrast shot of something that renders correctly. The stand URL belongs in your commands, never in the issue.
- **Pipeline / config bugs with no UI** — the failure signal itself: the exact error and stack, or a row-count contrast that runs the code's own filter (returns 0) against the unfiltered count (>0). **If the failure is silent** — completes "successfully" with zero effect — say so explicitly. That is the key symptom.
- **What the metric is *supposed* to do** lives in `docs/domain/metrics/specs/DESIGN.md` and the model under `src/ingestion/`. Read the intent before calling behaviour wrong.

## Localize the fix from the actual code

Naming *where the fix lands* is what makes an issue actionable and routes it to an owner. Read and quote the real code; never infer a formula from a metric's name. Use "X-side, not Y-side" when it disambiguates a layer — *"client-side, not API-side: the API correctly returns 403; the SPA renders the menu entry unconditionally."*

Gold is defined in two places and they are not interchangeable: the dbt models in `src/ingestion/gold/` materialize the measure observation tables, while the `insight.*` views and marts are created by the migrations in `src/ingestion/scripts/migrations/`. Views get redefined across several migrations — grep them all and read the **latest-timestamped** one before quoting a formula.

Verify every reference against `main` before linking it; your worktree may differ:

```sh
gh api "repos/constructorfabric/insight/contents/<path>?ref=main" \
  -H "Accept: application/vnd.github.raw" | grep -nE '<pattern>'
```

## Type and priority

- **Issue Type = `Bug`** — the native type (`--type "Bug"`), never a `bug` label.
- **Priority is the Insight #40 project *field*, not a label.** Never add `priority:*`. Options: `Blocker` (blocks the next installable release), `High` (meaningful demo features), `Medium` (default). Suggest a level and confirm it.
- **Don't label.** Component, team, release and planning labels are applied during grooming by the people who own that call, and a wrong one routes the bug to the wrong team. Name the owning layer in Root Cause instead, in words.

Naming the layer is still your job — it just belongs in Root Cause, in words, not in a label:

| Fleet `layer` | Symptom shape |
|---|---|
| `frontend` | Correct in ClickHouse, renders wrong — axis, colours, series, formatting, null-vs-zero |
| `analytics` | 500s, wrong filter or bucketing in the serving path, wrong measure binding |
| `ingestion` | Wrong at or before gold: bad dedup, dropped rows, wrong view expression, schema drift |
| `ingestion` (identity) | People or org empty / mis-resolved, producer↔consumer mismatch |
| `stand` (deploy path only) | Broken on a fresh install: missing config or wiring, chart or secret gap |

The layer follows the *fix*, not the symptom — a wrong number from a gold view is an ingestion bug even though it surfaces in the UI.

## Body template — four headings

```markdown
## Summary
<ONE sentence: what is broken in product terms, and its consequence. Nothing else — no repro
detail, no history, no scope. A reader triaging a list often reads only this line.>

## Steps to Reproduce
1. <UI path, or the fastest isolated check — one query or command>
2. <what to observe>
3. <When the failure emits anything — exception, stack, HTTP status and body, dbt or ClickHouse
   error — paste it verbatim in a fenced block, trimmed to the lines that identify the defect.>

**Expected:** <one line>
**Actual:** <one line — the failure at that step, NOT a restatement of Summary>

<A runnable proof, or a matched comparison with one variable changed. If a field being *absent*
(bug) versus *present-but-null* (no data) is the distinguishing signal, say so — that one line
stops a reviewer waving off a real defect as missing data. If it only reproduces from a given
state, name the STATE ("a freshly migrated database"), never the environment.>

## Root Cause
<2–4 sentences. Name the file, view or expression and quote it, each reference linked to `main`.>

## Notes        ← optional, one line (e.g. `related to #N`)
```

**No `## Impact` heading.** It restates the Summary in longer words. Affected instances or states go next to the evidence in Steps; a knock-on effect is one line in Notes. Wanting the heading back means the Summary sentence is not carrying its weight.

Additive when they sharpen the report: an **Examples** table (current-wrong → correct) for a rule, threshold, sign or mapping bug; a **Fix checklist** with each site linked when the fix spans several places. Link code, don't paste strings: `[file.ext#L79](https://github.com/constructorfabric/insight/blob/main/<path>#L79)`.

## Write plainly

One idea per sentence. Short declarative lines a tired on-call reader parses on the first pass. If a sentence has more than one comma-joined clause plus a dash-aside, split it. State what happens, then why.

- ✗ *"Deploy-side, not migration-side: the hook is skipped/lost on a successful fresh install while Helm reports success, leaving the gold layer unbuilt (install-time logs were unavailable — the Job leaves no trace because it never ran)."*
- ✓ *"The post-install hook never runs on a fresh install. Helm still reports success. No hook Job or Pod is ever created, so the gold layer stays unbuilt. The fix is deploy-side — the migration script works when run by hand."*

**Say each fact once.** Every fact lives in exactly one section. Repetition teaches the reader to skim, and skimming is how the one load-bearing line gets missed.

**Title = the plain, user-visible symptom.** No metric IDs, table or column symbols, or migration names — those live in the body. Don't append the diagnosis as a trailing clause, don't reach for filler adverbs, and don't use a qualifier the reader can't resolve from the title alone ("after a database migration" — which one?).

- ✗ *"YouTrack sync is reported failed and its transforms are skipped even though the data synced successfully"* → ✓ *"YouTrack sync is reported failed and its transforms never run"*
- ✗ *"A connector sync fails **outright** when the previous sync is still running"* → ✓ drop `outright`; "fails" already says it.

**No prescribed fix and no acceptance criteria** — that is the assignee's call. Describe an expected result in plain language and point at the prototype as the source of truth; don't specify exact colours or pixel values.

## Worked example

A real filed bug, condensed. Read it for calibration on how little text a complete report needs.

> **Adding a threshold to a metric makes its threshold list fail permanently**
>
> ## Summary
> Once a metric has its first threshold, every read of that metric's thresholds fails, so thresholds can no longer be viewed, edited or removed.
>
> ## Steps to Reproduce
> 1. Create a metric, then `POST /v1/metrics/{id}/thresholds` with any valid body.
> 2. Read them back: `GET /v1/metrics/{id}/thresholds`.
> 3. Both calls return 500 `application/problem+json`, and the log names the decode:
>    ```
>    failed to list thresholds error=Query Error: error occurred while decoding column
>    "value": mismatched types; Rust type `core::option::Option<f64>` (as SQL type
>    `DOUBLE`) is not compatible with SQL type `DECIMAL`
>    ```
>
> **Expected:** 201 with the created threshold, then 200 with the list.
> **Actual:** 500 on the create and on every later read of that metric's thresholds.
>
> The row is inserted despite the 500 — `SELECT field_name, operator, value FROM thresholds` returns it. The write path works and the read path does not, which is why one successful-looking create disables the endpoint for good. Reproduces on a freshly migrated database with no other data.
>
> ## Root Cause
> The `value` column is `DECIMAL(20,6)` ([`m20260414_000001_init.rs#L86`](https://github.com/constructorfabric/insight/blob/main/src/backend/services/analytics/src/migration/m20260414_000001_init.rs#L86)) but the entity maps it to `f64` ([`entities.rs#L39`](https://github.com/constructorfabric/insight/blob/main/src/backend/services/analytics/src/infra/db/entities.rs#L39)), and sqlx-mysql cannot decode `NEWDECIMAL` into `f64` — the `column_type = "Decimal(…)"` annotation on the field does not change how the value is read back. Writes coerce server-side; reads fail during decode, and the handler wraps that as internal ([`handlers.rs#L1070`](https://github.com/constructorfabric/insight/blob/main/src/backend/services/analytics/src/api/handlers.rs#L1070)). The newer admin threshold table reads the same shape safely by casting in raw SQL.
>
> ## Notes
> `metric_threshold` (admin) is unaffected — different read path.

Three things that example gets right, and they are the ones reports usually miss. The title is a symptom a user could have reported, with the diagnosis left for Root Cause. The "row is inserted despite the 500" line is load-bearing — without it a triager reads a 500 as a flaky write and moves on. And Root Cause stops at *where the defect is*, naming the safe read path as a hint without prescribing the fix.

## Scrub the body

Keep **out**: internal hostnames of any kind, the phrase "dev stand", cluster and kube context names, workspace paths (`wiki/…`, `scratch/…`), JWTs, tokens, credentials, and exact data values tied to a real person (genericize `14,753` → "~14.7k"; use `jane.doe@corp.com`).

Keep **in**: the repo's own code references — file paths, view, table and column names, API routes. Those *are* the product and are what make the bug actionable.

## Confirm before creating

Show the title, the type, the priority you propose and the rendered body, then wait — unless the user said "just create it". Creation is not a draft: the repo is public and watchers are notified the moment the issue exists, so a wrong title or an unscrubbed line is already out. This is also where the priority gets settled, since it is your suggestion until the user picks one.

## Create, board, priority, images

Write the scrubbed body to a file **outside this repo** — never inline a multi-line body.

```sh
BODY="$(mktemp -d)/bug-body.md"   # or a path under the session scratchpad dir

# 1. Create — native Type=Bug, NO labels (grooming applies those), NO bug label
gh issue create --repo constructorfabric/insight \
  --type "Bug" --title "<title>" \
  --body-file "$BODY"

# 2. Add to the Insight board — idempotent; auto-add is unreliable
gh project item-add 40 --owner constructorfabric --url <issue-url>

# 3. Set the Priority FIELD. Parse with jq, NOT python — issue bodies carry control chars
ITEM=$(gh project item-list 40 --owner constructorfabric --limit 800 --format json \
  | jq -r --argjson n <ISSUE_NUMBER> '.items[] | select((.content.number // -1)==$n) | .id')
gh project item-edit --project-id PVT_kwDOERGOus4Ba9e9 --id "$ITEM" \
  --field-id PVTSSF_lADOERGOus4Ba9e9zhVxXAs \
  --single-select-option-id <Blocker=79628723 | High=0a877460 | Medium=da944a9c>
```

Verify those IDs with `gh project field-list 40 --owner constructorfabric` if an edit fails.

**Images — the honest constraint.** GitHub has **no API to upload an image to an issue**, and `gh` cannot do it either; the web UI uploads via drag-drop. Create the issue with the body, then tell the user to drag the PNGs into the description box. Don't imply they will be attached automatically. For a data or pipeline bug the inline query proof is usually the evidence and no screenshot is needed.

## Verify what landed

```sh
gh issue view <n> --repo constructorfabric/insight --json title,labels,body,url
gh api repos/constructorfabric/insight/issues/<n> --jq '.type.name'          # → "Bug"
```

Confirm: Type is `Bug`; no `bug` or `priority:` label; the body renders and is grep-clean of internal detail. The `item-add` and `item-edit` calls above already report whether the board and Priority field took, so don't re-read them. Report the URL with a one-line summary.

Don't self-assign, and don't post a status comment unless asked. On this board, moving an issue is a separate decision — *To Verify* means development is done and awaiting validation, *Done* means QA verified it, and publishing that claim is the user's call.

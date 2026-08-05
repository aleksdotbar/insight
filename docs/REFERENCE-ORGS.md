# Reference Organisations

Defines the organisation sizes Insight is tested against, how much data each one holds, and the
rules for building a fixture that matches. It exists so that a performance or soak fixture can be
built to scale, latency budgets are comparable run to run, and a cost-of-ownership figure has a
denominator.

Figures are derived from measuring **two production installations** with identical SQL and are
stated here as project standards. Companion documents: [`TESTING.md`](TESTING.md) (where these
fixtures run) and [`product/VISION.md`](product/VISION.md) §6.4.2 (the company-size tiers).
Tracked in #2215 (reference organisations) and #2216 (recommended resource requirements).

---

## 1. Scope and purpose

The quality framework names one measurement fixture — *a reference organisation of ~1,000 users
with a typical connector mix* — shared by the **Efficiency** vector (#1785: compute footprint →
cost of ownership) and the **Performance** vector (#1787: P95 per data endpoint), and records its
connector and concurrency profile as an open input. [`TESTING.md`](TESTING.md) §7 places load,
stress and soak in the **Test** and **Beta** stages, and #1655 asks for latency budgets per
week / month / quarter / year data slice.

Neither states **how much data** a reference organisation holds. Without that, a fixture cannot be
built to scale, latency budgets cannot be compared run to run, and a TCO figure has no denominator.
This document supplies those numbers.

## 2. How organisation size is defined

> **Organisation size is counted in ACTIVE people only.** People marked terminated in the HR
> system of record are excluded from every denominator, every per-person figure and every
> extrapolation.

The distinction is load-bearing, not cosmetic: in one measured installation **71 % of person
records were terminated**, so quoting record counts would have overstated the organisation by
**3.7×**.

Four populations exist and must never be substituted for one another:

| Population | Definition | Used for |
|---|---|---|
| **Active people** | HR system of record, active status, distinct work emails | **the org size** — every density figure |
| Person records incl. terminated | all rows in the people model | storage accounting only |
| Directory accounts | active accounts in the identity directory | identity-store sizing, authentication load |
| Identities present in activity data | distinct actors per metric class | fixture population per class |

The last one is the trap: measured per-class activity identities run **0.07× to 1.77×** the active
roster. Ratios **above 1.0** are correct, not errors — external meeting attendees, service
accounts, shared mailboxes, automation and departed employees still attached to historical records
all author rows. A fixture populated only from the employee list will never reproduce them, and
identity resolution is precisely what they stress.

## 3. The reference organisations

Aligned to the company-size tiers in [`product/VISION.md`](product/VISION.md) §6.4.2. One reference
organisation per tier, placed at the tier ceiling so that passing at the reference covers the tier
below it.

| Tier | Span (people) | Reference org | Active people | Confidence |
|---|---|---|--:|---|
| Small teams | 5–50 | **REF-S** | 50 | extrapolated **down** 8–10× |
| Mid-size organizations | 50–500 | **REF-M** | 500 | **near-measured** |
| Large organizations | 500–5,000 | **REF-L** | **3,000** | extrapolated 5.8–7.7× |
| Enterprise organizations | 5,000+ | *not defined* | — | outside the evidence base |

**REF-L is the primary fixture.** At 78 % into the Large band it represents the tier's upper half
rather than its floor, and it matches the 3,000-user dataset already scoped for the shared load
harness, so one fixture serves every dependent scenario. **The cost is confidence:** 3,000 active
people is a **5.8–7.7× extrapolation** from installations of 392 and 521 active people, against
1.9–2.6× for a 1,000-person fixture — this is the largest single source of error in this document.
Note that the quality framework currently names *"~1,000 users"*; REF-L = 3,000 supersedes that
number and the framework text needs updating to match (tracked in #2215).

**REF-L covers the tier's upper half but not its ceiling.** A 5,000-person installation is 1.7×
REF-L, and no evidence in this study reaches either point directly. Figures for 5,000 people appear
below as an informational extrapolation only — not a defined reference organisation.

### 3.1 Volume per reference organisation

Logical rows and uncompressed bytes — what a generator must emit. The two figures are the observed
range between the two measured installations, which differ in connector mix; they are not
competing estimates of one quantity.

| Reference org | Active people | Window | Rows | GiB uncompressed | GiB on disk (LZ4) |
|---|--:|---|---|---|---|
| **REF-S** | 50 | 91 days | 0.33 – 0.46 M | 0.19 – 0.32 | 0.04 – 0.07 |
| | | 365 days | 1.3 – 1.8 M | 0.75 – 1.27 | 0.17 – 0.28 |
| **REF-M** | 500 | 91 days | 3.3 – 4.6 M | 1.9 – 3.2 | 0.42 – 0.70 |
| | | 365 days | 13 – 18 M | 7.5 – 12.7 | 1.7 – 2.8 |
| **REF-L** | 3,000 | 91 days | 19.5 – 27.6 M | 11.3 – 19.0 | 2.5 – 4.2 |
| | | 365 days | 78 – 111 M | 45 – 76 | 8.7 – 17.0 |
| *(informational)* | 5,000 | 365 days | 131 – 184 M | 75 – 127 | 17 – 28 |

Scaling is linear in active headcount. Both coefficients, all layers combined:
**71.6–100.9 logical rows** and **44.3–74.7 kB uncompressed** per active person per day.

### 3.2 Size is not only headcount

[`product/VISION.md`](product/VISION.md) §6.4.2 states it directly: *"Scale is defined not only by employee count, but also by
number of connected systems, repositories, work items, events, products, services, roles, teams,
and years of retained history."* The measurements bear this out sharply — the two installations
differ by up to **14× per active person** on entity inventory while their activity **rates** agree
within 1.62×.

**Headcount predicts event volume well and entity inventory badly.** Entity counts are therefore
fixture **inputs**, chosen and recorded, not derived from headcount:

| Entity | Observed per active person | REF-L (3,000) |
|---|---|--:|
| Repositories | 3.5 – 30.5 | 10,500 – 91,500 |
| Wiki pages | 2.7 – 38.1 | 8,100 – 114,300 |
| Work items (issues) | 182 – 380 | 546,000 – 1,140,000 |
| Agile boards / sprints | no person axis | 40 – 500 |
| Connected systems | — | 8 – 11 |
| Years of retained history | — | 10.2 – 17.6 (real installs) |

A 392-person organisation carrying ~12,000 repositories and 17 years of issue history is not a
"mid-size" workload in any sense a load test cares about. Record the entity inventory alongside
the headcount whenever a reference organisation is cited.

## 4. Typical organisation data

Everything an organisation produces, per **active** person, with a 1,000-person column. The range
is the observed span between two real installations — read it as *"a real organisation lands in
here"*, never as a mean or a distribution.

| What | Per active person | At 1,000 active people *(×3 for REF-L)* |
|---|---|---|
| **People** | | |
| Active people (org size) | 1 | 1,000 |
| Person records incl. terminated | 3.7 – 5.5 | 3,700 – 5,500 |
| Directory accounts | 1.0 – 2.8 | 1,000 – 2,800 |
| Identities per metric class | 0.07 – 1.77 | 70 – 1,770 |
| Identity-store rows | 71 – 146 | 71,000 – 146,000 |
| **Entities** | | |
| Repositories | 3.5 – 30.5 | 3,500 – 30,500 |
| Wiki pages | 2.7 – 38.1 | 2,700 – 38,100 |
| Work items | 182 – 380 | 182,000 – 380,000 |
| Connected systems | — | 8 – 11 |
| Years of history | — | 10.2 – 17.6 |
| **Daily activity** (logical rows/day) | | |
| Issue change events | 5.7 – 9.6 | 5,700 – 9,600 |
| Git file changes | 4.6 – 5.2 | 4,600 – 5,200 |
| Chat | 0.73 – 1.55 | 730 – 1,550 *(13.9–15.2 messages/person/day)* |
| Git commits | 0.76 – 1.44 | 760 – 1,440 |
| Email activity | 0.70 – 1.10 | 700 – 1,100 |
| Task comments | 0.32 – 0.82 | 320 – 820 |
| Worklogs | 0.17 – 0.91 | 170 – 910 *(1.81–6.80 h per entry)* |
| Document activity | 0.50 – 0.66 | 500 – 660 |
| Meetings | 0.37 – 0.58 | 370 – 580 |
| Issues created | 0.34 – 0.41 | 340 – 410 |
| Pull-request comments | 0.22 – 0.49 | 220 – 490 |
| Pull requests | 0.12 – 0.23 | 120 – 230 |
| AI dev usage | 0.11 – 0.20 | 110 – 200 *(49–281 accepted lines per adopter-day)* |
| Wiki edits | 0.08 – 0.17 | 85 – 170 |
| Wiki pages created | 0.015 – 0.021 | 15 – 21 |
| AI chat | 0.030 – 0.046 | 30 – 46 |
| **Volume** | | |
| Rows/day, all layers | 71.6 – 100.9 | **72,000 – 101,000** |
| Bytes/day, all layers | 44.3 – 74.7 kB | **42 – 71 MiB** |
| 12-month dataset (logical) | 15.4 – 26.0 MiB | **26–37 M rows · 15–25 GiB** |
| What an installation actually holds | 61.7 – 110.7 MiB | **60 – 111 GiB** |
| Re-emission multiplier (bronze) | — | **1.58× – 8.36×** |

**The last two volume rows are the point.** The *logical content* of a 1,000-person organisation is
15–25 GiB. What its ClickHouse actually stores is 60–111 GiB. The difference is ReplacingMergeTree
re-emission plus retained history — not user activity. That single gap drives both the TCO figure
and the soak workload.

## 5. REF-L — the fixture specification

Single-valued. Where the two installations disagreed, REF-L takes the **higher** value: under-
provisioning makes a P95 budget look achievable when it is not, while over-provisioning only costs
disk. Where the higher value comes from a **connector artefact** rather than human behaviour, the
class is sized on the human-rate figure that transfers and the artefact is applied as a named,
visible multiplier.

| Dimension | REF-L |
|---|--:|
| Active people | **3,000** |
| Person records incl. terminated | 16,500 |
| Directory accounts | 8,400 |
| Repositories | 91,500 |
| Wiki pages | 114,300 |
| Work items | 1,140,000 |
| Connected systems | 10 |
| History span | 365 days |
| **Rows (365 d, logical)** | **122,500,947** |
| **Uncompressed** | **89.43 GiB** |
| **On ClickHouse disk (LZ4)** | **18.63 GiB** |
| Rows per active person-day | 111.9 |
| Identity-store rows (`identity_inputs`) | 438,000 |
| Identity persons store | 248,000 |
| Re-emission multiplier, if sync-replayed | 8.36× |

**Minimum viable variant — 91 days:** 30,541,332 rows / 22.30 GiB uncompressed / 4.64 GiB on disk
(bronze 6.69 M · staging 7.81 M · silver 5.94 M · gold 10.09 M · identity 17 k). This is the window
every coefficient was measured over, so it is the least extrapolated fixture available. Use it when
regenerating a 365-day fixture is too expensive.

### 5.1 Connector mix

| # | Slot | Pick | Load-bearing for |
|--:|---|---|---|
| 1 | HR system of record | BambooHR | **36 of 49 gold views** resolve through `insight.people`, a view over the HR employees table. Non-negotiable |
| 2 | Second directory | MS Entra | Produces the people union and the person-in-two-directories case that identity resolution must handle |
| 3 | Issue tracker | Jira | Largest class either way, and the heavier: **80.6 kB per logical issue vs 19.2 kB** for the alternative. Also the only genuine silver fan-out observed |
| 4 | Git | GitLab | The only complete git measurement available, and the only source of a PR-review stream |
| 5 | Wiki | Outline | 2.01× the row emission of the alternative for identical human effort |
| 6 | Chat | any one, **sized in messages** | Row emission differs 2.12× between products; **message volume agrees within 9 %** |
| 7 | Email + documents | M365 | The only connector measured identically on both installations |
| 8 | Meetings | Zoom | Widest identity axis observed (1.77× the roster — external attendees) |
| 9 | AI dev | Claude Team + Cursor | Higher line volume; Cursor is the only per-call cost event stream |
| 10 | CRM + support | HubSpot | Without it, 6 gold CRM views and 3 support views scan empty tables |

**Deploy the empty bronze schemas too.** A real installation deploys ~25 bronze databases and
populates ~9. Gold views reference tables that may be empty; a fixture that omits unconfigured
connectors' schemas fails to resolve views that a real installation resolves-but-returns-nothing.

### 5.2 Classes that must not be scaled by headcount

| Class | Scale by instead |
|---|---|
| Git branches | repository count (observed 40–380 branches per active person — repo inventory, not headcount) |
| Sprints / boards | flat, 40–500 |
| CRM contacts | a target customer count — these are **external** people |
| CRM accounts, deals | the **sales sub-roster** (measured adoption 7.1 % and 4.4 %) |
| Support tickets | flat organisation rate — the source has no owner field |
| Wiki engagement | page count (grain is page × day) |
| Identity inputs | accounts per person × rows per account |
| Dimension tables | flat — organisation-shaped, not headcount-shaped; they do **not** shrink in a small fixture |

### 5.3 Coverage gaps — absent, not zero

AI-API usage and support events are **deployed and empty on both** measured installations. No
coefficient exists for them anywhere in the evidence base; they must not be interpolated. The
ClickHouse alias table is empty on every installation observed — the live store is the identity
service's relational alias map.

## 6. Build rules

1. **Logical vs as-stored.** Every figure above is logical: one row per real event. A running
   installation stores **1.58×–8.36×** more, because ReplacingMergeTree re-emits each logical row
   on every connector sync. A **bulk-loaded** fixture sees none of that; a **sync-replayed** one
   sees all of it. State which method the fixture uses and, if replayed, which multiplier it
   targets. There is no basis for a value in between.
2. **History ceilings.** Four classes cannot honestly fill 365 days: email and document activity
   cap at **122–137 days** (usage-report retention at the source), HR events and identity records
   at the connector install date. Cap the class and document it, or generate the tail and label it
   synthetic. **Never extend history by linear pro-rata** — activity grows over time, so
   back-filling early years at today's rate overstates them. Taper.
3. **Scale down by truncating payloads, not rows.** No gold object reads the dominant bronze table;
   fitting its JSON blob columns to a realistic length distribution removes **~29 GiB of 89 GiB
   with zero P95 impact**. Dropping rows is invalid — row count drives the merge and scan cost the
   P95 budget measures.
4. **Do not size gold from its disk footprint.** Gold is ~1 % of disk but **2.3 % of everything the
   server decompresses** (10.4× compression against bronze's 4.4×), and it is the only layer on the
   request path. Sizing it from disk under-provisions the measured layer by more than 2×.
5. **Realism markers.** Weekend share must be **1.4–5 %** on event classes; distributions must be
   heavy-tailed at **p99/p50 of 5–30×**. Flat distributions and 25–31 % weekend share are the
   signature of un-calibrated synthetic data.

## 7. Soak

At REF-L the connectors add **335,619 logical rows/day (251 MiB/day)** — **+0.27 %/day** against a
122.5 M-row fixture. **Dataset growth is not the soak workload; merge and deduplication are.** Under
sync-replay, bronze grows ~432 MiB per 24 h and a 7-day soak adds ~3 GiB of stored bronze for
~360 MiB of logical content. Instrument part counts, not dataset size.

**Identity.** The identity service's existing NFR specifies a 24 h soak at 100 RPS against a
**50,000-row** dataset, and separately a p95 bound *"for tenants under 50,000 persons"*. These are
**not the same scale — they differ 11–17×**. 50,000 rows in the persons store corresponds to
605–1,246 active people — **only 20–42 % of REF-L**, i.e. mid-size scale. Either resize that dataset
to **120 k–248 k rows** to match REF-L, or keep 50,000 and re-label it explicitly as a mid-size
fixture. Separately, *"under 50,000 persons"* implies a **565 k–864 k-row** store, 2.3–4.7× REF-L's
and a different fixture again. The identity store grows ~63 rows/day at REF-L (0.03 % of itself):
its soak is a **memory/GC test, not a growth test**.

## 8. Acceptance checks

A REF-L fixture is correct when:

1. The people model holds **3,000 active** and **16,500 total** person rows.
2. Per-class distinct active identities match the target ratios — **including the classes above
   1.0** (meetings ≈1.77×, email ≈1.63×).
3. Per-active-person-week medians land in band: issues created 3–4, git file changes 19–31, email
   6–7, issue change events 25–30.
4. Weekend share is 1.4–5 % on event classes.
5. Distributions are heavy-tailed (p99/p50 of 5–30×), not flat.
6. 36 of 49 gold views resolve non-empty; unconfigured-connector views resolve and return zero.
7. Totals land on **122.5 M rows / 89.4 GiB** at 365 days, or **30.5 M rows / 22.3 GiB** at 91 days.

## 9. Confidence and limits

* **Evidence base is two production installations**, of 392 and 521 active people — both within
  30 % of the mid/large tier boundary. REF-M is near-measured; **REF-L is a 5.8–7.7× extrapolation** and is the largest single source of
  error in this document; REF-S is an 8–10× extrapolation downward; 5,000 people has no support.
  A third measured installation above 1,000 active people would reduce the error more than any
  other work.
* **Density per person transfers across products; density per table does not.** Issue creation
  agrees to 1.20× across two different issue trackers, git file changes to 1.13× across two git
  products, chat messages to within 9 %. Of 22 classes measured on both, 15 agree within 2×, median
  1.62×. **None of the 7 disagreements is a difference in how people work** — every one is a
  property of the connector: emission policy, record grain, ingest completeness or automation share.
* **The transfer test is arithmetically circular.** Both coefficients are per-person-per-day and
  headcount is the only multiplier, so the prediction error equals the coefficient ratio by
  construction. It measures transferability, not correctness, and four of the 15 passing classes
  carry uncorrected asymmetric contamination. Read 1.62× as *"same order of magnitude, connector
  effects removed"*, not as an error bar.
* **Concurrency and query mix are not measured at all.** The 100 RPS figure is inherited from an
  existing NFR and has no evidence behind it for the metric endpoints. This is the largest gap
  between this document and a runnable performance plan.
* **Both measured installations are ~80 % one connector and ~50 % four columns.** Any total is
  dominated by one product's record shape. A fixture that spreads its volume evenly across metric
  classes models a system that does not exist.

## 10. Open decisions

1. Whether the 5,000-person tier ceiling is gated at all — it needs evidence not yet collected.
2. The entity inventory per reference organisation (repositories, wiki pages, work items), which
   headcount does not determine.
3. Fixture window: 365 days, or the 91-day least-extrapolated variant.
4. Build method: bulk-load, or sync-replay and which re-emission multiplier.
5. Whether CRM and support are in scope — decides whether 9 gold views are ever exercised.
6. Whether the 50,000-*person* identity boundary is gated, which needs a second dataset.
7. The concurrency profile — still unmeasured, and still the framework's open product input.

## 11. Scan volume per data slice

`TESTING.md` §7 and #1655 both express Performance as latency per **week / month / quarter / year**
data slice. At REF-L those slices are this much work. Gold is the only layer on the request path;
silver is shown because some drill-downs reach it.

| Slice | Gold rows | Gold uncompressed | Silver rows | Silver uncompressed |
|---|--:|--:|--:|--:|
| Week (7 d) | 775,975 | 189 MiB | 457,048 | 161 MiB |
| Month (30 d) | 3,325,608 | 810 MiB | 1,958,778 | 689 MiB |
| Quarter (91 d) | 10,087,677 | 2.40 GiB | 5,941,626 | 2.04 GiB |
| Year (365 d) | 40,461,562 | 9.63 GiB | 23,831,797 | 8.18 GiB |

Per-day rates behind it, at 3,000 active people: gold 110,854 rows / 27.0 MiB · silver 65,293 /
23.0 MiB · staging 85,813 / 31.1 MiB · bronze 73,471 / 169.9 MiB.

The **ratio** matters more than the absolute: a year slice is a ~40 M-row / ~9.6 GiB read against a
**52× smaller** week slice, and that spread is what a per-slice latency budget has to survive. Per
active person a drill-down is **259 gold rows** for a week and **13,487** for a year.

Slice sizes are stable across a soak: the fixture grows 335,619 logical rows/day (251 MiB/day), only
**+0.27 %/day**. What moves under sync-replay is part count, not scan volume.

**Not derivable from this data:** request rate, concurrency, query mix, cache-hit behaviour, and how
many metric calls a dashboard load issues. Those must be set by measurement or by an explicit,
recorded assumption.

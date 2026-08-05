# Constructor Insight — Main Scenarios by Persona

**Status:** draft for review · Companion to [VISION.md](VISION.md)

The vision says what Insight is and what it can do. This document says **who is standing in front of
it, what they are there to do, and how far each of them may reach**. It adds no capability and changes
no commitment.

- **Personas** — the four target user groups of VISION §6.1.
- **Scenarios** — ten, in three tiers, ordered by what the product is for.
- **Underneath** — the detailed user scenarios (Appendix A): thirty concrete questions, one person
  asking one thing at one moment, each traced to the scenario it belongs to.

## The three tiers

| Tier | What it is | Scenarios |
|---|---|---|
| **Main** | Review metrics, analyse them, reach conclusions. This is what the product is for. | S-1, S-2, S-3 |
| **Secondary** | Build new views, explore, take the output elsewhere, compare with the outside world. | S-4, S-5, S-6 |
| **Service** | Set the product up, keep it configured, keep it running. | S-7, S-8, S-9, S-10 |

The order is by importance to the reader, not by build order. Technically the service tier comes
first — sources and identity have to be right before a metric can be — but a customer does not buy
setup, and a scenario list that opens with configuration describes an installation rather than a
product.

**Why split by persona.** A capability is not one scenario. "Review metrics" is a single tier, but it
means three different things:

- an **individual contributor** sees their own work against a department or cohort median, and no team
  metrics at all;
- a **team manager** sees their team, the people in it by name, and groups recalculated for that team
  rather than carried over from the organization;
- an **executive** sees the whole organization.

The activity is shared. The boundary is not, and the boundary is the part that gets lost in
implementation. That is why it is written down as a statement rather than left implied.

**How to read a scenario block**

- **The scenario** — what holds for everyone, whoever is looking.
- **Per persona** — what each of them does, and what must never happen to them. Only the personas the
  scenario concerns are listed.
- **Not this** — what the scenario deliberately does not do, so it is not promised in a demo.
- **Detail** — the user-scenario IDs from Appendix A that belong here.

---

## 1. Personas

The four target user groups of VISION §6.1, with the short codes used throughout.

| Code | Group (VISION §6.1) | Arrives asking |
|---|---|---|
| **EXEC** | Executives and portfolio leaders | "Did it get better, or did we just get busier?" |
| **LEAD** | Functional leaders and team managers | "Where exactly is work blocked, and what can I do?" |
| **IC** | Functional teams and individual contributors | "How does my own work look, and what is in my way?" |
| **ADMIN** | Data stewards and administrators | "Which of these numbers can be trusted, and who may see them?" |

Finance and product management are not separate personas here. Their questions are carried by these
four: cost by EXEC, forward-looking planning by EXEC and LEAD. VISION §6.2 lists nine functions —
engineering, product, design, DevOps, QA, support, sales, marketing, finance — and a functional lead
in any of them is a LEAD.

### 1.1 Reach

The boundary each persona carries into every scenario below.

| | EXEC | LEAD | IC | ADMIN |
|---|---|---|---|---|
| **How far they see** | The whole organization | Their own team, and no further | Themselves | Settings, not people's data |
| **How far they zoom** | Organization, function, team, person | Team, sub-team, group, their own reports | Themselves, with the department or cohort as a median | n/a |
| **People by name** | Anyone, where person-level access is granted | The people reporting to them — that is what a team view is for | Themselves | Only while resolving who is who |
| **Comparison** | Between functions, teams and people | Between groups inside their own team, and between their own reports | Against a median, never against a named colleague | n/a |
| **Cost figures** | Where granted | Where granted | No | Where granted |
| **Conclusions and advice** | Reads conclusions | Reads conclusions, receives recommendations | Neither | Neither |
| **Never** | The underlying records · a default view that ranks people against one another · a number with no statement of coverage and confidence | Anything outside their own team · a default view that ranks their reports · group figures carried over from the organization instead of recalculated for the team | Any other person's activity · any team metric beyond the median they are placed against · their own place in a ranking | Administrative rights do **not** carry the right to see data — each kind is granted separately (VISION §9) |

**Naming and ranking are different things, and only one is restricted.** People are named wherever
person-level access has been granted: a manager's team view names their reports, and that is the point
of it. What VISION §3 rules out is a *default* view that ranks named individuals against one another,
or an unexplained productivity score. The line is the default surface and the granted scope — not the
name.

---

# 2. Main — review, analysis, conclusions

The loop the product exists for: measure → diagnose → recommend → validate (VISION §1).

## S-1 · Metrics review · Main

**The scenario.** Someone opens the product to see how things are going. Every number carries a
governed definition, unit, granularity, confidence and stated limitations, and is worked out the same
way for every persona and every scope (VISION §8.4).

**EXEC** — See how the organization as a whole is doing.
- organization, function and team, with change over time
- coverage counted from people who actually have data behind the figure, never by treating missing
  data as zero
- never a default view that ranks people against one another

**LEAD** — See how their team is doing and where work is stuck.
- their own team at any depth, and the people in it by name
- groups recalculated for the team in view, not carried over from the organization
- a group too small to keep an individual unidentifiable is not shown at all
- never a team they do not manage

**IC** — See their own work, with a reference point.
- their own activity, flow and AI usage
- the department or cohort as a median to place themselves against
- never another person's activity, never a team metric beyond that median, never their own rank

**ADMIN** — Nothing by default.
- never gains data visibility implicitly from administrative rights

**Not this.** No single "value of AI" number: seat-based and usage-based cost are not summed into one
figure, and unattributed cost stays its own line rather than being spread across the rest
(VISION §6.2.9, §11.4).

**Detail.** B1 (own context), B2 (team over a period), B3 (where work is stuck), B4 (knowledge
concentration), B5 (organization roll-up), B6 (AI cost), B8 (cost of coordination).

## S-2 · Analysis and diagnosis · Main

**The scenario.** The product stops showing shape and starts asserting a relationship — bottlenecks,
risks, anomalies, cost drivers, quality issues, role and activity mismatches — and says which kind of
claim it is making, with confidence and limitations (VISION §8.5). It rests on lineage: work is
followed across the systems it passes through, and **lineage comes before attribution** (VISION §7.3)
— what cannot be traced is shown as an evidence gap, never converted into a confident claim.
Forward-looking estimates (feasibility, cost, time, risk) come from the organization's own history and
are shown as extrapolations, not guarantees.

**LEAD** — Understand why, not just what.
- a conclusion for their team or cohort, with the evidence behind it
- where the chain of evidence breaks in their own area, and what would repair it
- never a verdict about a named individual
- never a conclusion drawn from a group too small to protect one

**EXEC** — The same at organization level, plus a forecast for proposed work.
- cost and outcome followed to the function, team, product or service — as far as the trail goes
- never attribution stronger than the lineage supports
- never a causal claim where the evidence supports a correlation
- never a forecast presented as a guarantee

**ADMIN** — See which evidence gaps and known defects limit a conclusion.

**IC** — Not an audience for diagnosis, and never a named example inside one.

**Not this.** "AI sped up development by X%" is not a claim Insight makes; what it can say is that a
cohort with high usage differs from one with low usage in stated ways, correlationally — with the word
said on the surface, not in a footnote. Attribution also has a ceiling, and the ceiling is stated
rather than worked around: "this change was written by AI" and "this change cost $N" are not claims
Insight makes.

**Detail.** C1 (AI gain and price together), C2 (review as a bottleneck), C3 (did quality hold),
C4 (support load vs release), C5 (sales activity vs pipeline), C6 (cost moved rather than fell),
E1 (assess a feature before starting), E2 (where to invest next).

## S-3 · Conclusions: recommendation and validation · Main

**The scenario.** A recommendation is a structured improvement object (VISION §1): observed problem,
affected area, evidence and confidence, recommended action, owner, expected metric movement, and the
follow-up window used to check it. Its origin is declared — evidence-derived from the customer's own
data, or heuristic. Afterwards the product reads the outcome from the measured system.

**LEAD** — Get an action, not an observation.
- one lever they can own, with what should move, which guardrail must not slip, and when it is checked
- never a recommendation that passes judgement on a named individual rather than on a process, team or
  cohort — a named *owner* is expected, a named *subject* is not
- never a recommendation whose origin is unstated

**EXEC** — Know whether it worked.
- whether the lever moved, whether the outcome moved, and the honest fourth answer: not enough data
- never a result assembled from metrics chosen after the fact — they are fixed when the recommendation
  is issued

**ADMIN** — Configure which recommendation families are enabled, who owns them, and how validation
windows are defined.

**IC** — Never the subject of a recommendation.

**Not this.** No surveys, and no self-reported productivity as an input. Validation is read from the
measured system. Insight recommends; it does not execute (VISION §13.3).

**Detail.** D1 (a recommendation, not an observation), D2 (did it work, a month later), D3 (context
the system cannot see).

---

# 3. Secondary — new views, exploration, reuse

Everything here extends the main loop. None of it is where a customer starts.

## S-4 · Dashboards, views and exploration · Secondary

**The scenario.** Someone builds a view rather than reading one: composing dashboards from the metric
and recommendation catalog, slicing by an attribute, defining a cohort, drilling from a figure into
what produced it, saving the result and sharing it (VISION §8.7, §9). Exploration moves the question,
never the boundary.

**LEAD** — Build the view their team actually needs.
- compose from the catalog; slice by attribute; define cohorts and comparison groups
- drill from any figure to what it was computed from
- groups recalculated for whatever is on screen at the time
- never an exploration path that reaches outside their own team

**EXEC** — Build the portfolio view.
- the same, at organization and function level

**IC** — Explore their own context only.

**ADMIN** — Curate what can be built.
- which metrics and thresholds exist, which cohorts are valid, who may publish a shared view

**Not this.** A shared or saved view is not a way around access rules: what a viewer sees is
re-evaluated for that viewer, so the same saved dashboard shows each person only what they may see. A
view carries the definitions and coverage of the metrics in it, not bare numbers.

**Detail.** B7 (compare cohorts), and the slicing side of B2–B5.

## S-5 · Sharing and reuse · Secondary

**The scenario.** A number keeps its meaning when it leaves the product — views, summaries, APIs and
governed data access carry the definition, coverage and confidence with the number (VISION §8.8).

**ADMIN** — Take Insight's output elsewhere.
- API and governed data access, under the same access rules that apply inside the product
- never a number stripped of its definition and confidence, so that outside the product it becomes a
  fact without caveats

**LEAD, EXEC** — Use a conclusion in their own report or review.

**Detail.** G2 (use conclusions in another system).

## S-6 · External comparison · Secondary

**The scenario.** Comparison against the organization's own history by default; opt-in comparison
against peers and public data where enabled. Every benchmark declares its source, cohort definition,
coverage and confidence (VISION §8.9, §12).

**EXEC** — Know whether a number is bad or normal.
- own history first, which requires sharing nothing
- peer comparison only where the customer has opted in

**ADMIN** — Turn participation on and off; it is revocable.

**Not this.** Raw customer data never leaves the customer boundary. Only anonymized aggregates at
cohort, team or organization level are shared — never individual data, never stack ranking
(VISION §3, §12.2).

**Detail.** F1 (are we slow, or is this normal).

---

# 4. Service — setup, configuration, operation

None of this is why anyone buys the product, and all of it has to work before the rest does.

## S-7 · Sources and evidence coverage · Service

**The scenario.** Whatever the wiring state, the product says so plainly: what is connected, what that
unlocks, and — where an answer is not possible — the cause and the smallest set of fixes with the
largest gain in confidence (readiness mode, VISION §7.5). Each source declares its fields, window,
freshness, blind spots and the level it supports (VISION §10.10).

**ADMIN** — Know what can be proven.
- every evidence category as connected, partly connected or absent, and which metrics each unlocks
- freshness per source
- never left to guess why a screen is empty

**LEAD, EXEC** — Meet a gap and know what to do about it.
- the cause named directly: which source is missing, which identities are unresolved, which link is
  broken — plus the minimal fix
- never a comparison across two periods whose source windows do not overlap

**IC** — The same guarantee on their own context.

**Not this.** No zero in place of missing data — a zero looks like a measurement and raises a false
alarm. And no "rough estimate for now": the honest answer to a missing source is what is missing.

**Detail.** A1 (what can be proven), A4 (no answer, but what to connect), C8 (event in the business or
break in the data).

## S-8 · Identity, roles and organization model · Service

**The scenario.** Someone who appears separately in code, tickets, chat and the HR system is
recognised as one person, with a stated confidence. The customer can correct the result, the correction
survives the next sync, and role and team history is preserved so past periods are not recalculated
under a model that was not valid at the time (VISION §8.2, §7.2).

**ADMIN** — Sort out who is who.
- sees what was matched automatically, where the system is unsure, and where it got it wrong
- merges and splits reversibly
- defines roles, several roles per person, and role history
- never loses a manual correction to the next sync

**LEAD, EXEC** — Trust the tree they roll up into.
- never a subtree quietly reshaped by re-resolution, with past periods recalculated underneath it

**IC** — Be one person, not four.
- never has their work attributed to a duplicate of themselves

**Not this.** Where observed work does not match the configured role model, Insight recommends
changing the configuration — not the person (VISION §9).

**Detail.** A2 (identity queue), A3 (roles and expected activities), C7 (role vs observed work).

## S-9 · Configuration and access · Service

**The scenario.** The customer configures roles, activities, sources, metrics, thresholds, cohorts,
dashboards, localization and access rules (VISION §9). Access to raw, people-level, aggregate, cost and
recommendation data is role-based and policy-controlled, and the boundary holds on every surface.

**ADMIN** — Decide who gets what.
- grants the five kinds of data one by one
- adapts roles, metrics and thresholds without engineering involvement
- sets language, date, number, currency and timezone rules
- never holds all five kinds implicitly; a refusal is enforced underneath, not only hidden on screen

**LEAD** — Their own team, in full.
- every depth inside it
- never one level up, and never sideways — the limit is structural, not a filter on a screen

**EXEC** — The organization as a whole.
- aggregates, and people where person-level access is granted
- never the underlying records

**IC** — Themselves.
- named to their own management chain and to anyone else granted person-level access for their part of
  the organization — and to nobody outside it

**Not this.** Insight is read-only towards connected systems: it writes its own configuration and
annotations, nothing else (VISION §13.3).

**Detail.** G1 (who sees what), G3 (language and timezone), and the configuration half of A3.

## S-10 · Deployment, upgrade and migration · Service

**The scenario.** The product is installed, updated, upgraded and — where it replaces something —
migrated into, without losing what already worked. Deployment models differ (Constructor-hosted,
customer cloud, private cloud, customer-operated), and in all of them customer data stays under
customer control (VISION §1, §14.1, §15.2).

**ADMIN** — Run it.
- install, configure, update and upgrade a customer-operated deployment
- inventory what exists first, then keep / rename / replace / retire
- import history where retention allows, and check parity over an agreed period
- never lose a surface that worked before an upgrade
- never discover a broken screen from a user

**EXEC** — Replace the previous system with confidence.
- parity stated openly, including what could not be reproduced
- never a parity claim that quietly omits it

**LEAD, IC** — Keep their history across the change.
- never a past period silently recalculated under a new model

**Not this.** Insight does not require Constructor to have default access to customer data in order to
operate (VISION §1).

**Detail.** G4 (migrate off a legacy system).

---

## 5. Rules that hold in every scenario

From the vision, stated once here instead of repeated in each scenario.

1. **Evidence gaps are shown, not hidden** (§3, §7.5) — never a zero for missing data, never an
   approximate estimate in place of an answer.
2. **Confidence and limitations travel with every conclusion** (§3) — a strong finding, a directional
   signal and an instrumentation problem stay distinguishable.
3. **Lineage before attribution** (§7.3) — untraceable work is a gap, not a quiet claim.
4. **No default ranking of named individuals, and no unexplained productivity scores** (§3) — people
   are named where person-level access has been granted; the ranking is what is ruled out.
5. **People-level access is role-based and policy-controlled** (§3, §9) — five kinds of data, granted
   separately, enforced where the data is produced rather than on the screen.
6. **Cost movement is preserved, not folded away** (§11.4) — a local saving that shifts cost, risk or
   effort downstream is shown as a shift; seat-based and usage-based cost are never one figure.
7. **Insight observes and advises; people act** (§13.3) — it writes its own configuration and
   annotations, nothing else.
8. **Clean room** (§12.2) — raw data stays inside the customer boundary; only anonymized aggregates
   are shared, opt-in and revocable.
9. **Role and activity are separate axes** (§7.2) — expected role model and observed activity are
   compared, never conflated; history is kept under the model valid at the time.
10. **A group too small to keep an individual unidentifiable is not shown** — the threshold protects a
    person from being identified through a cohort.

---

## Appendix A · The detailed user scenarios

Thirty concrete questions — one person, one question, one moment — from the product scenario draft of
2026-08-04. They are the level at which features are specified and tested; the scenarios in §2–§4 are
what all of them must obey. Availability is deliberately not recorded here: it changes per release and
per customer, since the connected source set differs.

| ID | The question behind it | Who asks | Scenario |
|---|---|---|---|
| B1 | What is visible about me, and what is in my way? | IC | S-1 |
| B2 | What changed for my team over the period? | LEAD | S-1 |
| B3 | Where is work stalling? | LEAD | S-1 |
| B4 | What falls apart if a specific person drops out? | LEAD, EXEC | S-1 |
| B5 | Where are we improving, and where just getting busier? | EXEC | S-1 |
| B6 | How much does AI cost, who spends it, in what form? | EXEC | S-1 |
| B8 | How much goes into coordination instead of work? | LEAD | S-1 |
| C1 | Did throughput rise where AI was adopted, and what did it cost? | LEAD, EXEC | S-2 |
| C2 | Is speed limited by writing code or by reviewing it? | LEAD | S-2 |
| C3 | Speed went up — did quality hold? | LEAD | S-2 |
| C4 | Is this ticket spike caused by what we shipped? | LEAD, EXEC | S-2 |
| C5 | Activity rose — did the deals move? | LEAD, EXEC | S-2 |
| C6 | Development got cheaper — did the cost move somewhere else? | EXEC | S-2 |
| E1 | Is it feasible, what will it cost, how long, what risks? | EXEC, LEAD | S-2 |
| E2 | Where is the effect larger for less effort? | EXEC | S-2 |
| D1 | I can see the problem — what do I do? | LEAD | S-3 |
| D2 | Was it applied? Did it help? | LEAD, EXEC | S-3 |
| D3 | We had a reorg that month — how do I say so? | LEAD | S-3 |
| B7 | How does group A differ from group B in the same scope? | LEAD, EXEC | S-4 |
| G2 | How do we pull this into our own BI or report? | ADMIN, LEAD | S-5 |
| F1 | Our three-day cycle — is that bad? | EXEC | S-6 |
| A1 | Which questions can I already ask, and which not? | ADMIN | S-7 |
| A4 | Why is this empty, and what would make it not empty? | ADMIN, LEAD | S-7 |
| C8 | Is this an event in the business, or a break in the data? | ADMIN, LEAD, EXEC | S-7 |
| A2 | Which identity links did the system make, where is it unsure, where wrong? | ADMIN | S-8 |
| A3 | How do I tell the product who is supposed to do what here? | ADMIN | S-8, S-9 |
| C7 | Someone is listed in one role and does another — error or reality? | LEAD, ADMIN | S-8 |
| G1 | How do I give a lead their team and nothing more? | ADMIN | S-9 |
| G3 | Can we work in our own language and timezone? | all | S-9 |
| G4 | How do we replace what we have without losing history? | ADMIN, EXEC | S-10 |

All thirty map to a scenario. S-6 is the only scenario with no surface behind it yet.

---

## Appendix B · Scenarios against VISION §8

The vision lists nine product capabilities. The scenarios above are organised by what a person is
doing rather than by capability, so the mapping is not one-to-one: configuration splits in two, and
deployment comes from elsewhere in the vision.

| VISION §8 capability | Scenario |
|---|---|
| 8.1 Source connection and evidence coverage | S-7 |
| 8.2 Identity, role and organization model | S-8 |
| 8.3 Work, outcome and cost lineage | S-2 (what analysis rests on) |
| 8.4 Measurement and metric definitions | S-1 |
| 8.5 Analysis, diagnosis and forecasting | S-2 |
| 8.6 Recommendation and validation | S-3 |
| 8.7 Customer configuration | S-9, and the view-building half in S-4 |
| 8.8 Exposure and consumption | S-5 |
| 8.9 Benchmarks and shared intelligence | S-6 |
| §1, §14.1, §15.2 — deployment models, adoption, migration | S-10 |

---

## Appendix C · Open points

- **Administrative rights and data visibility.** §1.1 asserts that ADMIN gains no data visibility
  implicitly. Worth confirming against the access model rather than the interface.
- **Shared views and access.** S-4 asserts that a saved or shared view re-evaluates what each viewer
  may see. This is the easiest place for the access boundary to leak, and it needs confirming rather
  than assuming.
- **The small-group threshold** is stated per surface today. It protects an individual from being
  identified through a cohort, so it belongs to §5 with one agreed value.
- **IC has the narrowest surface and the tightest boundary.** Everything an IC sees is their own or a
  median — the combination that is easiest to get wrong quietly.
- **S-6 has no surface yet.** Listed so the capability stays planned for, not to imply it exists.

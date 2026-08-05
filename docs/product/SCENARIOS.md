# Constructor Insight — Main Scenarios by Persona

**Status:** draft for review · Companion to [VISION.md](VISION.md)

The vision says what Insight is and what it can do. This document says **who is standing in front of
it and how far each of them may reach**. It adds no capability and changes no commitment.

- **Personas** — the four target user groups of VISION §6.1.
- **Main scenarios** — the nine product capabilities of VISION §8, written as scenarios and split per
  persona.
- **Underneath** — the detailed user scenarios (Appendix A): 30 concrete questions, one person asking
  one thing at one moment. Each is traced to the scenario it belongs to.

**Why split by persona.** A capability is not one scenario. "Work with metrics" is a single
capability, but it means three different things:

- an **individual contributor** sees their own work against a department or cohort median, and no
  team metrics at all;
- a **team manager** sees their team, the people in it by name, and groups recalculated for that team
  rather than carried over from the organization;
- an **executive** sees the whole organization.

The capability is shared. The boundary is not, and the boundary is the part that gets lost in
implementation. That is why it is written down as a statement rather than left implied.

**How to read a scenario block**

- **The scenario** — what holds for everyone, whoever is looking.
- **Per persona** — what each of them can do, and what must never happen to them. Only the personas
  the scenario concerns are listed.
- **Not this** — what the scenario deliberately does not do, so it is not promised in a demo.
- **Detail** — the user-scenario IDs from Appendix A that belong to this capability.

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
person-level access has been granted: a manager's team view names their reports, and that is the
point of it. What VISION §3 rules out is a *default* view that ranks named individuals against one
another, or an unexplained productivity score. The line is the default surface and the granted scope
— not the name.

---

## 2. Main scenarios

One scenario per capability in VISION §8.

**Class.** **Core** is the improvement loop the product exists for — measure → diagnose → recommend
→ validate, plus the forward-looking half. **Service** is what makes that loop possible or safe.

**Priority** is not a delivery order. It is which scenario has to hold before the ones after it mean
anything. The rule: *a service scenario outranks a core one when it gates it, or when its failure
cannot be undone.* A wrong person makes every number wrong; an access failure cannot be repaired by a
later fix.

| Priority | ID | Scenario | Class | VISION | Detail |
|---|---|---|---|---|---|
| **P0** | **S-2** | Identity, role and organization model | Service | §8.2, §7.2 | A2, A3, C7 |
| **P0** | **S-4** | Measurement and metric definitions | Core | §8.4 | B1–B8 |
| **P0** | **S-7** | Configuration and access control | Service | §8.7, §9, §3 | G1, A3, B7 |
| **P1** | **S-1** | Source connection and evidence coverage | Service | §8.1, §7.5, §10.10 | A1, A4, C8, G3 |
| **P1** | **S-3** | Work, outcome and cost lineage | Core | §8.3, §7.3 | B6, C4, C5, C6 |
| **P1** | **S-5** | Analysis, diagnosis and forecasting | Core | §8.5, §5.1 | C1, C2, C3, E1, E2 |
| **P1** | **S-6** | Recommendation and validation | Core | §8.6, §1 | D1, D2, D3 |
| **P2** | **S-8** | Exposure and consumption | Service | §8.8, §15.2 | G2, G4 |
| **P3** | **S-9** | Benchmarks and shared intelligence | Core | §8.9, §12 | F1 |

Read the three P0 rows as the answer to "what has to be right first": one core scenario and two
service ones, because those two are the failures that cannot be undone.

### S-2 · Identity, role and organization model · P0 · Service

**The scenario.** Someone who appears separately in code, tickets, chat and the HR system is
recognised as one person, with a stated confidence. The customer can correct the result, the
correction survives the next sync, and role and team history is preserved so past periods are not
recalculated under a model that was not valid at the time.

**ADMIN** — Sort out who is who.
- sees what was matched automatically, where the system is unsure, and where it got it wrong
- merges and splits reversibly
- defines roles, several roles per person, and role history
- never loses a manual correction to the next sync

**LEAD, EXEC** — Trust the tree they roll up into.
- the organization structure they see is the one the customer recognises
- never a subtree quietly reshaped by re-resolution, with past periods recalculated underneath it

**IC** — Be one person, not four.
- never has their work attributed to a duplicate of themselves

**Not this.** Role definitions are the customer's, not ours. Where observed work does not match the
configured role model, Insight recommends changing the configuration — not the person (VISION §9).

**Detail.** A2 (identity queue), A3 (roles and expected activities), C7 (role vs observed work).

### S-4 · Measurement and metric definitions · P0 · Core

**The scenario.** Every number carries a governed definition, unit, granularity, confidence and
stated limitations, and is worked out the same way for every persona and every scope.

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
- configures which metrics matter, thresholds, cohorts and comparison groups
- never gains data visibility implicitly from administrative rights

**Not this.** No single "value of AI" number: seat-based and usage-based cost are not summed into
one figure, and unattributed cost stays its own line rather than being spread across the rest
(VISION §6.2.9, §11.4).

**Detail.** B1 (own context), B2 (team over a period), B3 (where work is stuck), B4 (knowledge
concentration), B5 (organization roll-up), B6 (AI cost), B7 (cohorts), B8 (cost of coordination).

### S-7 · Configuration and access control · P0 · Service

**The scenario.** The customer configures roles, activities, sources, metrics, thresholds, cohorts,
dashboards, localization and access rules. Access to raw, people-level, aggregate, cost and
recommendation data is role-based and policy-controlled, and the boundary holds on every surface.

**ADMIN** — Decide who gets what.
- grants the five kinds of data one by one (VISION §9)
- adapts roles, metrics and thresholds without engineering involvement
- never holds all five implicitly; a refusal is enforced underneath, not only hidden on screen

**LEAD** — Their own team, in full.
- every depth inside it
- never one level up, and never sideways — the limit is structural, not a filter on a screen

**EXEC** — The organization as a whole.
- aggregates, and people where person-level access is granted
- never the underlying records

**IC** — Themselves.
- named to their own management chain and to anyone else granted person-level access for their part
  of the organization — and to nobody outside it

**Not this.** Insight is read-only towards connected systems: it writes its own configuration and
annotations, nothing else (VISION §13.3).

**Detail.** G1 (who sees what), A3 (role configuration), B7 (cohort definition).

### S-1 · Source connection and evidence coverage · P1 · Service

**The scenario.** Whatever the wiring state, the product says so plainly: what is connected, what
that unlocks, and — where an answer is not possible — the cause and the smallest set of fixes with
the largest gain in confidence (readiness mode, VISION §7.5). Each source declares its fields,
window, freshness, blind spots and the level it supports (VISION §10.10).

**ADMIN** — Know what can be proven.
- every evidence category as connected, partly connected or absent, and which metrics each unlocks
- never left to guess why a screen is empty

**LEAD, EXEC** — Meet a gap and know what to do about it.
- the cause named directly: which source is missing, which identities are unresolved, which link is
  broken — plus the minimal fix
- freshness per source wherever a comparison is drawn
- never a comparison across two periods whose source windows do not overlap

**IC** — The same guarantee on their own context.

**Not this.** No zero in place of missing data — a zero looks like a measurement and raises a false
alarm. And no "rough estimate for now": the honest answer to a missing source is what is missing.

**Detail.** A1 (what can be proven), A4 (no answer, but what to connect), C8 (not believing a
number), G3 (timezone and locale caveats).

### S-3 · Work, outcome and cost lineage · P1 · Core

**The scenario.** Work is followed across the systems it passes through — intent to work item to
change to review to release to incident; ticket to product area; campaign to deal; cost record to
team or service. **Lineage comes before attribution** (VISION §7.3): what cannot be traced is shown
as an evidence gap, never converted into a confident claim.

**EXEC** — Follow cost and outcome to where they came from.
- to the function, team, product or service, as far as the trail actually goes
- never attribution stronger than the lineage supports

**LEAD** — See where the chain breaks in their own area.
- never a broken link quietly filled in

**ADMIN** — See which links are weak and what would repair them.

**IC** — Never attributed work that reached them only by inference.

**Not this.** Attribution has a ceiling, and the ceiling is stated rather than worked around: "this
change was written by AI" and "this change cost $N" are not claims Insight makes.

**Detail.** B6 (cost attribution ceiling), C4 (support load vs release), C5 (sales activity vs
pipeline), C6 (cost moved rather than fell).

### S-5 · Analysis, diagnosis and forecasting · P1 · Core

**The scenario.** The product stops showing shape and starts asserting a relationship — bottlenecks,
risks, anomalies, cost drivers, quality issues, role and activity mismatches — and says which kind of
claim it is making, with confidence and limitations. Forward-looking estimates (feasibility, cost,
time, risk) are grounded in the organization's own history and shown as extrapolations, not
guarantees.

**LEAD** — Understand why, not just what.
- a conclusion for their team or cohort, with the evidence behind it
- never a verdict about a named individual
- never a conclusion drawn from a group too small to protect one

**EXEC** — The same at organization level, plus a forecast for proposed work.
- never a causal claim where the evidence supports a correlation
- never a forecast presented as a guarantee

**ADMIN** — See which evidence gaps and known defects limit a conclusion.

**IC** — Not an audience for diagnosis, and never a named example inside one.

**Not this.** "AI sped up development by X%" is not a claim Insight makes. What it can say is that a
cohort with high usage differs from one with low usage in stated ways, correlationally — with the
word said on the surface, not in a footnote.

**Detail.** C1 (AI gain and price together), C2 (review as a bottleneck), C3 (did quality hold),
E1 (assess a feature before starting), E2 (where to invest next).

### S-6 · Recommendation and validation · P1 · Core

**The scenario.** A recommendation is a structured improvement object (VISION §1): observed problem,
affected area, evidence and confidence, recommended action, owner, expected metric movement, and the
follow-up window used to check it. Its origin is declared — evidence-derived from the customer's own
data, or heuristic. Afterwards the product reads the outcome from the measured system.

**LEAD** — Get an action, not an observation.
- one lever they can own, with what should move, which guardrail must not slip, and when it is checked
- never a recommendation that passes judgement on a named individual rather than on a process, team
  or cohort — a named *owner* is expected, a named *subject* is not
- never a recommendation whose origin is unstated

**EXEC** — Know whether it worked.
- whether the lever moved, whether the outcome moved, and the honest fourth answer: not enough data
- never a result assembled from metrics chosen after the fact — they are fixed when the
  recommendation is issued

**ADMIN** — Configure which recommendation families are enabled, who owns them, and how validation
windows are defined.

**IC** — Never the subject of a recommendation.

**Not this.** No surveys, and no self-reported productivity as an input. Validation is read from the
measured system. Insight recommends; it does not execute (VISION §13.3).

**Detail.** D1 (a recommendation, not an observation), D2 (did it work, a month later), D3 (context
the system cannot see).

### S-8 · Exposure and consumption · P2 · Service

**The scenario.** A number keeps its meaning when it leaves the product, and when the product
replaces a system that came before it. Exports carry the definition, coverage and confidence with the
number. Migration states openly what could not be reproduced (VISION §15.2).

**ADMIN** — Take Insight's output elsewhere.
- views, summaries, APIs and governed data access, under the same access rules
- never a number stripped of its definition and confidence, so that outside the product it becomes a
  fact without caveats

**EXEC** — Replace what exists without losing history.
- an inventory of current dashboards and metrics, then keep / rename / replace / retire
- history imported where retention allows, and parity checked over an agreed period
- never a parity claim that quietly omits what could not be reproduced

**LEAD, IC** — Keep their history across the change.
- never a past period silently recalculated under a new model

**Detail.** G2 (use conclusions in another system), G4 (migrate off a legacy system).

### S-9 · Benchmarks and shared intelligence · P3 · Core

**The scenario.** Comparison against the organization's own history by default; opt-in comparison
against peers and public data where enabled. Every benchmark declares its source, cohort definition,
coverage and confidence.

**EXEC** — Know whether a number is bad or normal.
- own history first, which requires sharing nothing
- peer comparison only where the customer has opted in

**ADMIN** — Turn participation on and off; it is revocable.

**Not this.** Raw customer data never leaves the customer boundary. Only anonymized aggregates at
cohort, team or organization level are shared — never individual data, never stack ranking
(VISION §3, §12.2).

**Detail.** F1 (are we slow, or is this normal).

---

## 3. Rules that hold in every scenario

From the vision, stated once here instead of repeated in each capability.

1. **Evidence gaps are shown, not hidden** (§3, §7.5) — never a zero for missing data, never an
   approximate estimate in place of an answer.
2. **Confidence and limitations travel with every conclusion** (§3) — a strong finding, a directional
   signal and an instrumentation problem stay distinguishable.
3. **Lineage before attribution** (§7.3) — untraceable work is a gap, not a quiet claim.
4. **No default ranking of named individuals, and no unexplained productivity scores** (§3) — people
   are named where person-level access has been granted; the ranking is what is ruled out.
5. **People-level access is role-based and policy-controlled** (§3, §9) — five kinds of data, granted
   separately.
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
2026-08-04. They are the level at which features are specified and tested; the capabilities in §2 are
what all of them must obey. Availability is deliberately not recorded here: it changes per release
and per customer, since the connected source set differs.

| ID | The question behind it | Who asks | Capability |
|---|---|---|---|
| A1 | Which questions can I already ask, and which not? | ADMIN | S-1 |
| A2 | Which identity links did the system make, where is it unsure, where wrong? | ADMIN | S-2 |
| A3 | How do I tell the product who is supposed to do what here? | ADMIN | S-2, S-7 |
| A4 | Why is this empty, and what would make it not empty? | ADMIN, LEAD | S-1 |
| B1 | What is visible about me, and what is in my way? | IC | S-4 |
| B2 | What changed for my team over the period? | LEAD | S-4 |
| B3 | Where is work stalling? | LEAD | S-4 |
| B4 | What falls apart if a specific person drops out? | LEAD, EXEC | S-4 |
| B5 | Where are we improving, and where just getting busier? | EXEC | S-4 |
| B6 | How much does AI cost, who spends it, in what form? | EXEC | S-4, S-3 |
| B7 | How does group A differ from group B in the same scope? | LEAD, EXEC | S-4, S-7 |
| B8 | How much goes into coordination instead of work? | LEAD | S-4 |
| C1 | Did throughput rise where AI was adopted, and what did it cost? | LEAD, EXEC | S-5 |
| C2 | Is speed limited by writing code or by reviewing it? | LEAD | S-5 |
| C3 | Speed went up — did quality hold? | LEAD | S-5 |
| C4 | Is this ticket spike caused by what we shipped? | LEAD, EXEC | S-3 |
| C5 | Activity rose — did the deals move? | LEAD, EXEC | S-3 |
| C6 | Development got cheaper — did the cost move somewhere else? | EXEC | S-3 |
| C7 | Someone is listed in one role and does another — error or reality? | LEAD, ADMIN | S-2 |
| C8 | Is this an event in the business, or a break in the data? | ADMIN, LEAD, EXEC | S-1 |
| D1 | I can see the problem — what do I do? | LEAD | S-6 |
| D2 | Was it applied? Did it help? | LEAD, EXEC | S-6 |
| D3 | We had a reorg that month — how do I say so? | LEAD | S-6 |
| E1 | Is it feasible, what will it cost, how long, what risks? | EXEC, LEAD | S-5 |
| E2 | Where is the effect larger for less effort? | EXEC | S-5 |
| F1 | Our three-day cycle — is that bad? | EXEC | S-9 |
| G1 | How do I give a lead their team and nothing more? | ADMIN | S-7 |
| G2 | How do we pull this into our own BI or report? | ADMIN, LEAD | S-8 |
| G3 | Can we work in our own language and timezone? | all | S-1 |
| G4 | How do we replace what we have without losing history? | ADMIN, EXEC | S-8 |

Every one of the thirty maps to a capability; no capability in §2 is left without a concrete question
underneath it, except S-9, which has none yet.

---

## Appendix B · Open points

- **Administrative rights and data visibility.** §1.1 asserts that ADMIN gains no data visibility
  implicitly. Worth confirming against the access model rather than the interface.
- **The small-group threshold** is stated per surface today. It protects an individual from being
  identified through a cohort, so it belongs to §3 with one agreed value.
- **IC has the narrowest surface and the tightest boundary.** Everything an IC sees is their own or a
  median — the combination that is easiest to get wrong quietly.
- **S-9 has no surface yet.** Listed so the capability stays planned for, not to imply it exists.

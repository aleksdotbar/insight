# Constructor Insight — Main Scenarios by Persona

**Status:** draft for review · Companion to [VISION.md](VISION.md)

The vision states what Insight is and what it can do. This document states **who is standing in
front of it and how far each of them may reach**. It adds no new capability and changes no
commitment: personas are the four target user groups of VISION §6.1, and the main scenarios are the
nine product capabilities of VISION §8, expressed as scenarios and attributed per persona.

**Why the per-persona split matters.** A capability is not one scenario. "Work with metrics" is a
single capability, but an individual contributor working with metrics sees their own work placed
against a department or cohort median and no team metrics at all; a team manager sees their subtree
and the people in it by name, with cohorts recomputed inside that scope rather than carried over from
the organization; an executive sees the whole organization. The capability is shared; the boundary is
not. The **must never** column is the part that is easy to lose in implementation, so it is written
here as a statement rather than left implied.

**How this document is used.** It is the level above feature specs: a feature changes, a scenario
does not. Specs under `docs/components/<area>/specs/` describe how a surface behaves; this document
describes what must be true of every surface serving that capability, for each persona.

---

## 1. Personas

The four target user groups of VISION §6.1, with short codes used throughout this document.

| Code | Group (VISION §6.1) | Arrives asking |
|---|---|---|
| **EXEC** | Executives and portfolio leaders | "Did it get better, or did we just get busier?" |
| **LEAD** | Functional leaders and team managers | "Where exactly is work blocked, and what can I do about it?" |
| **IC** | Functional teams and individual contributors | "How does my own work context look, and what is getting in my way?" |
| **ADMIN** | Data stewards and administrators | "Which of these numbers can be trusted, and who is allowed to see them?" |

### 1.1 Reach

The boundary each persona inherits in every scenario below. The **Never** row states the guarantees
in the form they have to hold — not as a description of the current UI.

| | EXEC | LEAD | IC | ADMIN |
|---|---|---|---|---|
| **Scope** | Organization and everything under it | Own subtree | Self | Configuration, not scope |
| **Aggregation depth** | Organization, function, team, and the people in them | Team, sub-team, cohort within own scope, and their own reports | Self, with the department or cohort present only as a median | n/a |
| **Named individuals** | Yes, anywhere in the organization, where person-level access is granted | Yes — a team view names the people reporting to them; that is what a team view is for | Self only | Only in identity resolution, which is about identity, not performance |
| **Comparison** | Between functions, teams and people inside the organization | Between cohorts recomputed **inside the active scope**, and between their own reports | Against a department or cohort median, never against a named colleague | n/a |
| **Cost data** | Where granted | Where granted | No | Where granted |
| **Diagnosis / recommendation** | Reads diagnoses | Reads diagnoses, receives recommendations | Neither | Neither |
| **Never** | Raw data · a default view that ranks people against one another · a number without its coverage and confidence | Anything outside the subtree · a default view that ranks their reports against one another · cohorts inherited from the organization instead of recomputed in scope | Any other person's activity · any team metric beyond the median they are placed against · own position in a ranked list | Administrative rights do **not** imply data visibility — each data class is granted separately (VISION §9) |

**On naming and ranking.** These are two different things and only one is restricted. People are
named wherever person-level access has been granted for them — a manager's team view names their
reports, and that is the point of it. What VISION §3 rules out is a *default* view that ranks named
individuals against one another, or an unexplained productivity score. The line is the default
surface, not the name.

---

## 2. Main scenarios

One scenario per product capability (VISION §8), classified and ordered.

**Class** — **Core** is the improvement loop the product exists for (measure → diagnose → recommend
→ validate, plus the forward-looking half); **Service** is what makes that loop possible or safe.
**Priority** is not a delivery order; it is which scenario has to hold before the ones after it mean
anything.

**The ordering rule:** a Service scenario outranks a Core one when it *gates* it, or when its
failure cannot be undone. Identity gates every number, so a wrong person makes every conclusion
wrong. An access failure cannot be repaired by a later fix.

| Priority | ID | Scenario | Class | VISION |
|---|---|---|---|---|
| **P0** | **S-2** | Identity, role, and organization model | Service | §8.2, §7.2 |
| **P0** | **S-4** | Measurement and metric definitions | Core | §8.4 |
| **P0** | **S-7** | Configuration and access control | Service | §8.7, §9, §3 |
| **P1** | **S-1** | Source connection and evidence coverage | Service | §8.1, §7.5, §10.10 |
| **P1** | **S-3** | Work, outcome, and cost lineage | Core | §8.3, §7.3 |
| **P1** | **S-5** | Analysis, diagnosis, and forecasting | Core | §8.5, §5.1, §7.1 |
| **P1** | **S-6** | Recommendation and validation | Core | §8.6, §1, §9 |
| **P2** | **S-8** | Exposure and consumption | Service | §8.8, §15.2 |
| **P3** | **S-9** | Benchmarks and shared intelligence | Core | §8.9, §12 |

### S-2 · Identity, role, and organization model · P0 · Service

**The scenario.** A person appearing in several source systems resolves to one identity with a
stated confidence; the customer can correct the result and the correction survives the next sync;
roles and team membership keep their history, so past periods are not recomputed under a model that
was not valid at the time.

| Persona | Can | Must never |
|---|---|---|
| ADMIN | Review what was matched automatically, where the system is unsure, and where it was wrong; merge and split reversibly; configure roles, multiple roles per person, and role history | Lose a manual correction to the next sync; face a merge that cannot be undone |
| LEAD, EXEC | Trust that the organization tree they roll up into is the one the customer recognises | Be shown a subtree silently reshaped by re-resolution, with past periods recomputed |
| IC | Be one person rather than several | Have their work attributed to a duplicate of themselves |

### S-4 · Measurement and metric definitions · P0 · Core

**The scenario.** Every number carries a governed definition, unit, granularity, confidence and
stated limitations, and is computed the same way for every persona and every scope.

| Persona | Can | Must never |
|---|---|---|
| EXEC | Roll the organization up by function, team and person, with change over time | See a coverage figure produced by treating missing data as zero, or a default view that ranks people against one another |
| LEAD | See their subtree for a period, the people in it by name, where work is blocked, and cohorts recomputed inside the active scope | Compare against a team outside the subtree; inherit cohorts from the organization; be shown a group small enough to identify an individual; be handed a "who is best" ordering as the default view |
| IC | See their own activity, flow and AI usage, placed against a department or cohort median | See another person's activity, a team metric beyond that median, or their own position in a ranked list |
| ADMIN | Configure which metrics matter, thresholds, cohorts and comparison groups | Gain data visibility implicitly from administrative rights |

### S-7 · Configuration and access control · P0 · Service

**The scenario.** The customer configures roles, activities, sources, metrics, thresholds, cohorts,
dashboards, localization and access rules; access to raw, people-level, aggregate, cost and
recommendation data is role-based and policy-controlled, and the boundary holds on every surface.

| Persona | Can | Must never |
|---|---|---|
| ADMIN | Grant the five data classes independently (VISION §9); adapt roles, metrics and thresholds without engineering involvement | Hold all classes implicitly; have a grant enforced in the interface but not underneath it |
| LEAD | Reach every depth of their subtree | Reach one node above or sideways — the boundary is structural, not a filter |
| EXEC | Read organization-wide aggregates, and people where person-level access is granted | Reach raw data, or a default view that ranks people against one another |
| IC | Read themselves | Be named to anyone outside the part of the organization that has been granted person-level access to them |

### S-1 · Source connection and evidence coverage · P1 · Service

**The scenario.** Whatever the wiring state, the product states it honestly: what is connected, what
that unlocks, and — where an answer is not possible — the cause and the smallest set of fixes with
the largest gain in confidence (readiness mode, VISION §7.5). Each source declares its fields,
window, freshness, blind spots and the level it supports (VISION §10.10).

| Persona | Can | Must never |
|---|---|---|
| ADMIN | See every evidence category as connected, partial or absent, and which metrics each one unlocks | Be left to guess why a surface is empty |
| LEAD, EXEC | Meet a broken evidence chain and be told the cause and the minimal fix | Be shown a zero, or an approximate estimate, in place of missing data |
| IC | The same guarantee on their own context | — |

### S-3 · Work, outcome, and cost lineage · P1 · Core

**The scenario.** Work is followed across the systems it passes through, and **lineage comes before
attribution** (VISION §7.3): what cannot be traced is shown as an evidence gap, never converted into
a confident claim.

| Persona | Can | Must never |
|---|---|---|
| EXEC | Follow cost and outcome to the function, team or product that produced it | Be given attribution stronger than the lineage supports |
| LEAD | See where a chain breaks in their own area | Have a broken link silently filled in |
| ADMIN | See which links are weak and what would repair them | — |
| IC | — | Be attributed work that was traced to them only by inference |

### S-5 · Analysis, diagnosis, and forecasting · P1 · Core

**The scenario.** The product moves from showing shape to asserting a relationship — bottlenecks,
risks, anomalies, cost drivers, role and activity mismatches — and states which kind of claim it is
making, with confidence and limitations. Forward-looking estimates (feasibility, cost, time, risk)
are grounded in the organization's own history and presented as extrapolations, not guarantees.

| Persona | Can | Must never |
|---|---|---|
| LEAD | Read a diagnosis for their team or cohort, with the evidence behind it | Receive a verdict about a named individual, or a conclusion drawn from a group too small to protect one |
| EXEC | Read the same at organization level, and a forecast for proposed work | Be handed a causal claim where the evidence supports a correlation, or a forecast presented as a guarantee |
| ADMIN | See which evidence gaps and known defects limit a diagnosis | — |
| IC | — | Appear as a named example inside a diagnosis |

### S-6 · Recommendation and validation · P1 · Core

**The scenario.** A recommendation is a structured improvement object (VISION §1): observed problem,
affected area, evidence and confidence, recommended action, owner, expected metric movement and the
follow-up window used to validate it — with its origin declared as evidence-derived or heuristic.
Validation is read from the measured system afterwards, not from self-reporting.

| Persona | Can | Must never |
|---|---|---|
| LEAD | Receive an action they can own, with what should move, and when it will be checked | Receive a recommendation that passes judgement on a named individual rather than on a process, team or cohort, or one whose origin is unstated — a named *owner* is expected, a named *subject* is not |
| EXEC | Read whether recommendations changed the measured system | Be shown a validation result assembled from metrics chosen after the fact |
| ADMIN | Configure which recommendation families are enabled, who owns them and how validation windows are defined | — |
| IC | — | Be the object of a recommendation |

**Also true of the product as a whole:** Insight recommends and does not execute (VISION §13.3). It
writes its own configuration and annotations, nothing else.

### S-8 · Exposure and consumption · P2 · Service

**The scenario.** A number keeps its meaning when it leaves the product — exported through views,
summaries, APIs and governed data access together with its definition, coverage and confidence — and
keeps it when history is migrated from a system Insight replaces, with anything that cannot be
reproduced stated openly (VISION §15.2).

| Persona | Can | Must never |
|---|---|---|
| ADMIN | Take metrics into another system under the same access rules | Export a number stripped of its definition and confidence, where it becomes a fact without caveats |
| EXEC | See parity against the replaced system over an agreed period | Be shown a parity claim that quietly omits what could not be reproduced |
| LEAD, IC | Keep their history across a migration | Have a past period silently recomputed under a new model |

### S-9 · Benchmarks and shared intelligence · P3 · Core

**The scenario.** Comparison against the organization's own history by default; opt-in comparison
against peers and public data where enabled. Every benchmark declares its source, cohort definition,
coverage and confidence.

| Persona | Can | Must never |
|---|---|---|
| EXEC | Compare the organization against peers where the customer has opted in | Have raw customer data leave the customer boundary; receive individual-level or stack-ranked comparison (VISION §3, §12.2) |
| LEAD | Compare their area against the organization's own history without sharing anything | — |
| ADMIN | Turn participation on and off; it is revocable | — |

---

## 3. Rules that hold in every scenario

These come from the vision itself and are not restated in each scenario above. They apply to every
surface serving any capability.

1. **Evidence gaps are shown, not hidden** (§3, §7.5) — missing data is never rendered as zero, and
   never replaced by an approximate estimate.
2. **Confidence and limitations travel with every conclusion** (§3) — a strong finding, a directional
   signal and an instrumentation problem are distinguishable.
3. **Lineage before attribution** (§7.3) — untraceable work is a gap, not a quiet claim.
4. **No default stack ranking or unexplained productivity scores** (§3) — people are named where
   person-level access has been granted; what is ruled out is a default view that ranks them against
   one another.
5. **People-level access is role-based and policy-controlled** (§3, §9) — five independently granted
   data classes.
6. **Cost movement is preserved, not folded away** (§11.4) — a local saving that shifts cost, risk or
   effort downstream is shown as a shift; seat-based and usage-based cost are not summed into one
   figure (§6.2.9).
7. **Insight observes and advises; people act** (§13.3).
8. **Clean room** (§12.2) — raw data stays inside the customer boundary; only anonymized aggregates
   at cohort, team or organization level are shared, opt-in and revocable.
9. **Role and activity are separate axes** (§7.2) — expected role model and observed activity are
   compared, never conflated, and history is preserved under the model valid at the time.

---

## 4. Open points

- **IC has the narrowest surface and the strictest boundary.** Everything an IC sees is either their
  own or a median. That combination makes it the sharpest case to get wrong quietly.
- **Administrative rights vs data visibility.** §1.1 asserts that ADMIN gains no data visibility
  implicitly. This needs confirming against the access model rather than the interface.
- **Small-group thresholds are stated per surface today.** They protect an individual from being
  identified through a cohort, so they belong to the shared rules in §3 rather than to individual
  features — the exact values are worth agreeing in one place.
- **S-9 has no surface yet.** It is listed so the capability stays planned for, not to imply it exists.

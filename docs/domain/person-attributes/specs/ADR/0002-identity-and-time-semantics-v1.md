---
status: proposed
date: 2026-08-05
decision-makers: Insight engineering
---

# ADR-0002: Separate Corrective Account Identity from Temporal Attribute Facts


<!-- toc -->

- [Context and Problem Statement](#context-and-problem-statement)
- [Decision Drivers](#decision-drivers)
- [Considered Options](#considered-options)
- [Decision Outcome](#decision-outcome)
  - [Consequences](#consequences)
  - [Confirmation](#confirmation)
- [Pros and Cons of the Options](#pros-and-cons-of-the-options)
  - [Email Resolution for All Facts](#email-resolution-for-all-facts)
  - [Effective-Dated Person Assignment](#effective-dated-person-assignment)
  - [Corrective Account Assignment with Temporal Attributes](#corrective-account-assignment-with-temporal-attributes)
- [More Information](#more-information)
- [Traceability](#traceability)

<!-- /toc -->

**ID**: `cpt-person-attributes-adr-identity-and-time-semantics`
## Context and Problem Statement

Connector attribute claims identify native source accounts, while metric results and groups identify canonical people. The current identity model records the latest source-account-to-person decision, and a developing identity workflow is expected to preserve that corrective behavior. Attribute values, however, must retain the job title, department, office, or manager relationship that was valid during each metric period.

The architecture must decide how current account assignment, historical attribute facts, email-only metric observations, and period-crossing people-like comparisons interact without making #2028 depend on the unfinished identity workflow.

## Decision Drivers

- Stable source-account identifiers are stronger than email for connector attribute claims.
- Human identity corrections should repair all retained facts for an account.
- A person's historical attributes must not be rewritten to their current values.
- The current `identity.identity_persons` snapshot already exposes latest account bindings.
- Some metric observations currently contain only email and still require resolution.
- #2028 must be able to adopt the future identity workflow without changing analytical claims or APIs.
- Period-crossing comparisons must not hide changes in the subject's peer definition.

## Considered Options

- Resolve every attribute and metric observation through the current email snapshot.
- Introduce effective-dated person assignment and evaluate both assignment and attributes as-of every observation.
- Use current corrective source-account assignment for attribute claims, keep attribute facts effective-dated, and retain email resolution only for observations without stable account identity.

## Decision Outcome

Chosen option: **Current corrective source-account assignment plus temporal attribute facts**, because it matches the current and planned identity semantics while preserving the history #2028 actually needs. Attribute claims resolve through `(tenant, source type, source instance, source account ID)`. Reassigning that account to a different canonical person reattributes all retained claims on the next build. The claim's value intervals are not changed.

Email resolution remains an adapter for metric observations that do not carry stable source-account identity. Both adapters resolve to the same canonical person ID and are pinned by the analytical build's identity revision.

When a people-like subject changes a selected attribute during the requested period, analytics returns maximal stable temporal segments. Named-group conditions remain fixed and evaluate changing membership over the period.

### Consequences

- Good, because attribute ingestion does not regress to mutable or shared email identity.
- Good, because a human correction repairs historical attribution without rewriting source facts.
- Good, because job, office, and hierarchy history remain period-correct.
- Good, because the future identity workflow can replace the assignment producer behind a stable projection contract.
- Good, because existing email-only metric models can migrate independently.
- Bad, because assignment history is not effective-dated; the latest correction applies retroactively.
- Bad, because a long people-like request can return several comparison segments.
- Bad, because group membership and metric coverage may differ when metric aliases remain unresolved.
- Risk: a native account ID reused between humans would reattribute earlier claims incorrectly. Supported connectors must treat account IDs as non-reusable; shared and service accounts are excluded. If that invariant fails, a new ADR must introduce effective-dated assignment.
- Risk: assignment and metric resolution could use different revisions. The active-build manifest pins the identity revision and refuses inconsistent publication.

### Confirmation

The decision is confirmed by design and implementation review showing:

- Attribute claims join through stable source-account keys and never fall back to email when that key exists.
- The initial assignment projection derives the latest `value_type = 'id'` record from `identity.identity_persons`.
- Rebuilding after reassignment changes the canonical person but preserves claim value intervals.
- Email resolution remains isolated to observations that lack stable account identity.
- People-like results split when selected subject values change inside the period.
- Named groups retain fixed conditions while qualifying observations by temporal membership.

## Pros and Cons of the Options

### Email Resolution for All Facts

Every attribute claim and metric observation joins the latest normalized email-to-person snapshot.

- Good, because one existing resolver serves every input.
- Good, because implementation is initially small.
- Bad, because email can change, collide, or be absent while a stable account ID exists.
- Bad, because source provenance is discarded during resolution.
- Bad, because using email for stable account claims would make future identity migration harder.

### Effective-Dated Person Assignment

Account-to-person assignment and attribute values both carry historical validity and are evaluated as-of each fact.

- Good, because it can represent account reuse and assignment history exactly.
- Good, because historical attribution never changes after a correction.
- Bad, because it conflicts with the current human-decision model where corrections intentionally repair all history.
- Bad, because no current assignment event history exists to backfill trustworthy intervals.
- Bad, because it expands #2028 into a new identity semantics project and blocks delivery on the unfinished workflow.

### Corrective Account Assignment with Temporal Attributes

The latest source-account decision applies to all retained claims; each claim keeps its own effective-dated business value. Email remains only for observations without a stable account key.

- Good, because it matches current identity evidence and planned correction behavior.
- Good, because it preserves the business history needed for peer grouping.
- Good, because it creates a stable adapter boundary for the future identity workflow.
- Good, because account and email resolution converge on canonical person IDs before metric aggregation.
- Bad, because native account reuse cannot be represented safely.
- Bad, because two resolution adapters coexist during migration.

## More Information

**Scope**: Identity semantics for attribute claims, compatibility with email-only metric observations, and the temporal interpretation of people-like and named-group requests. The identity workflow's admin APIs and merge UX are outside this decision.

**Performance**: A current assignment projection avoids an interval join on identity history. Temporal joins remain limited to attribute values and metric periods. Segments are represented inside one ClickHouse statement; they can multiply query partitions and result rows, so request limits and representative benchmarks remain required.

**Security and compliance**: Tenant travels in every account key and analytical relation. Predicate enforcement remains controlled by `metric_catalog.enforce_tenant_scope` until platform tenant alignment is enabled. No authentication or authorization mechanism changes. Reassignment audit remains owned by Identity.

**Reliability and operations**: Assignment revision, unresolved counts, rebuild completion, and publication compatibility must be observable. Recovery rebuilds gold values from retained claims and the chosen assignment revision.

**Integration and compatibility**: Existing email metric models remain valid. The future identity workflow must publish the same typed assignment projection; its internal journal and APIs may differ. No connector API breaks when stable source-account identity is already present.

**Maintainability and testing impact**: Resolution mechanisms remain separate adapters with one canonical output type. Verification requires reassignment, unresolved, email-only metric, account-reuse rejection, temporal boundary, and period-segmentation coverage; test implementation belongs with the code.

**User and business impact**: Corrected identities repair historical metrics, while historical business attributes remain accurate for their periods. Users may see several explicit peer segments instead of one misleading blended result. No direct user migration is required.

**Review trigger**: Revisit if a supported source reuses native account IDs, Identity adopts effective-dated assignment rather than corrective decisions, or all metric producers gain stable source-account identity and the email adapter can be removed.

## Traceability

- **Requirements**: [GitHub issue #2028](https://github.com/constructorfabric/insight/issues/2028) — requires period-correct person grouping and comparison.
- **DESIGN**: [Person Attributes and Cohorting](../DESIGN.md) — defines the assignment projection, temporal values, and membership behavior.
- **Related ADR**: [ADR-0001](./0001-attribute-data-ownership-v1.md) — defines where the assignment revision and temporal facts are published.

This decision directly constrains:

- `cpt-person-attributes-principle-source-account-resolution`
- `cpt-person-attributes-principle-temporal-segmentation`
- `cpt-person-attributes-constraint-corrective-assignment`
- `cpt-person-attributes-component-assignment-publisher`
- `cpt-person-attributes-component-gold-builder`
- `cpt-person-attributes-seq-people-like`

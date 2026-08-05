---
status: proposed
date: 2026-08-05
decision-makers: Insight engineering
---

# ADR-0001: Keep Person Attribute Facts in ClickHouse and Governance in Identity


<!-- toc -->

- [Context and Problem Statement](#context-and-problem-statement)
- [Decision Drivers](#decision-drivers)
- [Considered Options](#considered-options)
- [Decision Outcome](#decision-outcome)
  - [Consequences](#consequences)
  - [Confirmation](#confirmation)
- [Pros and Cons of the Options](#pros-and-cons-of-the-options)
  - [Identity MariaDB as the Attribute System of Record](#identity-mariadb-as-the-attribute-system-of-record)
  - [ClickHouse as the Complete Attribute System of Record](#clickhouse-as-the-complete-attribute-system-of-record)
  - [Split Ownership with Immutable Publication](#split-ownership-with-immutable-publication)
- [More Information](#more-information)
- [Traceability](#traceability)

<!-- /toc -->

**ID**: `cpt-person-attributes-adr-attribute-data-ownership`
## Context and Problem Statement

Person attributes originate in connector data, require temporal analytical joins, and must be governed by tenant admins. The architecture must decide where source claims, editable policy, query-oriented values, and measured catalog statistics are authoritative without creating request-time cross-database joins or duplicate editable state.

This decision affects connector ingestion, Identity, dbt, ClickHouse, and Analytics. It is needed before #2028 implementation because storage ownership determines the publication contract and failure model.

## Decision Drivers

- Analytical grouping must join attributes to ClickHouse metric facts efficiently.
- Admin policy and audit require transactional writes and clear ownership.
- Source claim history must remain rebuildable after person-assignment corrections.
- Analytics reads must not depend on a request-time Identity service or MariaDB call.
- Curated permission must remain distinct from measured availability.
- Partial publication must not expose incompatible policy, values, and statistics.
- The solution should add no new datastore or deployable service.

## Considered Options

- Store claims, policy, and temporal values in Identity MariaDB, then copy query-ready values to ClickHouse.
- Store claims, policy, and temporal values entirely in ClickHouse.
- Split ownership: claims and analytical projections in ClickHouse; editable policy and audit in Identity MariaDB; publish immutable policy snapshots to ClickHouse.

## Decision Outcome

Chosen option: **Split ownership**, because each datastore then owns the workload it is designed to serve without duplicating authority. ClickHouse retains connector claims and serves temporal value and statistics queries. Identity MariaDB owns transactional definitions, policy revisions, and audit. Identity publishes complete immutable policy revisions into ClickHouse, and an active-build manifest admits only compatible claim, policy, assignment, values, and statistics revisions.

Named-group definitions are not part of this decision's Identity boundary. They remain tenant configuration in Analytics MariaDB because their consumers and lifecycle are owned by analytics.

### Consequences

- Good, because peer queries read attributes and metrics from one analytical store.
- Good, because connector history does not take an unnecessary ClickHouse-to-MariaDB-to-ClickHouse round trip.
- Good, because admin policy has one transactional, audited source of truth.
- Good, because values and per-attribute statistics can be rebuilt from retained claims after an identity correction.
- Good, because immutable snapshots remove request-time Identity availability from the analytics path.
- Bad, because the system is eventually consistent across MariaDB and ClickHouse.
- Bad, because snapshot publishers and a compatibility manifest are required.
- Bad, because rollback operates by activating a previous compatible build rather than one distributed transaction.
- Risk: stale policy could be paired with new values. The active-build manifest prevents activation unless source revisions and checksums are compatible.
- Risk: ClickHouse could be mistaken for an editable policy source. Analytics treats the projection as immutable and all writes remain in Identity.

### Confirmation

The decision is confirmed by design and implementation review showing:

- Connector attribute claims are retained in ClickHouse silver.
- Identity MariaDB contains definitions, immutable policy revisions, and audit, but no analytical claim history.
- Analytics query compilation reads active policy, values, and statistics from ClickHouse without an Identity call.
- A values revision and statistics revision cannot become active against different policy, assignment, or claim revisions.
- `person_attribute_stats` contains per-attribute metadata only, not requested metric or cohort results.

## Pros and Cons of the Options

### Identity MariaDB as the Attribute System of Record

Claims, policy, and effective values are persisted in Identity, with query-oriented values replicated to ClickHouse.

- Good, because identity and attribute mutation share one transactional database.
- Good, because policy and values can be inspected through one operational store.
- Bad, because connector facts already in ClickHouse must be copied into MariaDB and then back into ClickHouse.
- Bad, because high-volume temporal facts and analytical scans become an Identity storage concern.
- Bad, because two fact copies need reconciliation and retention coordination.

### ClickHouse as the Complete Attribute System of Record

Claims, editable policy, audit, values, and statistics are all stored in ClickHouse.

- Good, because all analytical and governance reads use one database.
- Good, because no policy publication step exists.
- Bad, because ClickHouse is a weak fit for transactional admin edits, optimistic concurrency, and actor-attributed audit.
- Bad, because analytical jobs and governance writes share ownership and failure semantics.
- Bad, because policy state becomes easier to mutate outside the Identity authorization boundary.

### Split Ownership with Immutable Publication

ClickHouse owns claims and analytical projections. Identity MariaDB owns editable definitions, policy revisions, and audit. Complete policy snapshots are published to ClickHouse.

- Good, because storage aligns with transactional and analytical workloads.
- Good, because read queries remain local to ClickHouse.
- Good, because each data class has one editable authority.
- Good, because revisions make freshness and compatibility explicit.
- Bad, because eventual consistency and publication monitoring are required.
- Bad, because operators must understand both source ownership and active analytical revision.

## More Information

**Scope**: Attribute claims, definitions, policy, person-linked values, measured attribute statistics, and their publication boundary. Named-group ownership and account-resolution semantics are covered elsewhere.

**Performance**: The chosen option avoids request-time cross-database joins and supports ClickHouse orderings for subject-history and value-membership reads. Publication adds scheduled work but no user-request latency.

**Security and compliance**: Existing Identity authorization protects governance writes. Existing database encryption, retention, backup, and access controls apply. No authentication mechanism changes. Sensitivity semantics are outside this storage choice; configured classification and comparison eligibility are both part of the published policy.

**Reliability and operations**: Publication lag and revision mismatch become observable failure modes. Existing database recovery applies; a previous compatible build is the rollback unit. No new infrastructure or paid service is introduced.

**Integration and compatibility**: Existing connectors and metric APIs gain additive contracts. No external protocol is broken. The legacy cohort path can coexist while new gold values are built.

**Maintainability and testing impact**: The split creates explicit owner contracts but avoids a second fact pipeline through Identity. Verification requires contract, rebuild, interrupted-publication, and revision-compatibility coverage; test implementation belongs with the code.

**User and business impact**: Users receive fresher and more scalable grouping without seeing storage boundaries. The approach adds publication work but avoids a larger Identity persistence expansion. No user migration or training is required for this decision itself.

**Review trigger**: Revisit if policy requires transactional coupling to source facts, ClickHouse no longer hosts metric facts, or publication lag cannot meet the accepted catalog freshness objective.

## Traceability

- **Requirements**: [GitHub issue #2028](https://github.com/constructorfabric/insight/issues/2028) — requires attributes in analytics plus a curated and measured attribute list.
- **DESIGN**: [Person Attributes and Cohorting](../DESIGN.md) — applies this ownership and publication boundary.
- **Related ADR**: [ADR-0002](./0002-identity-and-time-semantics-v1.md) — defines the assignment revision consumed by the active build.

This decision directly constrains:

- `cpt-person-attributes-principle-analytical-facts-in-clickhouse`
- `cpt-person-attributes-principle-curated-and-measured`
- `cpt-person-attributes-component-claim-store`
- `cpt-person-attributes-component-policy-publisher`
- `cpt-person-attributes-component-publication-manifest`
- `cpt-person-attributes-db-storage`

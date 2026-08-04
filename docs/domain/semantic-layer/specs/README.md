# Semantic Layer

Adopted target architecture for Phase B of the presentation-layer split
(constructorfabric/insight#1803): every analytical value is defined, validated,
computed, and served through one compiler over datasets, with definitions as
data.

- [DESIGN.md](./DESIGN.md) — the target architecture (system contract).
- [IMPLEMENTATION.md](./IMPLEMENTATION.md) — the migration plan (keep / rewrite
  / delete, phased with a parity-checked cutover).
- [FINDINGS.md](./FINDINGS.md) — adoption review: alignment, how the shipped
  registry (#1974) fits, the sub-issue re-scope, the org-scope authorization
  the design must name, and the load-bearing decision.

Read DESIGN.md before changing metric definitions, the compiler, or the
definition store. The metrics-domain design
([docs/domain/metrics/specs/DESIGN.md](../../metrics/specs/DESIGN.md)) is the
current implementation contract this layer supersedes on cutover.

# Cypilot: InsightFront

## Navigation Rules

ALWAYS open and follow `{cypilot_path}/config/artifacts.toml` WHEN working with artifacts or codebase

ALWAYS open and follow `{cypilot_path}/.core/schemas/artifacts.schema.json` WHEN working with artifacts.toml

ALWAYS open and follow `{cypilot_path}/.core/architecture/specs/artifacts-registry.md` WHEN working with artifacts.toml

# Cypilot Kit: SDLC (`sdlc`)

Agent quick reference.

## What it is

Artifact-first SDLC pipeline (PRD → ADR + DESIGN → DECOMPOSITION → FEATURE → CODE) with templates, checklists, examples, and per-artifact `rules.md` for deterministic validation + traceability.

## Artifact kinds

| Kind | Semantic intent (when to use) | References |
| --- | --- | --- |
| PRD | Product intent: actors + problems + FR/NFR + use cases + success criteria. | `config/kits/sdlc/artifacts/PRD/rules.md`, `config/kits/sdlc/artifacts/PRD/template.md`, `config/kits/sdlc/artifacts/PRD/checklist.md`, `config/kits/sdlc/artifacts/PRD/examples/example.md` |
| ADR | Decision log: why an architecture choice was made (context/options/decision/consequences). | `config/kits/sdlc/artifacts/ADR/rules.md`, `config/kits/sdlc/artifacts/ADR/template.md`, `config/kits/sdlc/artifacts/ADR/checklist.md`, `config/kits/sdlc/artifacts/ADR/examples/example.md` |
| DESIGN | System design: architecture, components, boundaries, interfaces, drivers, principles/constraints. | `config/kits/sdlc/artifacts/DESIGN/rules.md`, `config/kits/sdlc/artifacts/DESIGN/template.md`, `config/kits/sdlc/artifacts/DESIGN/checklist.md`, `config/kits/sdlc/artifacts/DESIGN/examples/example.md` |
| DECOMPOSITION | Executable plan: FEATURE list, ordering, dependencies, and coverage links back to PRD/DESIGN. | `config/kits/sdlc/artifacts/DECOMPOSITION/rules.md`, `config/kits/sdlc/artifacts/DECOMPOSITION/template.md`, `config/kits/sdlc/artifacts/DECOMPOSITION/checklist.md`, `config/kits/sdlc/artifacts/DECOMPOSITION/examples/example.md` |
| FEATURE | Precise behavior + DoD: CDSL flows/algos/states + test scenarios for implementability. | `config/kits/sdlc/artifacts/FEATURE/rules.md`, `config/kits/sdlc/artifacts/FEATURE/template.md`, `config/kits/sdlc/artifacts/FEATURE/checklist.md`, `config/kits/sdlc/artifacts/FEATURE/examples/example.md` |
| CODE | Implementation of FEATURE with optional `@cpt-*` markers and checkbox cascade/coverage validation. | `config/kits/sdlc/codebase/rules.md`, `config/kits/sdlc/codebase/checklist.md` |

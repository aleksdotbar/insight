# Rust style — backend workspace

Tooling owns formatting (rustfmt) and correctness lints (clippy pedantic,
deny). These rules cover only what tooling cannot enforce.

## Structure

- One noun per file. A module growing past ~400 lines splits into a directory
  (`dto.rs`, `validation.rs`, `compiler.rs`, ...) — never a second noun in the
  same file.
- Handlers are orchestration skeletons (≤ ~30 lines): extract → validate →
  domain call → map → respond. No business logic in the API layer; no
  serialization formats (CSV, XLSX, ...) outside domain.
- I/O shell, pure core: anything computable without a connection is a free
  function over values. Tests target the core; no mocks of our own types.
- Repetition becomes a named helper; the name is the documentation. Error
  construction lives in one helper per failure kind: log detailed internally,
  return a generic wire error.

## Types

- Parse, don't validate: newtypes at boundaries (`RelationName::parse(&str) ->
  Option<RelationName>`), never raw `String` carried through layers.
- Exhaustive `match` on our own enums — no `_` arm.
- Smallest visibility that compiles; `pub(crate)` before `pub`. No speculative
  API surface.
- `#[derive(Debug)]` always; `Clone` only when a consumer clones.
- Constants: module top, grouped, unit-suffixed (`_BYTES`, `_SECS`, `_DAYS`).

## Comments

- None, unless code cannot express the why: intentional redundancy
  (defense-in-depth re-checks), cross-function invariants ("the cursor is not
  an authorization token"), `#[allow]` justifications. One line each.
- No module headers, no `///` on self-describing items, no issue numbers in
  source, no phase/scope notes — that context lives in issues and PRs.
- Non-obvious semantics get a test whose name states the rule
  (`absent_null_and_value_are_three_distinct_states`), not a comment.

## Readability

- Paragraph functions: blank line between logical steps (gather → transform →
  emit). A multi-line statement (builder chain, `match`, closure block) gets a
  blank line after it before unrelated code. No blank line inside one tight
  thought.
- Early return / `let .. else` / `?` over nested `if`: two levels of
  indentation inside a function body is the ceiling to aim for.
- One `let` per binding; no expression so dense it needs re-reading — name the
  intermediate instead.
- Import order: std, external crates, workspace crates, `crate::`/`super::` —
  one blank line between groups (rustfmt won't fix this; keep it by hand).
- Match arms: single-line arms stay single-line; once one arm needs a block,
  give every arm breathing room.
- Tests read as spec: table-driven loops with per-case assert messages
  (`"should reject: {input:?}"`); alias `type R = Result<(), Box<dyn Error>>`
  to cut ceremony.

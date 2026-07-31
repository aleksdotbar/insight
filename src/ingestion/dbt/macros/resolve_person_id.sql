{#-
  THE resolve point of the metrics identity rework: collapses the raw
  `identity.identity_persons` observation log (see create_identity_persons /
  the service's persons-sync) into the CURRENT `email -> person_id` map, for
  gold observation models to LEFT JOIN:

      LEFT JOIN ({{ resolve_person_id() }}) AS identity_map
          ON identity_map.email = <entity_id expression>

  Emits (email, person_id), one row per email. Resolution rule v1 —
  latest-observation-wins: the newest `value_type='email'` row per normalized
  email claims it (`created_at DESC, id DESC`; the id tiebreak makes
  same-instant observations deterministic, matching the service's own
  reader ordering). No tenant filter — single-tenant reality (#1550); the
  tenant column is in the log when that changes.

  This macro is deliberately the ONLY place resolution semantics live.
  Future smarts — per-source maps ("this email as seen by git sources"),
  tenant scoping, as_of resolution off `created_at` — change this body and
  every consuming model picks it up on the next build. Consumers must not
  re-derive person_id any other way.

  NORMALIZATION CONTRACT: `lower(trimBoth(...))` on BOTH sides, enforced in
  the join itself — resolved_person_id_join() applies the same expression to
  the model's entity_id rather than trusting each model to have normalized
  identically (they don't: git trims, ai/wiki/task only lowercase, collab
  inherits person_key from the class contract). Idempotent for
  already-normalized keys; for the rest it is the difference between
  resolving and silently missing.

  Dedup note (check-dbt-conventions): identity_persons is a plain MergeTree
  replaced wholesale by an atomic snapshot swap — no ReplacingMergeTree, no
  duplicate row versions to collapse, so no FINAL here; LIMIT 1 BY picks the
  resolution winner, not a dedup survivor.
-#}

{% macro resolve_person_id() %}
    SELECT
        lower(trimBoth(value_effective)) AS email,
        person_id
    FROM identity.identity_persons
    WHERE value_type = 'email'
      AND value_effective IS NOT NULL
      AND trimBoth(value_effective) != ''
    ORDER BY
        email,
        created_at DESC,
        id DESC
    LIMIT 1 BY email
{% endmacro %}

{#-
  Companions for the observation models' final projections, so the join and
  the column read identically across every model (and grep finds one shape):

      SELECT
          ...,
          {{ resolved_person_id_column() }},
          ...
      FROM value_measures
      {{ resolved_person_id_join('value_measures') }}
      WHERE ...

  The `if` keeps a join miss an honest NULL instead of the zero UUID a plain
  LEFT JOIN default would mint — join_use_nulls deliberately stays off
  model-wide so the models' other joins keep their semantics.
-#}

{% macro resolved_person_id_join(rel) %}
    LEFT JOIN ({{ resolve_person_id() }}) AS identity_map
        ON identity_map.email = lower(trimBoth({{ rel }}.entity_id))
{% endmacro %}

{% macro resolved_person_id_column() %}
    if(
        identity_map.email != '',
        toNullable(identity_map.person_id),
        CAST(NULL AS Nullable(UUID))
    ) AS person_id
{% endmacro %}

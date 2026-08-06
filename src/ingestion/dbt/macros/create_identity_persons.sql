{#-
  Creates the two identity relations gold resolves through and dbt does NOT own
  the DATA of: `identity.identity_persons` — the persons-log copy written by the
  identity-resolution service's persons-sync — and `identity.identity_inputs`,
  the connector evidence written by the per-connector models. The resolver joins
  them (an email resolves through the accounts carrying it), so a build must
  meet both.

  On `identity_persons`: It is written exclusively by the identity-resolution
  service's persons-sync (full snapshot + atomic EXCHANGE swap); gold models
  LEFT JOIN it through the `resolve_person_id()` macro to attach a canonical
  `person_id` to email-keyed observations.

  Called from `on-run-start` (same pattern as `create_task_field_history_staging`)
  so a build on an environment where the sync has never run — fresh cluster,
  CI, local k3d — meets EMPTY tables instead of missing ones: every resolve
  comes back NULL and the pipeline behaves exactly as before the person_id
  column existed. Graceful degradation, not a build failure.

  SCHEMA CONTRACT: this DDL is a byte-for-byte copy of COLUMNS_DDL in
  src/backend/services/identity-resolution/src/infra/identity_persons.rs —
  the service owns the schema (it mirrors its own MariaDB `persons` log,
  `001_persons.sql`, minus the generated `value_hash`, plus the `_synced_at`
  watermark). If the service changes the schema, change THIS macro in the
  same PR. A drifted hook is mostly harmless (CREATE IF NOT EXISTS never
  alters an existing table; the service's own staging-swap upgrades the live
  schema on its next run) but a fresh environment would create the stale
  shape — keep them in lockstep.
-#}

{% macro create_identity_persons() %}
    {% do run_query("CREATE DATABASE IF NOT EXISTS identity") %}

    {% do run_query("
        CREATE TABLE IF NOT EXISTS identity.identity_persons
        (
            id                  UInt64,
            value_type          String,
            insight_source_type String,
            insight_source_id   UUID,
            insight_tenant_id   UUID,
            value_id            Nullable(String),
            value_full_text     Nullable(String),
            value               Nullable(String),
            value_effective     Nullable(String),
            person_id           UUID,
            author_person_id    UUID,
            reason              Nullable(String),
            created_at          DateTime64(6, 'UTC'),
            _synced_at          DateTime64(3, 'UTC')
        )
        ENGINE = MergeTree
        ORDER BY id
    ") %}

    {% do run_query("
        CREATE TABLE IF NOT EXISTS identity.identity_inputs
        (
            unique_key          String,
            insight_tenant_id   UUID,
            insight_source_id   UUID,
            insight_source_type String,
            source_account_id   Nullable(String),
            value_type          String,
            value               Nullable(String),
            value_field_name    String,
            operation_type      String,
            _synced_at          DateTime64(3),
            _version            Int64
        )
        ENGINE = ReplacingMergeTree(_version)
        ORDER BY unique_key
        SETTINGS allow_nullable_key = 1
    ") %}
{% endmacro %}

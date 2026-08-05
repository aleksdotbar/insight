-- depends_on: {{ ref('jira__bronze_promoted') }}
{{ config(
    materialized='view',
    alias='jira__task_issuetypes',
    schema='staging',
    tags=['jira', 'staging', 'silver:class_task_issuetypes']
) }}

-- Per-source issue-type dimension; unioned into `silver.class_task_issuetypes`
-- via `union_by_tag`. Maps every Jira issue type id to the source-neutral
-- `issue_kind` (bug / other / unknown), so Gold tells bug work from the rest
-- without matching a localized type display name.
--
-- `untranslatedName` — not `name` — feeds the classification: it is the type's
-- original name regardless of the instance's language, which is what makes the
-- kind stable across locales. `name` is retained as the display label.
--
-- `hierarchy_level` and `is_subtask` are carried raw (as the status dimension
-- carries `category_id` / `category_key`): they describe container-vs-work
-- structure, which no measure reads yet.
--
-- View, not table: bronze `jira_issuetypes` is MergeTree (full_refresh +
-- overwrite), so the current state of bronze is the current state of staging.
-- FINAL not needed.
--
-- `issue_type_id` is normalised to an integer-string (stripping any `.0` from
-- Airbyte numeric coercion) so it joins `class_task_field_history.value_ids[1]`,
-- which carries the Jira issue type id as a plain string (e.g. '10001').

SELECT
    s.unique_key                                                AS unique_key,
    s.source_id                                                 AS insight_source_id,
    CAST('jira' AS String)                                      AS data_source,
    replaceRegexpOne(toString(s.id), '\.0+$', '')               AS issue_type_id,
    s.name                                                      AS issue_type_name,
    nullIf(toString(s.untranslatedName), '')                    AS untranslated_name,
    toInt32OrNull(toString(s.hierarchyLevel))                   AS hierarchy_level,
    s.subtask                                                   AS is_subtask,
    {{ task_issue_kind("coalesce(nullIf(toString(s.untranslatedName), ''), toString(s.name))") }}
                                                                AS issue_kind,
    toDateTime64(s._airbyte_extracted_at, 3)                    AS collected_at,
    toUnixTimestamp64Milli(s._airbyte_extracted_at)             AS _version
FROM {{ source('bronze_jira', 'jira_issuetypes') }} s
-- `jira_issuetypes` bronze = MergeTree (full_refresh + overwrite), FINAL not supported.

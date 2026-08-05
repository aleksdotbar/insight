-- depends_on: {{ ref('youtrack__bronze_promoted') }}
{{ config(
    materialized='view',
    alias='youtrack__task_issuetypes',
    schema='staging',
    tags=['youtrack', 'silver:class_task_issuetypes']
) }}

-- Per-source issue-type dimension; unioned into `silver.class_task_issuetypes`
-- via `union_by_tag`. YouTrack has no global issue-type table: the equivalent
-- signal is the Type custom field's enum bundle, whose values are the instance's
-- issue types. We explode the bundle(s) and reconcile each value name to the
-- source-neutral `issue_kind` through the shared macro, the same axis Jira
-- derives from `untranslatedName`.
--
-- The Type field is selected by `field_name`, YouTrack's canonical field name —
-- never `field_localized_name`, which is per-language and would make the join
-- depend on the instance's UI locale.
--
-- No untranslated-name equivalent exists here, so a renamed or non-English type
-- set classifies as `unknown` until its names are added to the
-- `task_bug_type_names` / `task_non_bug_type_names` vars.
--
-- `bundle_values_json` is the raw JSON array of bundle value objects. An enum
-- bundle may be shared across projects, so the same value id appears in several
-- rows; `union_by_tag` dedups by `unique_key` (= source + issue_type_id) to one
-- row.

WITH type_fields AS (
    SELECT
        pcf.source_id                                           AS source_id,
        pcf.bundle_values_json                                  AS bundle_values_json,
        pcf._airbyte_extracted_at                               AS _airbyte_extracted_at
    FROM {{ source('bronze_youtrack', 'youtrack_project_custom_fields') }} pcf
    WHERE toString(pcf.field_name) = 'Type'
      AND (lower(toString(pcf.value_type)) = 'enum'
           OR toString(pcf.field_type_id) LIKE 'enum%')
)
SELECT
    concat(toString(tf.source_id), '-', JSONExtractString(val_raw, 'id')) AS unique_key,
    tf.source_id                                                AS insight_source_id,
    CAST('youtrack' AS String)                                  AS data_source,
    JSONExtractString(val_raw, 'id')                            AS issue_type_id,
    JSONExtractString(val_raw, 'name')                          AS issue_type_name,
    CAST(NULL AS Nullable(String))                              AS untranslated_name,
    CAST(NULL AS Nullable(Int32))                               AS hierarchy_level,
    CAST(NULL AS Nullable(Bool))                                AS is_subtask,
    {{ task_issue_kind("JSONExtractString(val_raw, 'name')") }} AS issue_kind,
    toDateTime64(tf._airbyte_extracted_at, 3)                   AS collected_at,
    toUnixTimestamp64Milli(tf._airbyte_extracted_at)            AS _version
FROM type_fields tf
ARRAY JOIN JSONExtractArrayRaw(ifNull(tf.bundle_values_json, '[]')) AS val_raw
WHERE JSONExtractString(val_raw, 'id') != ''

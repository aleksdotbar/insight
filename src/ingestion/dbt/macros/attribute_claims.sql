{% macro attribute_claims(snapshot_ref, entity_id_col, source_type, fields, track_raw_data=false, raw_data_exclude=[]) %}
{#
  Typed person-attribute claims from an SCD2 snapshot model.
  One row per (source account, field, transition): claim_action='set' when a
  field acquires or changes a non-empty value, 'clear' when it becomes empty.

  NULL and '' both normalize to '' (value absent). This differs from
  fields_history's bare toString(), which NULL-propagates through `!=` and
  silently drops any transition into or out of NULL — a claim stream cannot
  afford that: a lost 'x'→NULL transition leaves a stale open value forever.

  A 'clear' is emitted only when a delivered record carries an empty value for
  the field. Absence of the whole record from a sync never closes values:
  no sync-completeness signal exists in the warehouse, and closing values on
  possibly-partial snapshots would fabricate end dates.

  Claim visibility is gated by snapshot versioning: a change appends a claim
  only when it also appends a snapshot version. For track_raw_data producers
  that means the snapshot's check_raw_data_cols var (<conn>_custom_fields)
  must list every custom field the deployment extracts — a raw_data key
  outside that list changes without a snapshot version, so its claim arrives
  late (with the next tracked change) or not at all.

  Args:
    snapshot_ref:      ref() to the SCD2 snapshot (output of the snapshot macro)
    entity_id_col:     source-account identifier column in the snapshot
    source_type:       insight_source_type literal (e.g. 'bamboohr')
    fields:            top-level columns that become attribute claims
    track_raw_data:    also emit claims for every key found in the raw_data
                       JSON column (arbitrary custom fields, no config list)
    raw_data_exclude:  raw_data keys to skip; `fields` and entity_id_col are
                       always skipped. JSONExtractString yields '' for
                       non-string values, so missing key and empty string are
                       indistinguishable inside raw_data.

  Output columns match the class_person_attribute_claims contract:
    unique_key, insight_tenant_id, insight_source_type, insight_source_id,
    source_account_id, field_id, value_id, value_label, claim_action,
    observed_at, ingested_at, _version

  value_id is reserved for immutable source value identifiers; no current
  HR source exposes them, so it is emitted as NULL.
#}
{% set raw_excluded = fields + [entity_id_col] + raw_data_exclude %}

WITH versioned AS (
    SELECT
        unique_key,
        coalesce(tenant_id, '')                          AS insight_tenant_id,
        coalesce(source_id, '')                          AS insight_source_id,
        coalesce(toString({{ entity_id_col }}), '')      AS source_account_id,
        toDateTime64(_tracked_at, 3)                     AS observed_at,
        _airbyte_extracted_at                            AS ingested_at,
        CAST(
            arrayConcat(
                [
                    {% for f in fields %}
                    ('{{ f }}', ifNull(toString({{ f }}), '')){{ ',' if not loop.last }}
                    {% endfor %}
                ]
                {% if track_raw_data %},
                arrayMap(
                    k -> (k, JSONExtractString(ifNull(toString(raw_data), '{}'), k)),
                    arrayFilter(
                        k -> k NOT IN ({% for e in raw_excluded %}'{{ e }}'{{ ', ' if not loop.last }}{% endfor %}),
                        JSONExtractKeys(ifNull(toString(raw_data), '{}'))
                    )
                )
                {% endif %}
            ),
            'Map(String, String)'
        )                                                AS attrs
    FROM {{ snapshot_ref }}
),

-- lagInFrame's out-of-frame default is an empty Map, so the first snapshot
-- version compares against all-absent: every non-empty initial value emits a
-- set and no initial clear is possible — no separate first-version branch.
with_previous AS (
    SELECT
        insight_tenant_id,
        insight_source_id,
        source_account_id,
        observed_at,
        ingested_at,
        attrs                                            AS curr_attrs,
        lagInFrame(attrs) OVER (
            PARTITION BY unique_key
            ORDER BY observed_at
            ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
        )                                                AS prev_attrs
    FROM versioned
),

claims AS (
    SELECT
        insight_tenant_id,
        insight_source_id,
        source_account_id,
        observed_at,
        ingested_at,
        field_id,
        arrayElement(curr_attrs, field_id)               AS value_label
    FROM with_previous
    ARRAY JOIN arrayDistinct(arrayConcat(mapKeys(curr_attrs), mapKeys(prev_attrs))) AS field_id
    WHERE arrayElement(curr_attrs, field_id) != arrayElement(prev_attrs, field_id)
)

SELECT
    concat(
        insight_tenant_id, '-',
        insight_source_id, '-',
        '{{ source_type }}', '-',
        source_account_id, '-',
        field_id, '-',
        toString(toUnixTimestamp64Milli(observed_at))
    )                                                    AS unique_key,
    insight_tenant_id,
    '{{ source_type }}'                                  AS insight_source_type,
    insight_source_id,
    source_account_id,
    field_id,
    CAST(NULL, 'Nullable(String)')                       AS value_id,
    value_label,
    CAST(
        if(value_label = '', 'clear', 'set'),
        'Enum8(\'set\' = 1, \'clear\' = 2)'
    )                                                    AS claim_action,
    observed_at,
    ingested_at,
    toUnixTimestamp64Milli(observed_at)                  AS _version
FROM claims
WHERE source_account_id != ''
{% if is_incremental() %}
  AND toUnixTimestamp64Milli(observed_at) > (SELECT max(_version) FROM {{ this }})
{% endif %}
{% endmacro %}

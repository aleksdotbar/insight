{{ config(
    tags=['data_quality'],
    severity='warn',
    store_failures=true,
    meta={
        'title': 'One attribute claim per account, field, and observation instant',
        'domain': 'hr',
        'category': 'grain',
        'tier': 'error',
        'remediation': 'class_person_attribute_claims must hold at most one claim per (tenant, source type, source instance, account, field, observed_at). A surplus row means two staging producers emitted the same logical claim under different unique_key values (check the unique_key expression in attribute_claims), so RMT cannot collapse them and the gold interval builder sees a same-instant tie with undefined lead() ordering.'
    }
) }}
SELECT
    insight_tenant_id,
    insight_source_type,
    insight_source_id,
    source_account_id,
    field_id,
    observed_at,
    count()                   AS row_count,
    uniqExact(unique_key)     AS distinct_unique_keys
FROM {{ ref('class_person_attribute_claims') }} FINAL
GROUP BY
    insight_tenant_id,
    insight_source_type,
    insight_source_id,
    source_account_id,
    field_id,
    observed_at
HAVING count() > 1

{{ config(
    tags=['data_quality'],
    severity='warn',
    store_failures=true,
    meta={
        'title': 'At most one open attribute interval per account and field',
        'domain': 'hr',
        'category': 'consistency',
        'tier': 'error',
        'remediation': 'A second valid_to IS NULL row for one (tenant, source type, source instance, account, field) means two currently-effective values for a single-valued source field. The gold builder closes every interval except the latest claim per key — two open intervals indicate the leadInFrame partition or claim dedup broke. NULL-aware by construction: this check counts open rows directly instead of comparing NULL bounds.'
    }
) }}
SELECT
    insight_tenant_id,
    insight_source_type,
    insight_source_id,
    source_account_id,
    field_id,
    count()                    AS open_intervals,
    groupArray(value_label)    AS open_values
FROM {{ ref('account_attribute_values') }}
WHERE valid_to IS NULL
GROUP BY
    insight_tenant_id,
    insight_source_type,
    insight_source_id,
    source_account_id,
    field_id
HAVING count() > 1

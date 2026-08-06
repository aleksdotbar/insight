{{ config(
    tags=['data_quality'],
    severity='warn',
    store_failures=true,
    meta={
        'title': 'A clear claim leaves no open attribute interval',
        'domain': 'hr',
        'category': 'consistency',
        'tier': 'error',
        'remediation': 'When the latest claim for a (tenant, source type, source instance, account, field) is a clear, gold must hold no valid_to IS NULL row for that key: the clear closes the previous interval and opens nothing. An open row here means the gold builder emitted a value row for a clear claim or missed the closing lead() — check the claim_action filter in gold/account_attribute_values.sql.'
    }
) }}
WITH latest_claims AS (
    SELECT
        insight_tenant_id,
        insight_source_type,
        insight_source_id,
        source_account_id,
        field_id,
        argMax(claim_action, observed_at) AS last_action
    FROM {{ ref('class_person_attribute_claims') }} FINAL
    GROUP BY
        insight_tenant_id,
        insight_source_type,
        insight_source_id,
        source_account_id,
        field_id
)
SELECT
    v.insight_tenant_id,
    v.insight_source_type,
    v.insight_source_id,
    v.source_account_id,
    v.field_id,
    v.value_label,
    v.valid_from
FROM {{ ref('account_attribute_values') }} AS v
INNER JOIN latest_claims AS l
    ON  v.insight_tenant_id  = l.insight_tenant_id
    AND v.insight_source_type = l.insight_source_type
    AND v.insight_source_id  = l.insight_source_id
    AND v.source_account_id  = l.source_account_id
    AND v.field_id           = l.field_id
WHERE l.last_action = 'clear'
  AND v.valid_to IS NULL

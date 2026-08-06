{{ config(
    tags=['data_quality'],
    severity='warn',
    store_failures=true,
    meta={
        'title': 'Closed attribute intervals are strictly half-open',
        'domain': 'hr',
        'category': 'consistency',
        'tier': 'error',
        'remediation': 'Every closed interval must satisfy valid_from < valid_to; an equal pair is an empty interval and an inverted pair is a corrupted one. Both mean the closing claim did not sort strictly after the opening claim — check claim grain uniqueness (same-instant set and clear collapse lead() ordering).'
    }
) }}
SELECT
    insight_tenant_id,
    insight_source_type,
    insight_source_id,
    source_account_id,
    field_id,
    value_label,
    valid_from,
    valid_to
FROM {{ ref('account_attribute_values') }}
WHERE valid_to IS NOT NULL
  AND valid_to <= valid_from

-- depends_on: {{ ref('bamboohr__bronze_promoted') }}
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    schema='staging',
    engine='ReplacingMergeTree(_version)',
    order_by=['unique_key'],
    tags=['bamboohr', 'silver:class_person_attribute_claims']
) }}

{{ attribute_claims(
    snapshot_ref=ref('bamboohr__employees_snapshot'),
    entity_id_col='id',
    source_type='bamboohr',
    fields=[
        'jobTitle', 'department', 'division',
        'status', 'employmentHistoryStatus',
        'location', 'country', 'city'
    ],
    track_raw_data=true,
    raw_data_exclude=[
        'displayName', 'firstName', 'lastName', 'workEmail',
        'employeeNumber', 'supervisor', 'supervisorEId', 'supervisorEmail',
        'hireDate', 'originalHireDate', 'terminationDate',
        'lastChanged', 'standardHoursPerWeek'
    ]
) }}

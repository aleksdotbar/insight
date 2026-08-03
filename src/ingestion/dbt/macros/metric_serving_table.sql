{% macro metric_serving_query_settings(join_use_nulls=none) %}
    {% set settings = {
        'max_memory_usage': 1610612736,
        'max_threads': 2,
        'max_block_size': 32768,
        'max_insert_block_size': 32768,
        'min_insert_block_size_rows': 32768,
        'min_insert_block_size_bytes': 16777216,
        'max_partitions_per_insert_block': 512,
        'max_bytes_before_external_group_by': 268435456,
        'max_bytes_before_external_sort': 268435456,
        'max_bytes_in_join': 268435456,
        'join_algorithm': 'auto'
    } %}
    {% if join_use_nulls is not none %}
        {% do settings.update({'join_use_nulls': join_use_nulls}) %}
    {% endif %}
    {{ return(settings) }}
{% endmacro %}

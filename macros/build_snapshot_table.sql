{% macro build_snapshot_table(strategy, sql) %}

    select 
        *,
        {{ strategy.scd_id }} as dbt_scd_id,
        cast(DATETIME_ADD(CURRENT_DATETIME('Europe/Istanbul'), INTERVAL 1 DAY) as timestamp) as dbt_updated_at,
        nullif(current_timestamp(),timestamp('1900-01-01 00:00:00')) as dbt_valid_from,
        nullif(timestamp('9999-12-31 00:00:00'),current_timestamp()) as dbt_valid_to
    from (
        {{ sql }}
    ) sbq

    
{% endmacro %}
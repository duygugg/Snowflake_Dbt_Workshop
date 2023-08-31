{% macro build_snapshot_table(strategy, sql) %}

    select 
        *,
        {{ strategy.scd_id }} as dbt_scd_id,
        current_timestamp() as dbt_updated_at,
        nullif('1900-01-01 00:00:00'::timestamp,current_timestamp) as dbt_valid_from,
        nullif('9999-12-31 00:00:00'::timestamp,current_timestamp()) as dbt_valid_to
    from (
        {{ sql }}
    ) sbq

    
{% endmacro %}
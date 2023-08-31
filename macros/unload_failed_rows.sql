{% macro unload_failed_rows(target_data,unique_column,table) %}
    
    {% set create_table %}
        create or replace table TESTS.TASTYBYTES.{{table}}_{{unique_column}}_failed  as (
            select *  from {{ target_data }}
            where {{ unique_column }} in ( select {{ unique_column }} from {{ target_data }}  group by {{ unique_column }} HAVING COUNT(*) > 1)
    
        )
    {% endset %}

    {% do run_query(create_table) %}
    

{% endmacro %}
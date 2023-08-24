{% macro unload_failed_rows(target_data,unique_column,table) %}
        
    {% set sql -%}
        with target_counts as (
            select {{ unique_column }}, count(*) as count
            from {{ target_data }}
            group by {{ unique_column }}
        ),
        non_unique_values as (
            select {{ unique_column }}
            from target_counts
            where count > 1
        )
        select * from {{ target_data }}
        where {{ unique_column }} not in (select {{ unique_column }} from non_unique_values)
    
    {%- endset %}

    {% set create_table %}
        create or replace table TESTS.TASTYBYTES.{{table}}_{{unique_column}}_failed  as (
            select *  from {{ target_data }}
            where {{ unique_column }} in ( select {{ unique_column }} from {{ target_data }}  group by {{ unique_column }} HAVING COUNT(*) > 1)
    
        )
    {% endset %}

    {% do run_query(create_table) %}
    

{% endmacro %}
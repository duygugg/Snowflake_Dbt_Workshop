
{{
  config(
    materialized = 'incremental',
    database = "STAGING",
    transient = False,
    unique_key = 'result_id'
  )
}}

with empty_table as (
    select
        null as result_id,
        null as invocation_id,
        null as unique_id,
        null as database_name,
        null as schema_name,
        null as name,
        null as test_applied_column_name,
        null as resource_type,
        null as complied_sql,
        null as status,
        cast(null as float) as execution_time,
        cast(null as int) as rows_affected
)

select * from empty_table where 1 = 0
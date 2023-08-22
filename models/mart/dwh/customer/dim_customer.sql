
{{
    config(
        materialized='incremental',
        unique_key = 'customer_key',
        incremental_strategy='merge',
        merge_update_columns = ['last_name','marital_status','children_count','phone_number','changedate']
    )
}}
with customer as(
    select * from {{ref('stg_tastybytes_customer')}}
)

select 
    customer_key,
    first_name,
    last_name,
    --{% if table_exists(this.database,this.schema,this.table) == 1 %}
    --    CASE 
    --        WHEN t2.customer_key is not null THEN cte.last_name
    --        ELSE t2.last_name
    --    end as last_name,
    --{% else %}
    --    last_name,
    --{% endif %}
    city,
    country,
    postal_code,
    preferred_language,
    gender,
    marital_status,
    children_count,
    register_date,
    birthday_date,
    email,
    phone_number,
    current_timestamp() as changedate

from customer



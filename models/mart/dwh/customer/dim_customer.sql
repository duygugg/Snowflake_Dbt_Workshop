
{{
    config(
        materialized='incremental',
        unique_key = 'customer_key',
        incremental_strategy='merge',
        merge_update_columns = ['last_name','marital_status','children_count','phone_number','change_date','insert_date']
    )
}}
with customer as(
    select * from {{ref('stg_tastybytes_customer')}}
)

select 
    t1.customer_key,
    t1.first_name,
    t1.last_name,
    t1.city,
    t1.country,
    t1.postal_code,
    t1.preferred_language,
    t1.gender,
    t1.marital_status,
    t1.children_count,
    t1.register_date,
    t1.birthday_date,
    t1.email,
    t1.phone_number,
{% if table_exists(this.database,'CUSTOMER',this.table) ==  1 %}
    CASE 
    --if record with same customer key is added then keep the inital insert_date
        WHEN t2.customer_key is not null THEN t2.insert_date
    -- if a new customer record added, assign a current timestamp
        ELSE current_timestamp()
    end as insert_date,
{% else %}
    -- initial load of the table, assign a default insertdate value
    '2023-01-15 00:00:00'::timestamp as insert_date,
{% endif %}
{% if table_exists(this.database,'CUSTOMER',this.table) == 1 %}
    CASE 
    --if record with same customer key is added then change the change_date as current_timestamp
        WHEN t2.customer_key is not null THEN current_timestamp()
    -- if a new customer record added, assign null as change_date. Bc newly inserted records doesnt subject to any change
        ELSE null
    end as change_date
{% else %}
-- initial load of the table, assign null as change_date.
    null as change_date
{% endif %}
from customer as t1

{% if table_exists("DWH","CUSTOMER",this.table) == 1 %}
    LEFT JOIN 
        DWH.CUSTOMER.dim_customer as t2
    ON 
        t1.customer_key = t2.customer_key
{% endif %}



{{
    config(
        materialized = "table"
    )
}}
with source as (
    select * from {{ source('tastybytes', 'order_header_test') }}
),

renamed as (
    select
        order_id as order_key,
        customer_id as customer_key,
        truck_id as truck_key,
        discount_id as discount_key,
        shift_id as shift_key,
        shift_start_time,
        shift_end_time,
        timediff(hour,shift_start_time,shift_end_time) as shift_duration,  
        DAY( order_ts :: date) AS order_date_in_day,
        order_ts as order_date,
        served_ts as served_date,
        order_tax_amount as tax_amount,
        order_amount,
        order_discount_amount,
        order_total,
        order_currency as currency         
    from source
)

select * from renamed

--{{ target.schema }}
--{{ schema }}
--{{this.schema}}
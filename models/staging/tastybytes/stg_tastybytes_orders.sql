with source as (

    select * from {{ source('tastybytes', 'order_header') }}

),

renamed as (

    select

        order_id as order_key,
        customer_id as customer_key,
        truck_id as truck_key,
        shift_id as shift_key,
        shift_start_time,
        shift_end_time,
        order_amount,
        order_discount_amount,
        DATE_TRUNC('day', order_ts) AS order_date_in_day,
        order_ts as order_date,
        order_total,
        order_currency as currency
            
    from source

)

select * from renamed
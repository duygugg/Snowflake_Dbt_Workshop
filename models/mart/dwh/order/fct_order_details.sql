{{
    config(
        materialized = "table",
        schema = "ORDER"
    )
}}

with order_item as (

    select * from {{ ref('int_order_details') }}

)

select 
    order_item_key,
    order_key,
    menu_item_key,
    discount_key,
    line_number,
    quantity,
    discount_amount,
    discount_percentage,
    tax_amount,
    currency,
    net_price 

from order_item
    
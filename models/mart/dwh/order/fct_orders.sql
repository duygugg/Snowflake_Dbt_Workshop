{{
    config(
        materialized = "table",
        schema = "ORDER"
    )
}}

with orders as (
    
    select * from {{ ref('stg_tastybytes_orders') }} 

),
order_item as (
    
    select * from {{ ref('int_order_details') }}

),
order_item_summary as (

    select 
        order_key,
        sum(order_item_total_price) as order_total_price,
        sum(quantity) as order_total_quantity,
        sum(tax_amount) as order_tax_price,
        sum(discount_amount) as order_discount_price,
        sum(net_price) as order_net_price
    from order_item
    group by
        1
),
final as (

    select 

        orders.order_key, 
        orders.order_date,
        orders.customer_key,
        orders.truck_key,
        orders.shift_duration,
        1 as order_count,  
        orders.currency,              
        order_item_summary.order_total_price,
        order_item_summary.order_total_quantity,
        order_item_summary.order_discount_price,
        order_item_summary.order_tax_price,
        order_item_summary.order_net_price
    from
        orders
        inner join order_item_summary
            on orders.order_key = order_item_summary.order_key
)
select 
    *
from
    final

order by
    order_date
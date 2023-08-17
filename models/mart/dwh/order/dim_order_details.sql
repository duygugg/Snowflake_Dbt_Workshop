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
    {{dbt_utils.star(from=ref('int_order_details'),
        except=["customer_key","order_item_total_price","discount_amount","net_price"])}},
    order_item_total_price/quantity as unit_price,
    order_item_total_price as total_price,
    discount_amount,
    net_price

from order_item
    
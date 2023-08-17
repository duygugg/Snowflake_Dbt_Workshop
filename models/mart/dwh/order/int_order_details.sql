{{
    config(
         materialized = "view"
    )
}}
with orders as (
    
    select * from {{ ref('stg_tastybytes_orders') }}

),

order_item as (

    select * from {{ ref('stg_tastybytes_orderdetails') }}

)

,order_item_discount_added as(
    select
        {{ dbt_utils.star(from=ref('stg_tastybytes_orderdetails'), except=["order_item_discount_amount", "discount_id"]) }},
        case 
            when order_item_total_price>40.000 then uniform(4.000, 6.000, random())
            else order_item_discount_amount
        end as order_item_discount_amount,
        case 
            when order_item_total_price>40.000 then uniform(1, 294 , random()) 
            else discount_id
        end as discount_id
    from 
        order_item
)

select 
    t2.order_item_key,
    t1.order_key,
    t2.menu_item_key,
    t2.discount_id as discount_key,
    t2.line_number, 
    t1.customer_key,
    t2.quantity,
    t1.order_date,
    t1.served_date,
    --: Discount (%) = (Discount amount/ Total price) × 100
    (t2.ORDER_ITEM_DISCOUNT_AMOUNT /t2.order_item_total_price::decimal(16,2))*100 as discount_percentage,
    t1.tax_amount,
    t1.currency,
    t2.order_item_total_price,
    t2.order_item_discount_amount as discount_amount,
    case 
        when 
            t2.ORDER_ITEM_DISCOUNT_AMOUNT is not null 
            then 
                (t2.order_item_total_price) - (t2.ORDER_ITEM_DISCOUNT_AMOUNT)           
        else order_item_total_price
    end as net_price

from
    orders as t1
inner join order_item_discount_added as t2
        on t1.order_key = t2.order_key
order by
    t1.order_date
with source as (

    select * from {{ source('tastybytes', 'order_detail') }}

),

renamed as (

    select
    
        {{ dbt_utils.generate_surrogate_key(
            ['ORDER_DETAIL_ID', 
            'ORDER_ID']) }}
                as order_item_key,
        ORDER_ID as order_key,
        order_detail_id,
        discount_key,
        menu_item_id,
        line_number,
        quantity,
        unit_price,
        quantity*unit_price as order_item_total_price,
        case 
            when DISCOUNT_AMOUNT is not null then (quantity*unit_price) - (DISCOUNT_AMOUNT) 
            else (quantity*unit_price)
        end  as net_price,
       ((quantity*unit_price) / (DISCOUNT_AMOUNT)) *100 as discount_percentage

    from source

)

select * from renamed
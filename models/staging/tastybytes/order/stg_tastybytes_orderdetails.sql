{{
    config(
        materialized = "table",
        target_database = "STAGING",
        target_schema = "ORDER",
        pre_hook = "{{load_failed_rows(this,this.name)}}",
        post_hook = "{{deletle_rows_failed_test(this,this.name,'order_key')}}"
    )
}}

with source as (

    select * from {{ source('tastybytes', 'order_detail_test') }}

),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['ORDER_DETAIL_ID', 'ORDER_ID']) }} as order_item_key,
        ORDER_ID as order_key,
        order_detail_id,
        cast(discount_id as integer) as discount_key,
        menu_item_id as menu_item_key,
        line_number,
        quantity,
        cast(ORDER_ITEM_DISCOUNT_AMOUNT as decimal(15,4)) as ORDER_ITEM_DISCOUNT_AMOUNT,
        unit_price,
        quantity*unit_price as order_item_total_price

    from source

)

select * from renamed

--{{this}}
--{{this.schema}}
--{{this.database}}

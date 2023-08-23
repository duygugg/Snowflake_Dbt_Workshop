{{
    config(
        materialized = "table",
        target_database = "STAGING",
        target_schema = "ORDER",
        pre_hook = "{{unload_failed_rows(this,'order_key',this.table)}}",
        post_hook = "delete from {{ this }} where order_key  in ( select  order_key from {{ this }}  group by order_key HAVING COUNT(*) > 1)"
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
        discount_id,
        menu_item_id as menu_item_key,
        line_number,
        quantity,
        ORDER_ITEM_DISCOUNT_AMOUNT,
        unit_price,
        quantity*unit_price as order_item_total_price

    from source

)

select * from renamed
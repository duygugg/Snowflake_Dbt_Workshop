{{
    config(
        materialized = "table"
    )
}}
with source as (

    select * from {{source('tastybytes','menu')}}

)

,renamed as(
    select 
        m.menu_id as menu_key,
        m.menu_item_id as menu_item_key,
        m.menu_type_id as menu_type_key,
        m.menu_type as menu_type_name,
        m.truck_brand_name,
        m.menu_item_name,
        m.item_category,
        m.item_subcategory,
        'USD' as currency,
        m.COST_OF_GOODS_USD as cost_of_goods,
        m.SALE_PRICE_USD as sale_price,
        obj.value:"ingredients"::VARIANT AS ingredients,
        obj.value:"is_healthy_flag"::VARCHAR(1) AS is_healthy_flag,
        obj.value:"is_gluten_free_flag"::VARCHAR(1) AS is_gluten_free_flag,
        obj.value:"is_dairy_free_flag"::VARCHAR(1) AS is_dairy_free_flag,
        obj.value:"is_nut_free_flag"::VARCHAR(1) AS is_nut_free_flag
    from  source as m,
    LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj

   
)

select * from renamed

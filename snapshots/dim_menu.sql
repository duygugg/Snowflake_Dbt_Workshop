{% snapshot dim_product %}
{{
    config (
        target_database = 'DWH',
        target_schema = 'PRODUCT',
        unique_key = 'menu_id',
        strategy = 'check',
        check_cols = ['menu_item_key','menu_item_name','truck_brand_name','item_category','item_subcategory']
    )
}}

select
    menu_key,
    menu_type_key,
    menu_type_name,
    truck_brand_name,
    menu_item_key,
    menu_item_name,
    item_category,
    item_subcategory,
    currency,
    cost_of_goods,
    sale_price
    ingredients,
    is_healthy_flag,
    is_gluten_free_flag,
    is_dairy_free_flag,
    is_nut_free_flag,
    case 
        when 
            {{ table_exists(this.database,"PRODUCT",this.table) }} == 1 
        then
            1 + row_number()over(partition by menu_key order by dbt_valid_from)
        else 1
    end as version
from
    {{ref('int_menu')}}

{% endsnapshot %}

--{{this.database}}
--{{this.schema}}
--{{this.table}}
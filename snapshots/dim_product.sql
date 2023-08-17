{% snapshot dim_product %}
{{
    config (
        target_database = 'DWH',
        target_schema = 'PRODUCT',
        unique_key = 'menu_id',
        strategy = 'check',
        check_cols = ['menu_item_id','cost_of_goods','sale_price']
    )
}}

select
    *
from
    {{ref('int_product')}}


{% endsnapshot %}
{{
    config(
         materialized = "ephemeral"
    )
}}
with menu as (

    select * from {{ref("stg_tastybytes_menu")}}
)

, truck as(

    select * from {{source("tastybytes","truck")}}

)

,final as (
    select 
        t1.*,
        t2.truck_id  
    from 
        menu as t1
    inner join 
        truck as t2
    on t1.menu_type_id = t2.menu_type_id
)

select * from final
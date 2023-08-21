with customer as(
    select * from {{ref('stg_tastybytes_customer')}}
)

select * from customer
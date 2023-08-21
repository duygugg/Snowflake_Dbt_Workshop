with source as (
    select * from {{source('tastybytes_customer','customer_loyalty')}}
)

select
    *
from source